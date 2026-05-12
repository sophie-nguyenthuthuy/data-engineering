from __future__ import annotations

from pathlib import Path

import duckdb
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import auth, ingest, worker
from ..config import DATA_DIR
from ..eval import harness
from ..stubs import pubsub, slack, warehouse

app = FastAPI(title="Email Triage Control Plane")
BASE = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


@app.on_event("startup")
def _startup() -> None:
    pubsub.init()
    warehouse.init()
    auth.init()


# ---------- auth helpers ----------
def _user_from_cookie(request: Request) -> dict:
    token = request.cookies.get("triage_token")
    if not token:
        raise HTTPException(status_code=401, detail="login required")
    return auth._decode(token)


def _require_admin(request: Request) -> dict:
    user = _user_from_cookie(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="admin required")
    return user


# ---------- pages ----------
@app.get("/", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    user = auth.authenticate(username, password)
    if not user:
        return JSONResponse({"error": "invalid credentials"}, status_code=401)
    token = auth.mint_token(user)
    resp = RedirectResponse(url="/app", status_code=303)
    resp.set_cookie("triage_token", token, httponly=True, samesite="lax")
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/", status_code=303)
    resp.delete_cookie("triage_token")
    return resp


@app.get("/app", response_class=HTMLResponse)
def shell(request: Request):
    try:
        user = _user_from_cookie(request)
    except HTTPException:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})


# ---------- API (tenant from token, never from args) ----------
@app.get("/api/overview")
def overview(request: Request):
    user = _user_from_cookie(request)
    tid = user["tenant_id"]
    processed = warehouse.query(
        "SELECT COUNT(*) AS n FROM emails_processed WHERE tenant_id = ?", [tid],
    )[0]["n"]
    raw = warehouse.query(
        "SELECT COUNT(*) AS n FROM emails_raw WHERE tenant_id = ?", [tid],
    )[0]["n"]
    by_label = warehouse.query(
        """
        SELECT predicted_label AS label, COUNT(*) AS n, AVG(confidence) AS avg_conf
        FROM emails_processed WHERE tenant_id = ?
        GROUP BY predicted_label ORDER BY n DESC
        """,
        [tid],
    )
    by_priority = warehouse.query(
        "SELECT priority, COUNT(*) AS n FROM emails_processed WHERE tenant_id = ? GROUP BY priority",
        [tid],
    )
    return {
        "tenant_id": tid,
        "raw_count": raw,
        "processed_count": processed,
        "by_label": by_label,
        "by_priority": by_priority,
        "pubsub": pubsub.stats(),
    }


@app.get("/api/messages")
def messages(request: Request, limit: int = 50):
    user = _user_from_cookie(request)
    rows = warehouse.query(
        """
        SELECT r.id, r.subject, r.sender, r.received_at,
               p.predicted_label, p.confidence, p.priority, p.summary, p.latency_ms
        FROM emails_raw r
        LEFT JOIN emails_processed p ON r.id = p.id
        WHERE r.tenant_id = ?
        ORDER BY r.received_at DESC
        LIMIT ?
        """,
        [user["tenant_id"], limit],
    )
    return {"rows": rows}


@app.get("/api/dlq")
def dlq(request: Request):
    _user_from_cookie(request)
    with duckdb.connect(str(DATA_DIR / "pubsub.duckdb")) as c:
        cur = c.execute(
            """
            SELECT message_id, topic, last_error, delivery_count, published_at
            FROM messages
            WHERE state = 'dlq' OR topic = 'emails.dlq'
            ORDER BY published_at DESC
            LIMIT 50
            """
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {"rows": rows}


@app.get("/api/runs")
def runs(request: Request):
    _user_from_cookie(request)
    rows = warehouse.query(
        """
        SELECT run_id, kind, tenant_id, status, started_at, finished_at, details
        FROM runs ORDER BY started_at DESC LIMIT 30
        """
    )
    return {"rows": rows}


@app.get("/api/eval")
def eval_results(request: Request):
    _user_from_cookie(request)
    rows = warehouse.query(
        """
        SELECT e.run_id, e.label, e.precision, e.recall, e.f1, e.support, r.started_at
        FROM eval_results e
        JOIN runs r ON e.run_id = r.run_id
        ORDER BY r.started_at DESC, e.label
        LIMIT 100
        """
    )
    return {"rows": rows}


@app.get("/api/slack")
def slack_outbox(request: Request):
    _user_from_cookie(request)
    tid = _user_from_cookie(request)["tenant_id"]
    all_recent = slack.recent(200)
    return {"rows": [r for r in all_recent if r["tenant_id"] == tid][:30]}


# ---------- admin actions ----------
@app.post("/api/actions/ingest")
def action_ingest(request: Request):
    _require_admin(request)
    return ingest.run_once()


@app.post("/api/actions/process")
def action_process(request: Request):
    _require_admin(request)
    return worker.drain()


@app.post("/api/actions/eval")
def action_eval(request: Request):
    _require_admin(request)
    return harness.run()
