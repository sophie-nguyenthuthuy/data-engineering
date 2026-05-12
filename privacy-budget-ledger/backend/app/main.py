"""
Privacy Budget Ledger — FastAPI application.

Endpoints overview
──────────────────
  /datasets      CRUD for datasets
  /analysts      CRUD for analysts
  /budgets       Budget allocations + composition summaries + ledger audit
  /planner/plan  Dry-run query evaluation (accept/rewrite/reject)
  /planner/execute  Execute a query through the planner gateway
  /planner/logs  Query audit log
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import engine, Base
from .routers import datasets, analysts, budgets, planner


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Privacy Budget Ledger",
    description=(
        "Advanced differential-privacy budget accounting using "
        "Rényi DP (RDP) and zero-concentrated DP (zCDP) composition theorems. "
        "Tighter than basic ε-composition — the query planner rejects or "
        "rewrites queries that would exceed budget under the tighter bounds."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasets.router)
app.include_router(analysts.router)
app.include_router(budgets.router)
app.include_router(planner.router)


@app.get("/", tags=["health"])
def root():
    return {
        "service": "Privacy Budget Ledger",
        "version": "1.0.0",
        "docs": "/docs",
        "accounting_modes": ["basic_epsilon", "rdp", "zcdp"],
    }


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}
