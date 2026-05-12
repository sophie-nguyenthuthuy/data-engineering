"""
Data Catalog with Lineage Tracking
Entry point — run with:  python main.py  or  uvicorn main:app --reload
"""
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from catalog.database import init_db
from catalog.api import router

app = FastAPI(
    title="Data Catalog & Lineage Tracker",
    description="Auto-discover data assets, tag PII columns, and visualize column-level lineage.",
    version="1.0.0",
)

# Initialize catalog SQLite DB on startup
@app.on_event("startup")
def on_startup():
    init_db()

# REST API under /api
app.include_router(router, prefix="/api")

# Serve static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")

@app.get("/assets", include_in_schema=False)
def assets_page():
    return FileResponse("static/assets.html")

@app.get("/lineage", include_in_schema=False)
def lineage_page():
    return FileResponse("static/lineage.html")

@app.get("/pii", include_in_schema=False)
def pii_page():
    return FileResponse("static/pii.html")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
