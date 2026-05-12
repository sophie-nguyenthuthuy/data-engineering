from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import engine
from . import models
from .routers import datasets, analysts, budgets, queries

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Differential Privacy Budget Manager",
    description="Query gateway with ε-budget tracking, Laplace/Gaussian mechanisms, and data-owner UI.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasets.router)
app.include_router(analysts.router)
app.include_router(budgets.router)
app.include_router(queries.router)


@app.get("/health")
def health():
    return {"status": "ok"}
