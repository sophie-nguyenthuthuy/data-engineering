from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from catalog.database import get_db
from catalog.models import (
    ColumnLineage, ColumnNode, DataSource, LineageJob, SchemaNode, TableNode
)
from catalog.schemas import (
    ColumnOut, ColumnUpdate, DataSourceCreate, DataSourceOut,
    LineageEdge, LineageGraph, LineageJobCreate, LineageJobOut,
    LineageNode, SchemaOut, SearchResult, TableOut
)
from catalog.discovery import discover_source
from catalog.lineage import extract_lineage, parse_column_refs

router = APIRouter()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_or_404(db: Session, model, id: int):
    obj = db.get(model, id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"{model.__name__} {id} not found")
    return obj


def _col_node_id(col: ColumnNode) -> str:
    return f"col:{col.id}"


def _table_node_id(tbl: TableNode) -> str:
    return f"tbl:{tbl.id}"


# ── DataSources ───────────────────────────────────────────────────────────────

@router.get("/sources", response_model=list[DataSourceOut])
def list_sources(db: Session = Depends(get_db)):
    return db.query(DataSource).all()


@router.post("/sources", response_model=DataSourceOut, status_code=201)
def create_source(payload: DataSourceCreate, db: Session = Depends(get_db)):
    if db.query(DataSource).filter_by(name=payload.name).first():
        raise HTTPException(400, "Source name already exists")
    src = DataSource(**payload.model_dump())
    db.add(src)
    db.commit()
    db.refresh(src)
    return src


@router.delete("/sources/{source_id}", status_code=204)
def delete_source(source_id: int, db: Session = Depends(get_db)):
    src = _get_or_404(db, DataSource, source_id)
    db.delete(src)
    db.commit()


@router.post("/sources/{source_id}/scan", response_model=dict)
def scan_source(source_id: int, db: Session = Depends(get_db)):
    src = _get_or_404(db, DataSource, source_id)
    result = discover_source(src.connection_string, src.engine_type)
    if result.errors:
        raise HTTPException(422, detail=result.errors)

    # Upsert schemas
    schema_map: dict[str, SchemaNode] = {}
    for s in result.schemas:
        node = db.query(SchemaNode).filter_by(source_id=src.id, name=s["name"]).first()
        if not node:
            node = SchemaNode(source_id=src.id, name=s["name"])
            db.add(node)
            db.flush()
        schema_map[s["name"]] = node

    # Upsert tables
    table_map: dict[tuple, TableNode] = {}
    for t in result.tables:
        snode = schema_map.get(t["schema"])
        if not snode:
            continue
        node = db.query(TableNode).filter_by(schema_id=snode.id, name=t["name"]).first()
        if not node:
            node = TableNode(schema_id=snode.id, name=t["name"])
            db.add(node)
        node.row_count = t["row_count"]
        db.flush()
        table_map[(t["schema"], t["name"])] = node

    # Upsert columns
    col_count = 0
    pii_count = 0
    for c in result.columns:
        tnode = table_map.get((c["schema"], c["table"]))
        if not tnode:
            continue
        col = db.query(ColumnNode).filter_by(table_id=tnode.id, name=c["name"]).first()
        if not col:
            col = ColumnNode(table_id=tnode.id, name=c["name"])
            db.add(col)
        col.data_type = c["data_type"]
        col.is_nullable = c["is_nullable"]
        col.is_primary_key = c["is_primary_key"]
        col.pii_tags = c["pii_tags"]
        col.sample_values = c["sample_values"]
        col_count += 1
        if c["pii_tags"]:
            pii_count += 1

    src.last_scanned_at = datetime.now(timezone.utc)
    db.commit()

    return {
        "schemas": len(result.schemas),
        "tables": len(result.tables),
        "columns": col_count,
        "pii_columns": pii_count,
        "errors": result.errors,
    }


# ── Schemas / Tables / Columns ────────────────────────────────────────────────

@router.get("/sources/{source_id}/schemas", response_model=list[SchemaOut])
def list_schemas(source_id: int, db: Session = Depends(get_db)):
    src = _get_or_404(db, DataSource, source_id)
    schemas = db.query(SchemaNode).filter_by(source_id=src.id).all()
    out = []
    for s in schemas:
        tables = []
        for t in s.tables:
            cols = [ColumnOut.model_validate(c) for c in t.columns]
            tables.append(TableOut(
                id=t.id, name=t.name, description=t.description,
                row_count=t.row_count, tags=t.tags or [], columns=cols
            ))
        out.append(SchemaOut(id=s.id, name=s.name, description=s.description, tables=tables))
    return out


@router.get("/tables/{table_id}", response_model=TableOut)
def get_table(table_id: int, db: Session = Depends(get_db)):
    tbl = _get_or_404(db, TableNode, table_id)
    cols = [ColumnOut.model_validate(c) for c in tbl.columns]
    return TableOut(
        id=tbl.id, name=tbl.name, description=tbl.description,
        row_count=tbl.row_count, tags=tbl.tags or [], columns=cols
    )


@router.patch("/columns/{col_id}", response_model=ColumnOut)
def update_column(col_id: int, payload: ColumnUpdate, db: Session = Depends(get_db)):
    col = _get_or_404(db, ColumnNode, col_id)
    if payload.description is not None:
        col.description = payload.description
    if payload.pii_tags is not None:
        col.pii_tags = payload.pii_tags
    db.commit()
    db.refresh(col)
    return col


# ── LineageJobs ───────────────────────────────────────────────────────────────

@router.get("/jobs", response_model=list[LineageJobOut])
def list_jobs(db: Session = Depends(get_db)):
    return db.query(LineageJob).all()


@router.get("/jobs/{job_id}", response_model=LineageJobOut)
def get_job(job_id: int, db: Session = Depends(get_db)):
    return _get_or_404(db, LineageJob, job_id)


@router.post("/jobs", response_model=LineageJobOut, status_code=201)
def create_job(payload: LineageJobCreate, db: Session = Depends(get_db)):
    if db.query(LineageJob).filter_by(name=payload.name).first():
        raise HTTPException(400, "Job name already exists")

    job = LineageJob(
        name=payload.name,
        description=payload.description,
        sql_query=payload.sql_query,
        job_type=payload.job_type,
        tags=payload.tags,
    )
    db.add(job)
    db.flush()

    # Parse and resolve lineage
    edges = extract_lineage(payload.sql_query, dialect=payload.dialect)
    resolved = 0
    for edge in edges:
        if edge["source"] is None:
            continue
        src_col = _resolve_column(db, edge["source"])
        tgt_col = _resolve_column(db, edge["target"])
        if src_col and tgt_col:
            existing = db.query(ColumnLineage).filter_by(
                job_id=job.id,
                source_column_id=src_col.id,
                target_column_id=tgt_col.id,
            ).first()
            if not existing:
                db.add(ColumnLineage(
                    job_id=job.id,
                    source_column_id=src_col.id,
                    target_column_id=tgt_col.id,
                    transformation_logic=edge.get("transform"),
                ))
                resolved += 1

    db.commit()
    db.refresh(job)
    return job


@router.delete("/jobs/{job_id}", status_code=204)
def delete_job(job_id: int, db: Session = Depends(get_db)):
    job = _get_or_404(db, LineageJob, job_id)
    db.delete(job)
    db.commit()


def _resolve_column(db: Session, fqn: str) -> ColumnNode | None:
    """Try to look up a column by 'schema.table.col' or 'table.col'."""
    schema_name, table_name, col_name = parse_column_refs(fqn)
    q = db.query(ColumnNode).join(TableNode).join(SchemaNode)
    if schema_name:
        q = q.filter(SchemaNode.name == schema_name)
    if table_name:
        q = q.filter(TableNode.name == table_name)
    q = q.filter(ColumnNode.name == col_name)
    return q.first()


# ── Lineage Graph ─────────────────────────────────────────────────────────────

@router.get("/lineage/column/{col_id}", response_model=LineageGraph)
def column_lineage(col_id: int, depth: int = 5, db: Session = Depends(get_db)):
    """Return upstream + downstream column-level lineage graph up to `depth` hops."""
    root = _get_or_404(db, ColumnNode, col_id)
    nodes: dict[str, LineageNode] = {}
    edges: list[LineageEdge] = []
    visited: set[int] = set()

    def _add_col_node(col: ColumnNode):
        nid = _col_node_id(col)
        if nid not in nodes:
            tbl = col.table
            src = tbl.schema.source if tbl and tbl.schema else None
            nodes[nid] = LineageNode(
                id=nid,
                label=f"{tbl.name}.{col.name}" if tbl else col.name,
                type="column",
                pii_tags=col.pii_tags or [],
                source_name=src.name if src else None,
            )

    def _traverse_up(col: ColumnNode, hops: int):
        if hops <= 0 or col.id in visited:
            return
        visited.add(col.id)
        _add_col_node(col)
        for edge in col.incoming_lineage:
            src = edge.source_column
            if src:
                _add_col_node(src)
                eid = f"e:{edge.id}"
                edges.append(LineageEdge(
                    id=eid,
                    source=_col_node_id(src),
                    target=_col_node_id(col),
                    job_id=edge.job_id,
                    job_name=edge.job.name,
                    transform=edge.transformation_logic,
                ))
                _traverse_up(src, hops - 1)

    def _traverse_down(col: ColumnNode, hops: int):
        if hops <= 0 or col.id in visited:
            return
        visited.add(col.id)
        _add_col_node(col)
        for edge in col.outgoing_lineage:
            tgt = edge.target_column
            if tgt:
                _add_col_node(tgt)
                eid = f"e:{edge.id}"
                if not any(e.id == eid for e in edges):
                    edges.append(LineageEdge(
                        id=eid,
                        source=_col_node_id(col),
                        target=_col_node_id(tgt),
                        job_id=edge.job_id,
                        job_name=edge.job.name,
                        transform=edge.transformation_logic,
                    ))
                _traverse_down(tgt, hops - 1)

    visited_up: set[int] = set()
    visited_down: set[int] = set()

    def up(col, hops):
        if hops <= 0 or col.id in visited_up:
            return
        visited_up.add(col.id)
        _add_col_node(col)
        for edge in col.incoming_lineage:
            src = edge.source_column
            if src:
                _add_col_node(src)
                eid = f"e:{edge.id}"
                if not any(e.id == eid for e in edges):
                    edges.append(LineageEdge(
                        id=eid, source=_col_node_id(src), target=_col_node_id(col),
                        job_id=edge.job_id, job_name=edge.job.name,
                        transform=edge.transformation_logic,
                    ))
                up(src, hops - 1)

    def down(col, hops):
        if hops <= 0 or col.id in visited_down:
            return
        visited_down.add(col.id)
        _add_col_node(col)
        for edge in col.outgoing_lineage:
            tgt = edge.target_column
            if tgt:
                _add_col_node(tgt)
                eid = f"e:{edge.id}"
                if not any(e.id == eid for e in edges):
                    edges.append(LineageEdge(
                        id=eid, source=_col_node_id(col), target=_col_node_id(tgt),
                        job_id=edge.job_id, job_name=edge.job.name,
                        transform=edge.transformation_logic,
                    ))
                down(tgt, hops - 1)

    up(root, depth)
    down(root, depth)

    return LineageGraph(nodes=list(nodes.values()), edges=edges)


@router.get("/lineage/table/{table_id}", response_model=LineageGraph)
def table_lineage(table_id: int, db: Session = Depends(get_db)):
    """Full column-level lineage graph for all columns in a table."""
    tbl = _get_or_404(db, TableNode, table_id)
    all_nodes: dict[str, LineageNode] = {}
    all_edges: list[LineageEdge] = []
    seen_edge_ids: set[str] = set()

    def _add_node(col: ColumnNode):
        nid = _col_node_id(col)
        if nid not in all_nodes:
            t = col.table
            src = t.schema.source if t and t.schema else None
            all_nodes[nid] = LineageNode(
                id=nid,
                label=f"{t.name}.{col.name}" if t else col.name,
                type="column",
                pii_tags=col.pii_tags or [],
                source_name=src.name if src else None,
            )

    for col in tbl.columns:
        _add_node(col)
        for edge in col.incoming_lineage + col.outgoing_lineage:
            _add_node(edge.source_column)
            _add_node(edge.target_column)
            eid = f"e:{edge.id}"
            if eid not in seen_edge_ids:
                seen_edge_ids.add(eid)
                all_edges.append(LineageEdge(
                    id=eid,
                    source=_col_node_id(edge.source_column),
                    target=_col_node_id(edge.target_column),
                    job_id=edge.job_id,
                    job_name=edge.job.name,
                    transform=edge.transformation_logic,
                ))

    return LineageGraph(nodes=list(all_nodes.values()), edges=all_edges)


# ── Search ────────────────────────────────────────────────────────────────────

@router.get("/search", response_model=list[SearchResult])
def search(q: str, pii_only: bool = False, db: Session = Depends(get_db)):
    results: list[SearchResult] = []
    term = f"%{q.lower()}%"

    # Search tables
    tables = (
        db.query(TableNode)
        .join(SchemaNode).join(DataSource)
        .filter(TableNode.name.ilike(term))
        .limit(30).all()
    )
    for t in tables:
        results.append(SearchResult(
            type="table", id=t.id,
            source_name=t.schema.source.name,
            schema_name=t.schema.name,
            table_name=t.name,
            column_name=None,
            pii_tags=[],
        ))

    # Search columns
    cols_q = (
        db.query(ColumnNode)
        .join(TableNode).join(SchemaNode).join(DataSource)
        .filter(ColumnNode.name.ilike(term))
    )
    if pii_only:
        cols_q = cols_q.filter(ColumnNode.pii_tags != "[]")
    cols = cols_q.limit(50).all()
    for c in cols:
        tags = c.pii_tags or []
        if pii_only and not tags:
            continue
        results.append(SearchResult(
            type="column", id=c.id,
            source_name=c.table.schema.source.name,
            schema_name=c.table.schema.name,
            table_name=c.table.name,
            column_name=c.name,
            pii_tags=tags,
        ))

    return results


@router.get("/pii-report", response_model=list[SearchResult])
def pii_report(db: Session = Depends(get_db)):
    cols = (
        db.query(ColumnNode)
        .join(TableNode).join(SchemaNode).join(DataSource)
        .all()
    )
    results = []
    for c in cols:
        tags = c.pii_tags or []
        if tags:
            results.append(SearchResult(
                type="column", id=c.id,
                source_name=c.table.schema.source.name,
                schema_name=c.table.schema.name,
                table_name=c.table.name,
                column_name=c.name,
                pii_tags=tags,
            ))
    return results


# ── Stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
def stats(db: Session = Depends(get_db)):
    return {
        "sources": db.query(DataSource).count(),
        "schemas": db.query(SchemaNode).count(),
        "tables": db.query(TableNode).count(),
        "columns": db.query(ColumnNode).count(),
        "pii_columns": db.query(ColumnNode).filter(ColumnNode.pii_tags != "[]").count(),
        "lineage_jobs": db.query(LineageJob).count(),
        "lineage_edges": db.query(ColumnLineage).count(),
    }
