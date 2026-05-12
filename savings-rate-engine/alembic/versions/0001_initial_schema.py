"""Initial schema — banks, rate_snapshots, rate_records.

Revision ID: 0001
Revises:
Create Date: 2026-05-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "banks",
        sa.Column("code",       sa.String(20),  primary_key=True),
        sa.Column("name_vi",    sa.String(200), nullable=False),
        sa.Column("name_en",    sa.String(200), nullable=False),
        sa.Column("website",    sa.String(300)),
        sa.Column("logo_url",   sa.String(300)),
        sa.Column("active",     sa.Boolean, default=True, nullable=False),
        sa.Column("created_at", sa.DateTime),
    )

    op.create_table(
        "rate_snapshots",
        sa.Column("id",             sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("bank_code",      sa.String(20), sa.ForeignKey("banks.code"), nullable=False),
        sa.Column("scraped_at",     sa.DateTime, nullable=False, index=True),
        sa.Column("scrape_success", sa.Boolean, default=True, nullable=False),
        sa.Column("error_message",  sa.String(500)),
    )
    op.create_index("ix_snapshots_bank_scraped", "rate_snapshots", ["bank_code", "scraped_at"])

    op.create_table(
        "rate_records",
        sa.Column("id",              sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("snapshot_id",     sa.Integer, sa.ForeignKey("rate_snapshots.id"), nullable=False),
        sa.Column("bank_code",       sa.String(20), nullable=False),
        sa.Column("term_days",       sa.Integer, nullable=False),
        sa.Column("term_label",      sa.String(50)),
        sa.Column("rate_pa",         sa.Float,   nullable=False),
        sa.Column("rate_type",       sa.String(30), default="standard"),
        sa.Column("min_amount_vnd",  sa.Integer),
        sa.Column("currency",        sa.String(5), default="VND"),
    )
    op.create_index("ix_records_bank_term",  "rate_records", ["bank_code", "term_days"])
    op.create_index("ix_records_snapshot",   "rate_records", ["snapshot_id"])


def downgrade() -> None:
    op.drop_table("rate_records")
    op.drop_table("rate_snapshots")
    op.drop_table("banks")
