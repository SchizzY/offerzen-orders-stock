"""stock status and outbox sequence

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-20 01:22:58.394363

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column("stock_status", sa.Text(), server_default="pending", nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_orders_stock_status_known"),
        "orders",
        "stock_status IN ('pending', 'reserved', 'insufficient')",
    )
    op.alter_column("outbox_events", "processed_at", new_column_name="published_at")
    op.add_column("outbox_events", sa.Column("seq", sa.BigInteger(), nullable=True))
    op.create_unique_constraint(op.f("uq_outbox_events_seq"), "outbox_events", ["seq"])
    op.create_index(
        op.f("ix_outbox_events_unsequenced"),
        "outbox_events",
        ["id"],
        postgresql_where=sa.text("seq IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_outbox_events_unsequenced"), table_name="outbox_events")
    op.drop_constraint(op.f("uq_outbox_events_seq"), "outbox_events", type_="unique")
    op.drop_column("outbox_events", "seq")
    op.alter_column("outbox_events", "published_at", new_column_name="processed_at")
    op.drop_constraint(op.f("ck_orders_stock_status_known"), "orders", type_="check")
    op.drop_column("orders", "stock_status")
