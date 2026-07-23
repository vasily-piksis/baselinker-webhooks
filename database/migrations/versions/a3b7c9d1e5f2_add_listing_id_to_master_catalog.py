"""Add listing_id to master_catalog

Revision ID: a3b7c9d1e5f2
Revises: 120f8e23e14c
Create Date: 2026-02-11 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a3b7c9d1e5f2"
down_revision: Union[str, None] = "120f8e23e14c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "master_catalog",
        sa.Column("listing_id", sa.String(length=100), nullable=True),
    )
    op.create_index(
        "idx_master_catalog_listing_id",
        "master_catalog",
        ["listing_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_master_catalog_listing_id", table_name="master_catalog")
    op.drop_column("master_catalog", "listing_id")
