"""Gold — dim_customer (SCD Type 2).

Address changes create a new row; business key `customer_id` is preserved and
a surrogate key `customer_sk` is minted per version. `is_current` flag and
`valid_from`/`valid_to` timestamps are maintained by `APPLY CHANGES`.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F


dlt.create_streaming_table(
    name="dim_customer",
    comment="SCD2 customer dimension.",
    table_properties={"quality": "gold"},
)

dlt.apply_changes(
    target="dim_customer",
    source="silver_customers",
    keys=["customer_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["address_line1", "city", "country", "email"],
    except_column_list=["_ingest_ts"],
)


@dlt.table(
    name="dim_customer_current",
    comment="Convenience view: only the current version of each customer.",
    table_properties={"quality": "gold"},
)
def dim_customer_current():
    return (
        dlt.read("dim_customer")
        .where("__END_AT IS NULL")
        .withColumn(
            "customer_sk",
            F.sha2(F.concat_ws("||", "customer_id", F.coalesce("__START_AT", F.lit("init"))), 256),
        )
    )
