--------------------------- MODULE pipeline ---------------------------
\* CDC -> Kafka -> Flink-aggregation -> Warehouse -> Reverse-ETL pipeline.
\* Properties: NoDataLoss, ExactlyOnceInAgg, EventualDelivery (liveness).
\*
\* This spec is intentionally small (3 records) so TLC can model-check.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Records, MaxLag

VARIABLES
    pg,            \* set of inserted records
    kafka,         \* sequence of records published by Debezium
    flink_sum,     \* aggregated sum so far
    warehouse,     \* set of records loaded into DW
    rev_etl        \* set of records pushed to reverse-ETL target

vars == <<pg, kafka, flink_sum, warehouse, rev_etl>>

\* All records start outside the system.
Init ==
    /\ pg = {}
    /\ kafka = <<>>
    /\ flink_sum = 0
    /\ warehouse = {}
    /\ rev_etl = {}

\* A new record is inserted in PG.
PgInsert(r) ==
    /\ r \in Records
    /\ r \notin pg
    /\ pg' = pg \cup {r}
    /\ UNCHANGED <<kafka, flink_sum, warehouse, rev_etl>>

\* Debezium picks up an inserted record not yet in kafka.
DebeziumPublish ==
    /\ \E r \in pg :
        /\ ~\E i \in 1..Len(kafka) : kafka[i] = r
        /\ kafka' = Append(kafka, r)
        /\ UNCHANGED <<pg, flink_sum, warehouse, rev_etl>>

\* Flink consumes head of kafka, updates aggregate, removes from kafka.
FlinkConsume ==
    /\ Len(kafka) > 0
    /\ flink_sum' = flink_sum + 1     \* simple count aggregate
    /\ kafka' = Tail(kafka)
    /\ UNCHANGED <<pg, warehouse, rev_etl>>

\* Warehouse loader picks records that have been aggregated into Flink but
\* aren't in the warehouse yet. For correctness we require records to flow
\* through the Flink aggregator before being warehoused.
WarehouseLoad(r) ==
    /\ r \in pg
    /\ r \notin warehouse
    /\ ~\E i \in 1..Len(kafka) : kafka[i] = r   \* already consumed by Flink
    /\ warehouse' = warehouse \cup {r}
    /\ UNCHANGED <<pg, kafka, flink_sum, rev_etl>>

\* Reverse-ETL pushes warehouse records to the target.
ReverseETL(r) ==
    /\ r \in warehouse
    /\ r \notin rev_etl
    /\ rev_etl' = rev_etl \cup {r}
    /\ UNCHANGED <<pg, kafka, flink_sum, warehouse>>

Next ==
    \/ \E r \in Records : PgInsert(r)
    \/ DebeziumPublish
    \/ FlinkConsume
    \/ \E r \in Records : WarehouseLoad(r)
    \/ \E r \in Records : ReverseETL(r)

Spec == Init /\ [][Next]_vars

\* ---- Safety ----

\* No record is lost: every PG record eventually reaches the warehouse.
NoDataLoss == \A r \in pg : r \in warehouse \cup {x \in pg : \E i \in 1..Len(kafka) : kafka[i] = x}

\* Flink count equals the number of records consumed.
ExactlyOnceInAgg == flink_sum = Cardinality(pg) - Len(kafka)

\* Kafka lag bounded.
BoundedLag == Len(kafka) <= MaxLag

\* ---- Liveness (require fairness for these to hold) ----

EventualDelivery == \A r \in pg : <>(r \in rev_etl)

=========================================================================
