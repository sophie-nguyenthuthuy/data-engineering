----------------------------- MODULE pipeline -----------------------------
\* TLA+ specification of the CDC -> Kafka -> Flink -> Warehouse -> Reverse-ETL
\* pipeline. Mirrors the Python state machine in src/tlavp/state/.
\*
\* Safety properties (must hold at every state):
\*   WarehouseSubsetOfPg
\*   RevETLSubsetOfWarehouse
\*   KafkaSubsetOfPg
\*   ExactlyOnceInAgg (informal — see Python impl)
\*   BoundedLag
\*
\* Liveness (eventually):
\*   EventualDelivery  : every pg record eventually ends up in rev_etl
\*   BoundedLagInfOft  : kafka lag stays bounded infinitely often

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Records, MaxLag

VARIABLES pg, kafka, kafka_consumed, warehouse, rev_etl

vars == <<pg, kafka, kafka_consumed, warehouse, rev_etl>>

Init ==
    /\ pg = {}
    /\ kafka = <<>>
    /\ kafka_consumed = <<>>
    /\ warehouse = {}
    /\ rev_etl = {}

PgInsert(r) ==
    /\ r \in Records
    /\ r \notin pg
    /\ pg' = pg \cup {r}
    /\ UNCHANGED <<kafka, kafka_consumed, warehouse, rev_etl>>

DebeziumPublish ==
    \E r \in pg :
        /\ ~ (\E i \in 1..Len(kafka) : kafka[i] = r)
        /\ kafka' = Append(kafka, r)
        /\ UNCHANGED <<pg, kafka_consumed, warehouse, rev_etl>>

FlinkConsume ==
    /\ Len(kafka) > 0
    /\ LET head == Head(kafka)
       IN  /\ kafka_consumed' = Append(kafka_consumed, head)
           /\ kafka' = Tail(kafka)
    /\ UNCHANGED <<pg, warehouse, rev_etl>>

WarehouseLoad ==
    \E r \in Records :
        /\ r \in pg
        /\ r \notin warehouse
        /\ \E i \in 1..Len(kafka_consumed) : kafka_consumed[i] = r
        /\ warehouse' = warehouse \cup {r}
        /\ UNCHANGED <<pg, kafka, kafka_consumed, rev_etl>>

ReverseETL ==
    \E r \in Records :
        /\ r \in warehouse
        /\ r \notin rev_etl
        /\ rev_etl' = rev_etl \cup {r}
        /\ UNCHANGED <<pg, kafka, kafka_consumed, warehouse>>

Next ==
    \/ \E r \in Records : PgInsert(r)
    \/ DebeziumPublish
    \/ FlinkConsume
    \/ WarehouseLoad
    \/ ReverseETL

Spec == Init /\ [][Next]_vars
            /\ WF_vars(DebeziumPublish)
            /\ WF_vars(FlinkConsume)
            /\ WF_vars(WarehouseLoad)
            /\ WF_vars(ReverseETL)

\* ---- Safety ----
WarehouseSubsetOfPg     == warehouse \subseteq pg
RevETLSubsetOfWarehouse == rev_etl \subseteq warehouse
KafkaSubsetOfPg         == \A i \in 1..Len(kafka) : kafka[i] \in pg
BoundedLag              == Len(kafka) <= MaxLag

\* ---- Liveness ----
EventualDelivery == \A r \in Records : r \in pg ~> r \in rev_etl

=========================================================================
