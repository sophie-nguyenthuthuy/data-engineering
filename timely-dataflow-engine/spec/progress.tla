----------------------------- MODULE progress -----------------------------
\* Progress-tracking invariants for timely-dataflow.
\*
\* Variables:
\*   counts : Pointstamp -> Int   (counts ≥ 0)
\*   frontier : Antichain         (minimal active pointstamps)
\*
\* Action: Update(op, ts, delta) changes counts[(op, ts)] by delta.
\* Constraint: counts never go negative.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Operators, MaxEpoch, MaxIter

Timestamps == [epoch: 0..MaxEpoch, iter: 0..MaxIter]
PointStamps == [op: Operators, ts: Timestamps]

VARIABLES counts

vars == <<counts>>

Init ==
    counts = [p \in PointStamps |-> 0]

Update(op, ts, delta) ==
    LET p == [op |-> op, ts |-> ts]
        new == counts[p] + delta
    IN  /\ new >= 0
        /\ counts' = [counts EXCEPT ![p] = new]

Next ==
    \E op \in Operators, ts \in Timestamps, delta \in {-1, +1} :
        Update(op, ts, delta)

Spec == Init /\ [][Next]_vars

\* ---- Safety: counts never go negative ----
NonNegativeCounts ==
    \A p \in DOMAIN counts : counts[p] >= 0

\* ---- A point with count = 0 is "complete" at that op ----
\* The frontier is the set of pointstamps with positive count that are
\* minimal in the partial order. We don't model frontier directly here
\* (would need a more complex spec), but the NonNegative invariant is
\* the foundation.

=========================================================================
