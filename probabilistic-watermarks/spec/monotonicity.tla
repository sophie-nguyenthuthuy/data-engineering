--------------------------- MODULE monotonicity ---------------------------
\* Watermark monotonicity invariant.
\* Variables:
\*   W           : current watermark (non-decreasing)
\*   safeDelay   : per-key (1-δ) delay quantile (non-decreasing)
\*   now         : latest observed arrival time
\* Action:
\*   ObserveRecord(k, e, a)  -- update safeDelay and W
\*
\* The safety property we prove: W never decreases.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Keys, MaxTime, LambdaMin

VARIABLES W, safeDelay, rate, now

vars == <<W, safeDelay, rate, now>>

\* Initial state.
Init ==
    /\ W = 0
    /\ safeDelay = [k \in Keys |-> 0]
    /\ rate = [k \in Keys |-> 0]
    /\ now = 0

\* Active keys participate in W.
ActiveKeys ==
    {k \in Keys : rate[k] >= LambdaMin}

\* Compute candidate watermark from delivered events.
NextW(t) ==
    IF ActiveKeys = {}
    THEN W
    ELSE LET cands == { t - safeDelay[k] : k \in ActiveKeys }
         IN  Min(cands \cup {W})       \* never decrease

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* Action: a new record arrives, updating safeDelay for k.
\* `delay_estimate` is the new (1-δ)-quantile candidate, which we clamp
\* to be non-decreasing.
ObserveRecord(k, t, delay_estimate) ==
    /\ k \in Keys
    /\ t > now
    /\ now' = t
    /\ safeDelay' = [safeDelay EXCEPT ![k] = IF delay_estimate > @ THEN delay_estimate ELSE @]
    /\ rate' = [rate EXCEPT ![k] = @ + 1]  \* abstract rate update
    /\ W' = LET cand == Min({t - safeDelay'[m] : m \in ActiveKeys} \cup {W})
            IN  IF cand > W THEN cand ELSE W

Next ==
    \E k \in Keys, t \in 1..MaxTime, d \in 0..MaxTime : ObserveRecord(k, t, d)

Spec == Init /\ [][Next]_vars

\* ---- Safety: watermark is monotone non-decreasing ----
WatermarkMonotone == [][W' >= W]_vars

\* ---- Safety: per-key safeDelay is monotone ----
SafeDelayMonotone ==
    \A k \in Keys: [][safeDelay'[k] >= safeDelay[k]]_vars

\* ---- A late record never causes W to retreat ----
NoLateRecordRegression ==
    \A k \in Keys, t \in 1..MaxTime :
        (t < W) => UNCHANGED W
=========================================================================
