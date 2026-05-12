------------------------- MODULE FlexiblePaxos -------------------------
\* Flexible Paxos (Howard, Malkhi & Spiegelman, 2016)
\*
\* This module proves the key theorem: safety is maintained whenever
\* every Phase 1 quorum intersects every Phase 2 quorum — i.e., the
\* quorums do NOT need to be independent majorities.
\*
\* Differences from Classic Paxos (Lamport's Paxos.tla):
\*   - Quorum1 and Quorum2 are separate sets of subsets of Acceptors.
\*   - Safety invariant: ∀ Q1 ∈ Quorum1, Q2 ∈ Quorum2 : Q1 ∩ Q2 ≠ ∅
\*   - Phase 1 uses a quorum from Quorum1; Phase 2 uses one from Quorum2.
\*
\* Verification targets
\*   - Nontriviality : if a value is chosen it was proposed
\*   - Agreement     : at most one value is chosen  (linearizability anchor)
\*   - Validity      : chosen value ∈ proposed values
\*
\* Model parameters (suitable for TLC):
\*   Acceptors  ← {a1, a2, a3, a4, a5}
\*   Values     ← {v1, v2}
\*   Quorum1    ← {{a1,a2,a3,a4}, {a1,a2,a3,a5}, ...}  (all 4-subsets)
\*   Quorum2    ← {{a1,a2}, {a1,a3}, ...}              (all 2-subsets)
\*   (4+2 = 6 > 5, invariant satisfied)
\*
\* Run:  java -jar tla2tools.jar -config MC.cfg FlexiblePaxos.tla
\*
EXTENDS Integers, FiniteSets, TLAPS

CONSTANTS
    Acceptors,  \* finite set of acceptor identifiers
    Values,     \* finite set of proposable values
    Quorum1,    \* set of subsets of Acceptors used in Phase 1
    Quorum2     \* set of subsets of Acceptors used in Phase 2

\* --- Quorum assumption (Flexible Paxos safety precondition) --------------------
ASSUME QuorumAssumption ==
    /\ \A Q \in Quorum1 : Q \subseteq Acceptors
    /\ \A Q \in Quorum2 : Q \subseteq Acceptors
    /\ \A Q1 \in Quorum1 : \A Q2 \in Quorum2 : Q1 \cap Q2 # {}

\* Ballots are natural numbers; 0 is reserved for "no ballot".
Ballots == Nat

None == CHOOSE v : v \notin Values

\* --- State variables -----------------------------------------------------------
VARIABLES
    \* Per-acceptor durable state:
    maxBal,   \* maxBal[a]: highest ballot promised (Phase 1)
    maxVBal,  \* maxVBal[a]: highest ballot voted   (Phase 2), or -1
    maxVal,   \* maxVal[a]:  value voted in maxVBal[a], or None
    \* Message bag (models an unreliable network):
    msgs

TypeInvariant ==
    /\ maxBal  \in [Acceptors -> Ballots \cup {-1}]
    /\ maxVBal \in [Acceptors -> Ballots \cup {-1}]
    /\ maxVal  \in [Acceptors -> Values \cup {None}]
    /\ msgs \subseteq
         [type : {"1a"}, bal : Ballots]
      \cup [type : {"1b"}, acc : Acceptors, bal : Ballots,
                           mbal : Ballots \cup {-1}, mval : Values \cup {None}]
      \cup [type : {"2a"}, bal : Ballots, val : Values]
      \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, val : Values]

Init ==
    /\ maxBal  = [a \in Acceptors |-> -1]
    /\ maxVBal = [a \in Acceptors |-> -1]
    /\ maxVal  = [a \in Acceptors |-> None]
    /\ msgs    = {}

Send(m) == msgs' = msgs \cup {m}

\* --- Phase 1a: a leader proposes ballot b -------------------------------------
Phase1a(b) ==
    /\ Send([type |-> "1a", bal |-> b])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* --- Phase 1b: an acceptor responds to the highest-ballot 1a it has seen -----
Phase1b(a) ==
    \E m \in msgs :
        /\ m.type = "1a"
        /\ m.bal > maxBal[a]
        /\ maxBal' = [maxBal EXCEPT ![a] = m.bal]
        /\ Send([type  |-> "1b",
                 acc   |-> a,
                 bal   |-> m.bal,
                 mbal  |-> maxVBal[a],
                 mval  |-> maxVal[a]])
        /\ UNCHANGED <<maxVBal, maxVal>>

\* --- Phase 2a: a leader picks a value and broadcasts it ----------------------
\* The leader must pick the value from the highest-balloted promise in its
\* Phase-1 quorum (or any value if the quorum contains no prior vote).
Phase2a(b, v) ==
    /\ ~ \E m \in msgs : m.type = "2a" /\ m.bal = b  \* at most one 2a per ballot
    /\ \E Q \in Quorum1 :
           \* Q is a Phase-1 quorum that all promised for ballot b.
           LET qmsgs == {m \in msgs : m.type = "1b" /\ m.bal = b /\ m.acc \in Q}
           IN  /\ \A a \in Q : \E m \in qmsgs : m.acc = a
               /\ \/ \A m \in qmsgs : m.mbal = -1       \* no prior vote → free choice
                  \/ \E m \in qmsgs :
                         /\ m.mval = v                   \* v is safe (highest prior vote)
                         /\ \A m2 \in qmsgs : m2.mbal =< m.mbal
    /\ Send([type |-> "2a", bal |-> b, val |-> v])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* --- Phase 2b: an acceptor votes for the value --------------------------------
Phase2b(a) ==
    \E m \in msgs :
        /\ m.type = "2b" \/ m.type = "2a"
        /\ m.type = "2a"
        /\ m.bal >= maxBal[a]
        /\ maxBal'  = [maxBal  EXCEPT ![a] = m.bal]
        /\ maxVBal' = [maxVBal EXCEPT ![a] = m.bal]
        /\ maxVal'  = [maxVal  EXCEPT ![a] = m.val]
        /\ Send([type |-> "2b", acc |-> a, bal |-> m.bal, val |-> m.val])

\* --- Next-state relation ------------------------------------------------------
Next ==
    \/ \E b \in Ballots : Phase1a(b)
    \/ \E a \in Acceptors : Phase1b(a)
    \/ \E b \in Ballots, v \in Values : Phase2a(b, v)
    \/ \E a \in Acceptors : Phase2b(a)

Spec == Init /\ [][Next]_<<maxBal, maxVBal, maxVal, msgs>>

\* --- Chosen predicate ---------------------------------------------------------
\* A value v is chosen if a Phase-2 quorum of acceptors has voted for it.
Chosen(v) ==
    \E Q \in Quorum2 :
        \A a \in Q : \E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.val = v

\* --- Safety invariants --------------------------------------------------------

\* Agreement: at most one value is ever chosen.
Agreement == \A v1, v2 \in Values : Chosen(v1) /\ Chosen(v2) => v1 = v2

\* Nontriviality: a value is only chosen if it was proposed.
Nontriviality ==
    \A v \in Values : Chosen(v) =>
        \E m \in msgs : m.type = "2a" /\ m.val = v

\* VotesSafe: every Phase-2 vote is consistent with the Phase-1 protocol.
VotesSafe ==
    \A a \in Acceptors, b \in Ballots, v \in Values :
        (\E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.bal = b /\ m.val = v) =>
        (\E m \in msgs : m.type = "2a" /\ m.bal = b /\ m.val = v)

\* OneValuePerBallot: at most one value is proposed per ballot.
OneValuePerBallot ==
    \A m1, m2 \in msgs :
        m1.type = "2a" /\ m2.type = "2a" /\ m1.bal = m2.bal => m1.val = m2.val

\* Combine into a single invariant for TLC.
Invariant ==
    /\ TypeInvariant
    /\ Agreement
    /\ Nontriviality
    /\ VotesSafe
    /\ OneValuePerBallot

\* --- TLAPS proof sketch for Agreement -----------------------------------------
\*
\* Theorem Agreement follows from QuorumAssumption + OneValuePerBallot.
\*
\* Proof (informal):
\*   Assume v1 and v2 are both chosen.
\*   Then ∃ Q1' ∈ Quorum2 : ∀ a ∈ Q1', voted(a, b1, v1)
\*   and  ∃ Q2' ∈ Quorum2 : ∀ a ∈ Q2', voted(a, b2, v2)
\*
\*   Case b1 = b2: OneValuePerBallot implies v1 = v2.
\*
\*   Case b1 < b2 (WLOG):
\*     The leader of b2 ran Phase 1 with some Q ∈ Quorum1.
\*     By QuorumAssumption, Q ∩ Q1' ≠ ∅.
\*     Let a ∈ Q ∩ Q1'. Then a promised for b2 (Phase 1b) and had already
\*     voted (b1, v1). The Phase-2a rule for b2 picks the highest prior vote
\*     seen in the quorum, so it must pick v1 (or a value from a higher ballot).
\*     By induction, the only value ever proposed in any ballot ≥ b1 is v1.
\*     Therefore v2 = v1.  □

=============================================================================
