# Tuning `GEO_VELOCITY`: pre-registered

**Written before any tuning run.** Everything above the Results section was committed to this
file first, so the choice below is made by a rule fixed in advance, not by picking whichever
number looks best afterwards.

## The problem, from the evaluation

On all 1.85M transactions, `GEO_VELOCITY` fired 28,630 times, and 98.9% of those were
legitimate. That was **71% of all false declines**, for 3.3% of fraud caught. Every hit was a
DECLINE (score 80), and the REVIEW band went completely unused.

## Hypothesis

The same impossible speed is different evidence at different distances. Across a continent
(Berlin to São Paulo), no location noise explains it. Across a region (~150 km), a merchant
registered away from where the card was actually used explains it easily. Sparkov jitters each
merchant by up to ~1° per transaction, so two jittered points around one cardholder can be up to
roughly 300 km apart.

So: keep **DECLINE** for impossible travel over a distance no noise could explain, and treat
shorter hops as weaker evidence (`GEO_SHORT_HOP`).

## Candidates

All run on **`fraudTrain` only** (`--files=fraudTrain`). `fraudTest` is not read during tuning.

| Variant | Certain distance | `GEO_SHORT_HOP` score | Why this candidate |
|---|---:|---:|---|
| **V0** | 100 km | – | Baseline: the original behaviour (`RiskPolicy.ORIGINAL`). Every hit declines. |
| **V1** | 500 km | 45 | The hypothesis. 500 km sits clear of the ~300 km noise ceiling. A short hop alone is REVIEW. |
| **V2** | 1,000 km | 45 | Sensitivity check. If V1 and V2 barely differ, the result doesn't hinge on the exact 500. |
| **V3** | 500 km | 25 | A short hop only *contributes*. Alone it approves; it needs another rule to reach REVIEW. |

## Selection rule

1. **Objective:** the fewest false declines (legitimate purchases blocked) on `fraudTrain`.
2. **Constraint:** flagged recall (fraud either declined *or* sent to REVIEW) must not fall more
   than **1 percentage point** below V0. Moving a case from "block" to "review" is acceptable;
   losing it entirely is not.
3. **Tie-break:** if two variants are within 1% of each other on false declines, take the one
   with the smaller REVIEW queue, since reviews cost analyst time.
4. The chosen variant becomes `RiskPolicy.DEFAULT`. Then it and V0 are both run on the full
   dataset, and **only the `fraudTest` rows are reported as the result.**

Expected, stated in advance: V1 and V2 cut false declines sharply and move those cases to
REVIEW, with flagged recall unchanged. V3 likely fails the constraint, because short hops that
used to be flagged would now approve.

## Results

*(Filled in after the runs.)*
