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

The pre-registration was committed at 23:20:47 (`615197c`); the four runs finished between 23:23
and 23:38. All numbers below are **`fraudTrain` only** (1,296,675 transactions, 7,506 fraudulent).

| | False declines | Declined precision | Declined recall | Flagged recall | REVIEW queue |
|---|---:|---:|---:|---:|---:|
| **V0** baseline | 26,124 | 9.2% | 35.4% | 35.44% | 0 |
| **V1** 500 km / 45 | **8,627** | 22.8% | 33.9% | 35.84% (+0.40) | 18,149 |
| **V2** 1,000 km / 45 | 8,627 | 22.8% | 33.9% | 35.84% (+0.40) | 18,149 |
| **V3** 500 km / 25 | 7,916 | 24.3% | 33.9% | 34.36% (**−1.08**) | 2,485 |

V0 reproduces the earlier full evaluation's `fraudTrain` row exactly (2,660 / 26,124 / 4,846),
so the baseline is the same one the README reported.

### Applying the rule

1. **Constraint** (flagged recall at least 34.44%): V1 ✅, V2 ✅, **V3 ❌**, at 34.36%, 0.08
   points short. It had the fewest false declines. The rule was fixed before the runs, and it
   isn't bent now that the margin turned out small.
2. **Objective** (fewest false declines) among V1 and V2: a tie, at 8,627 each.
3. **Tie-break** (smaller REVIEW queue): also a tie, at 18,149 each.

V1 and V2 are identical because **every geo hit in this dataset is under 500 km**. The tables
show no `GEO_VELOCITY` line at all under V1 or V2, only `GEO_SHORT_HOP`. So the data can't
separate them. **V1 is chosen** because 500 km was the value derived from the noise model in
the hypothesis. V2 was only ever the sensitivity check, and it did its job: the result doesn't
depend on the exact threshold.

**Decision: `RiskPolicy.DEFAULT` = V1** (`geoCertainDistanceKm=500`, `geoShortHopScore=45`).

### What the rule didn't weigh

- **The review queue V1 creates is low-yield.** 18,149 REVIEWs contain 143 frauds
  (2,690 flagged − 2,547 declined): about **1 in 127**, barely above the 0.58% base rate. A real
  fraud team might prefer V3, trading 1.08 points of recall to avoid ~15,700 reviews. The rule
  encoded "don't lose fraud", so V1 is what it picks. Changing that trade-off is a business
  decision, and it would need a new pre-registered rule, not a quiet swap.
- **Flagged recall went *up*** in V1 (2,660 → 2,690 frauds flagged), although the change only
  softened a rule. That's the stage 2 design showing through: a declined transaction doesn't
  add to daily spend. Once short hops stop being declined, more spend accumulates, and
  `DAILY_LIMIT` catches 30 more frauds later on. The rules aren't independent, because state
  follows the verdict.

### Test-period result

The full run with `RiskPolicy.DEFAULT = V1` reproduced V1's `fraudTrain` row exactly
(2,547 / 8,627 / 4,959). The baseline column is the `fraudTest` row from the original full
evaluation, whose `fraudTrain` row V0 had already reproduced exactly. Both columns are **`fraudTest`
only** (555,719 transactions, 2,145 fraudulent), which played no part in the choice:

| `fraudTest` | Baseline (V0) | Tuned (V1) | Change |
|---|---:|---:|---|
| Legitimate purchases declined | 13,727 | **4,868** | **−64.5%** |
| Decline precision | 5.3% | **13.3%** | 2.5× |
| False-positive rate (declined) | 2.5% | **0.88%** | |
| Fraud declined (recall) | 35.9% (769) | 34.7% (744) | −1.2 points |
| Fraud declined **or** reviewed | 35.9% (769) | **36.6% (784)** | +0.7 points |
| Fraud amount declined | 54.7% | 54.2% | −0.5 points |
| REVIEW queue | 0 | 9,221 | |

**The training result held on unseen data.** Nearly two thirds fewer legitimate customers were
declined. Fraud caught, counting reviews, went slightly up rather than down, and the fraud
amount blocked barely moved.

**The cost held too, and grew.** The test period's 9,221 reviews contain 40 frauds
(784 − 744): **1 in 231**, against 1 in 127 on `fraudTrain`. Short hops are weak evidence, and
fraud is rarer in the test period. Under the pre-registered rule, V1 is the right choice. Whether
roughly 230 reviews per extra fraud found is worth paying for is a business decision. The next
experiment is a pre-registered rule that puts a price on reviews, tested against V3.
