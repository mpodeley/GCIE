# Customer Scoring Engine — Active Program

## Current state
SP5 is not yet implemented.
The original scoring outline is still directionally useful, but it now needs to reflect physical deliverability, not just volume and spread.

## Active objective
Build a customer prioritization layer that ranks opportunities by:
- expected economic value
- corridor feasibility
- risk under stressed network conditions

## Why this changed
With the canonical network now in place, two customers with similar demand and spread may have very different operational value if one sits behind a constrained corridor and the other does not.

## Priority agenda

### Phase A — Feasibility-aware scoring
1. Keep the score explainable and rule-based at first.
2. Add corridor feasibility as a first-class feature:
   - route family
   - winter stress exposure
   - expected deliverability penalty
3. Use SP1 for volume expectations and SP3 later for margin expectations.

### Phase B — Portfolio-aware scoring
4. Penalize customer concentration on already stressed corridors.
5. Reward optionality where clients can be served through more than one effective route.

### Phase C — Risk-aware scoring
6. Bring SP4 scenario loss into the score only after the scenario library is real.

## Constraint
No “pure commercial” scoring divorced from the physical network.
The score should remain explainable and auditable.

## Immediate next step
Define a first corridor-aware heuristic score once SP3 exposes delivered-cost / spread by destination corridor.
