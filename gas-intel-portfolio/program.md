# Portfolio Optimizer — Active Program

## Current state
SP6 is not yet implemented.
The project is still upstream of true portfolio optimization, but the foundations are much stronger than before:

- SP1 and SP2 are alive
- network deliverability is explicit
- corridor stress can be observed and simulated

That means portfolio optimization should be staged, not started as a full LP/QP from day one.

## Active objective
Prepare SP6 to optimize commercial exposure against physical deliverability, not just financial spread.

## Required inputs before full optimization
- stable SP3 delivered-cost / spread layer
- first SP4 scenario library
- route or corridor capacity budgets that are auditable

## Priority agenda

### Phase A — Descriptive portfolio feasibility
1. Build corridor-level capacity budgets by month.
2. Tag hypothetical client demand to destination corridors.
3. Measure which client mixes are physically feasible before scoring spread.

### Phase B — Simple optimization
4. Start with deterministic constrained selection:
   - maximize gross spread
   - subject to corridor headroom and minimum reliability
5. Only then add volatility / risk penalties.

### Phase C — Stochastic optimization
6. Use SP4 stress scenarios to penalize corridor concentration.
7. Add diversification across supply basins and route families.

## Constraint
Do not optimize a portfolio against abstract transport capacity only.
Use the canonical network and scenario layers once SP3 and SP4 are ready enough.

## Immediate next step
Wait for the first real SP3 corridor-cost table and SP4 scenario set, then implement a feasibility-first optimizer.
