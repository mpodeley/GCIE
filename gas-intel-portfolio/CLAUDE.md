# SP6 — Portfolio Optimizer

## Role
Future optimization engine for client mix plus sourcing mix under physical and commercial constraints.

## Current state
Specification only. No live baseline yet.
This engine should not be treated as ready until SP3-SP5 produce stable contracts.

## Intended objective
Maximize portfolio quality under:
- expected spread,
- sourcing reliability,
- transport deliverability,
- seasonal balance,
- concentration and downside risk.

## Dependency stance
SP6 depends on real upstream outputs from:
- SP1 demand
- SP2 supply cost and deliverability
- SP3 commercial pricing
- SP4 risk
- SP5 customer scoring

## Design note
The old version was too abstract about transport. The active interpretation should assume deliverability and corridor stress are first-class constraints, not a late penalty term.

## Implementation rule
Do not rush implementation until the upstream engines have stable interfaces and metrics.
