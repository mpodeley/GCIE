# Pricing & Spread Engine — Active Program

## Current state
SP3 is not yet implemented as a real engine.
The project now has enough upstream structure to begin it seriously:

- SP1 baseline is stable
- SP2 baseline is stable
- transport and canonical network layers exist
- dashboard can already expose network stress and solver state

## Why SP3 changed
The old framing assumed transport as an additive cost input.
That is too weak for this project now.

Pricing must reflect:
- acquisition cost at origin
- corridor deliverability
- seasonal scarcity and congestion
- interconnection optionality between TGS and TGN systems

## Active objective
Build the first real spread engine with explicit physical context.

Primary metric:
- simulated net spread on backtest

But before optimization, the first milestone is descriptive:
- reconstruct a plausible delivered-cost stack by corridor and destination

## Phase 1 — Foundational model
1. Define a first pricing target:
   - sale price at destination
   - implied spread vs. acquisition cost
2. Build corridor-level delivered-cost features:
   - source basin
   - interconnection path
   - stressed route count
   - effective capacity on relevant corridor
3. Start with deterministic pricing bands instead of optimization.

## Phase 2 — Basis and scarcity
4. Add congestion premium by route family, not a single transport utilization scalar.
5. Model scarcity regimes:
   - normal
   - winter stressed
   - interconnection constrained
6. Calibrate how much spread can widen when cheap Neuquina gas is not fully deliverable.

## Phase 3 — Commercial formulas
7. Compare:
   - fixed margin
   - seasonal margin
   - congestion-aware margin
   - index-linked formula
8. Add client/segment differentiation only after the corridor-level basis is believable.

## Dependencies
- SP2 kept baseline for acquisition cost
- F19 transport utilization
- F20b canonical network
- F23 solver summary
- F24 compressors and loops

## Constraint
Do not start SP3 from synthetic “spread optimization” before reconstructing the physical delivered-cost story.

## Immediate next step
Implement the first corridor-level delivered-cost table and use it as the base layer for SP3.
