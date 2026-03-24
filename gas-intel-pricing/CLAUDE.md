# SP3 — Pricing & Spread Engine

## Role
Future pricing engine that converts SP1 demand and SP2 supply into commercial offers and expected spread.

## Current state
Specification only. No real baseline yet.

## What the active version should optimize
- sale price by segment or client class,
- pass-through policy versus margin capture,
- deliverability-aware pricing when transport is stressed,
- commercial competitiveness relative to market references.

## Dependency stance
SP3 should consume:
- SP1 forecasted demand,
- SP2 acquisition cost and transport-aware sourcing context,
- transport and network state from SP0,
- later, market references such as MEGSA where available.

## Design note
The pricing problem is no longer just `cost + margin`.
It should eventually price scarcity, basin basis and corridor stress, especially in winter.

## Implementation rule
Wait until SP1 and SP2 interfaces are stable enough to avoid redoing the entire contract twice.
