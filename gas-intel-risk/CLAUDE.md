# SP4 — Risk Engine

## Role
Future engine for operational and economic downside risk across demand, sourcing and network deliverability.

## Current state
Specification only. No live model yet.

## Risk view that should drive the implementation
- demand forecast error,
- acquisition cost volatility,
- transport congestion and cuts,
- basis widening between origin and destination,
- structural shifts in associated-gas supply,
- policy or regulatory shocks.

## Dependency stance
SP4 should sit on top of real outputs from SP1, SP2 and SP3, plus the network state from SP0.

## Design note
The transport piece is no longer optional.
Given the current project direction, winter bottlenecks and corridor failure modes should be explicit state variables, not just scenario labels.

## Implementation rule
Defer implementation until SP3 exists, unless a narrow transport-risk prototype becomes immediately useful.
