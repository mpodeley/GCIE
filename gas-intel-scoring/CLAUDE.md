# SP5 — Customer Scoring Engine

## Role
Future ranking engine for commercial targets under expected value, risk and operational fit.

## Current state
Specification only. No live baseline yet.

## Important limitation
There is still no proprietary customer history in the project.
Any first version should be explicit that scores are relative and proxy-based.

## Dependency stance
SP5 depends on upstream commercial structure from:
- SP1 demand
- SP2 acquisition context
- SP3 pricing
- SP4 risk

## Design note
Customer quality should not ignore deliverability.
A client that looks attractive on paper but sits behind a stressed corridor in winter is not equivalent to one that can be served reliably.

## Implementation rule
Do not build this as a pure tabular ranking detached from network and sourcing constraints.
