# Risk Engine — Active Program

## Current state
SP4 is still unimplemented as a real engine, but the project now has much better physical inputs than originally planned.

Available inputs now include:
- SP1 forecast error behavior
- SP2 acquisition-price behavior
- transport congestion layers
- canonical network topology
- heuristic network solver outputs
- compressor and loop asset layer

This changes the risk agenda materially.

## Active objective
Build a risk engine that treats transport failure and winter congestion as structural drivers, not just generic Bernoulli shocks.

Primary metric:
- tail cost / shortage risk under stressed network conditions

## Reframed risk model
The baseline should no longer assume independent synthetic shocks only.
It should include:
- demand forecast error
- supply price variability
- corridor congestion regime
- interconnection stress
- deliverability shortfall from the network layer

## Priority agenda

### Phase A — Structural risk inputs
1. Use SP1 and SP2 residuals as empirical error inputs.
2. Use `red_solver_resumen_mensual` as a physical stress proxy.
3. Define route-family stress scenarios:
   - Centro Oeste bottleneck
   - Neuba stress
   - Norte constrained winter
   - Perito Moreno / Mercedes transfer shortfall

### Phase B — Scenario design
4. Build explicit scenario tables instead of only random parametric shocks.
5. Separate:
   - commercial risk
   - acquisition risk
   - deliverability risk
6. Quantify shortage cost under unmet demand regimes from `F23`.

### Phase C — Hedging logic
7. Evaluate how much firm transport, alternative sourcing or margin reserve is worth under stressed regimes.
8. Use F24 assets as mitigation structure, not just as descriptive metadata.

## Constraint
The first usable SP4 should be scenario-driven and explainable.
Do not jump straight into high-volume Monte Carlo without a believable physical scenario set.

## Immediate next step
Create a scenario library built from observed stressed months in `F23`, then layer probabilistic shocks on top.
