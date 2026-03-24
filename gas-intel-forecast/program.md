# Demand Forecast Engine — Active Program

## Current state
SP1 is implemented and running end-to-end against the local DuckDB snapshot.

- Current active metric: `weighted_mape = 0.15378441199363313`
- Current model: seasonal monthly baseline with lag priors and HDD adjustment
- Current cadence: monthly, using ENARGAS historical consumption as the observed demand proxy
- Main limitation: the target is still served/observed volume, not latent demand

## Operational context
The forecast should not be treated as pure “weather to demand”.
In winter, transport and deliverability constraints can cap served volume.
That means SP1 now sits on top of two realities:

1. latent demand signal from weather, seasonality and segment behavior
2. physical deliverability signal from the transport network

The network stack is now available in the datalake through:
- `transporte_utilizacion_mensual`
- `red_nodos_canonica`
- `red_tramos_canonica`
- `red_solver_resumen_mensual`

## Active objective
Improve SP1 while keeping it interpretable and robust on the latest validation window.

Primary metric:
- `weighted_mape`

Secondary objective:
- separate “demand wanted” from “volume actually delivered” during stressed winter months

## Current baseline contract
The kept baseline uses:
- monthly seasonality
- segment priors
- lag structure adapted to the available monthly history
- HDD and temperature adjustments

It is intentionally simple and stable.
No change should replace it unless it improves the validation metric on the real snapshot.

## Priority agenda

### Phase A — Demand vs deliverability
1. Add regime features from network stress:
   - corridor congestion flags
   - `unmet_withdrawal_mm3_dia` from `red_solver_resumen_mensual`
   - winter stress indicator by month
2. Train a two-stage approach:
   - latent demand baseline
   - served volume adjustment under transport stress
3. Compare one unified model vs. winter-specific correction layer.

### Phase B — Better weather structure
4. Test alternate HDD bases and interactions by segment.
5. Add smoother temperature seasonality instead of only discrete month effects.
6. Revisit lag structure once higher-frequency or cleaner demand history appears.

### Phase C — Explainability and monitoring
7. Keep every promoted run logged in `results.tsv`.
8. Keep dashboard charts aligned with the active model contract.
9. Add segment-level error decomposition for winter vs. non-winter periods.

## Constraints
- No leakage
- No future transport data in features
- Keep runtime small and interpretation clear
- Do not confuse observed delivered volume with latent demand without labeling it explicitly

## Immediate next step
Use network stress outputs as a correction layer on top of the current monthly baseline, not as a direct replacement of the core seasonality model.
