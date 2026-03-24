# Supply & Sourcing Engine — Active Program

## Current state
SP2 is implemented and running end-to-end on the current DuckDB snapshot.

- Current active metric: `mae_usd_mmbtu = 0.29947537459945683`
- Current target: monthly `precios_boca_pozo.precio_referencia_mmbtu`
- Current kept baseline: seasonal supply-price blend with FX and unconventional output adjustments
- Current status: transport features were tested, but the first direct inclusion attempts did not beat the kept baseline

## Operational context
The supply problem is no longer just “production and FX”.
The project now has a working transport stack:

- `transporte_utilizacion_mensual`
- `red_tramos_canonica`
- `red_solver_tramos_mensuales`
- `red_compresoras_canonica`
- `red_loops_canonica`

This means SP2 can move from a cuenca-only view toward a deliverability-aware sourcing model.

## Active objective
Reduce acquisition-price MAE while preserving the economic interpretation of the model.

Primary metric:
- `mae_usd_mmbtu`

Secondary objective:
- start distinguishing price at origin from effective delivered cost under corridor stress

## Current baseline contract
The kept baseline uses:
- month / quarter seasonality
- lags `1/3/6`
- unconventional output features from `pozos_no_convencional`
- FX from `tipo_cambio`

It does not yet rely on the canonical network in the kept version.

## Priority agenda

### Phase A — Better target definition
1. Decide whether SP2 should keep forecasting pure `boca de pozo` price or move to a deliverability-adjusted acquisition cost.
2. If the target stays at PIST, use network only as regime context.
3. If the target moves toward delivered cost, incorporate transport basis explicitly.

### Phase B — Network-aware features
4. Use `red_solver_resumen_mensual` as a regime layer:
   - unmet withdrawal
   - saturated edge count
   - stressed corridor flags
5. Add corridor-specific features instead of one linear congestion adjustment.
6. Use `F24` assets as structural state:
   - active loops
   - compressor availability proxy
   - effective capacity on canonical corridors

### Phase C — Upstream fidelity
7. Recover better historical `gas_asociado_ratio` and basin fidelity from production sources.
8. Bring `produccion_diaria` and unconventional activity into a cleaner basin/corridor mapping.
9. Compare unified vs. corridor-specific submodels.

## Constraints
- No look-ahead on production or transport
- Preserve interpretability
- Prefer regime models over opaque black-box corrections
- Do not promote a worse MAE model just because it uses more physics

## Immediate next step
Run SP2 in two tracks:
1. keep the current kept baseline for `boca de pozo`
2. open a parallel experiment track for delivered-cost / basis modeling using `F23/F24`
