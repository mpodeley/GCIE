# Supply & Sourcing Engine — Research Program

## Current objective
Minimize MAE of weighted average acquisition price ($/MMBtu) on 12-month backtest.
MAE = mean(|actual_acquisition_price - predicted_acquisition_price|)
Lower is better.

## Key context
Vaca Muerta shale oil is scaling (Bajada del Palo Oeste, La Amarga Chica, Rincón de Aranda).
Associated gas enters at near-zero marginal cost and should pressure prices down when oil ramps.
But that is only half of the market: once gas is priced at PIST, transport constraints determine whether low-cost gas can actually reach the demand center. Winter saturation on TGN/TGS corridors can widen basin spreads and raise effective acquisition cost.

## Current baseline
Implemented monthly deterministic baseline by cuenca with:
- price lags (1/3/6)
- month and quarter seasonality
- unconventional gas/oil output and shares from `pozos_no_convencional`
- FX level from `tipo_cambio`

Current realized baseline MAE on the latest 12-month backtest: about `0.30 USD/MMBtu`.
Transport congestion is now available in the data lake and already part of the next iteration plan, but the first direct inclusion attempt did not beat the kept baseline.

## Research agenda

### Phase A — Core supply model
1. Improve the current baseline with richer corridor-specific transport utilization and headroom features.
2. Restore historical `gas_asociado_ratio` by cuenca from F01 and add GOR trend (3-month rolling slope).
3. Add vm petroleum production lag3 and lag6 once `produccion_diaria` recovers basin fidelity.
4. Compare unified vs. basin-specific models for Neuquina, Austral, Noroeste and South corridor basins.

### Phase B — Supply curve modeling
5. Model explicit aggregate supply curve:
   - free gas portion at reference price X
   - asociado gas portion at processing_cost only (~0.5 USD/MMBtu)
   - Find equilibrium clearing price given demand volume and corridor headroom
6. Incorporate production capacity constraints by basin and transport constraints by gasoducto.

### Phase C — Opportunity windows
7. Build opportunity window detector:
   - Identify months where spot < firm contract cost
   - Features: inventory proxy, demand seasonality, production ramp-up rate, transport slack
   - Binary classifier: is this an opportunity window?

### Phase D — Contract mix optimization
8. Given supply forecast, model optimal firm/interrumpible/spot mix that minimizes cost subject to:
   - Minimum reliability constraint (95% of demand met)
   - Volume commitment constraints

## Constraints
- No look-ahead on petroleum production data
- No look-ahead on transport congestion; only use published or lagged monthly operational data
- Budget: 3 minutes max
- All prices must be in USD/MMBtu (use tipo_cambio table for AR$ conversion)

## Expected progress
Baseline MAE: ~0.30 USD/MMBtu
Target MAE: <10% of average acquisition price
