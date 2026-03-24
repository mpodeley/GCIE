# Supply & Sourcing Engine — Research Program

## Current objective
Minimize MAE of weighted average acquisition price ($/MMBtu) on 12-month backtest.
MAE = mean(|actual_acquisition_price - predicted_acquisition_price|)
Lower is better.

## KEY CONTEXT
Vaca Muerta shale oil is scaling (Bajada del Palo Oeste, La Amarga Chica, Rincón de Aranda).
Associated gas enters at near-zero marginal cost. The gas_asociado_ratio table captures this.
The ratio GOR (m3 gas / m3 oil) by basin and its trend is the strongest predictor of downward price pressure.

## Baseline
LightGBM predicting monthly average acquisition price with:
- total_gas_production_by_basin
- gas_asociado_libre_ratio (KEY FEATURE from gas_asociado_ratio table)
- se_reference_price
- month, quarter (seasonality)
- vm_petroleum_production_lag1 (leading indicator of future gas asociado)
- injection_vs_capacity_ratio (congestion proxy)
- megsa_price_lag1 (historical spot reference)

## Research agenda

### Phase A — Core supply model
1. Establish baseline with above features. Document baseline MAE.
2. Add vm_petroleum_production_lag3 and lag6 (longer leading indicators).
3. Add GOR trend (3-month rolling slope of gas_asociado_ratio) as feature.
4. Split by basin (Neuquina, Austral, Noroeste) and train basin-specific models.

### Phase B — Supply curve modeling
5. Model explicit aggregate supply curve:
   - free gas portion at reference price X
   - asociado gas portion at processing_cost only (~0.5 USD/MMBtu)
   - Find equilibrium clearing price given demand volume
6. Incorporate production capacity constraints by basin.

### Phase C — Opportunity windows
7. Build opportunity window detector:
   - Identify months where spot < firm contract cost
   - Features: inventory proxy, demand seasonality, production ramp-up rate
   - Binary classifier: is this an opportunity window?

### Phase D — Contract mix optimization
8. Given supply forecast, model optimal firm/interrumpible/spot mix that minimizes cost subject to:
   - Minimum reliability constraint (95% of demand met)
   - Volume commitment constraints

## Constraints
- No look-ahead on petroleum production data
- Budget: 3 minutes max
- All prices must be in USD/MMBtu (use tipo_cambio table for AR$ conversion)

## Expected progress
Baseline MAE: TBD
Target MAE: <10% of average acquisition price
