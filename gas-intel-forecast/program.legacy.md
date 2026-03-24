# Demand Forecast Engine — Research Program

## Current objective
Minimize MAPE weighted by volume on validation set (last 3 months of available data).
MAPE = sum(|actual - predicted| / actual * volume) / sum(volume)
Lower is better.

## Baseline
LightGBM with: temperature, day_of_week, month, HDD, CDD, consumption lags (7/14/28 days), segment.
Establish baseline before any modifications.

## Research agenda (in order of priority)

### Phase A — Feature engineering
1. Add temperature×segment interaction features. Hypothesis: residential has stronger temp sensitivity than industrial.
2. Add HDD with different base temperatures (15°C, 17°C, 18°C, 20°C) and select best.
3. Cyclic encoding for month and day_of_week (sin/cos transforms).
4. Add 60-day and 90-day consumption lags.
5. Add rolling std of consumption (volatility feature) at 7 and 30 day windows.

### Phase B — Segmentation
6. Train separate models per segment (residential, commercial, industrial, GNC). Compare vs. unified model.
7. Add segment×month interaction.

### Phase C — Algorithm exploration
8. XGBoost with same features as best LightGBM.
9. CatBoost with categorical features (segment, estacion).
10. Linear baseline (Ridge) for interpretability comparison.

### Phase D — Advanced features
11. Trend decomposition: add STL decomposition residuals as features.
12. Add INDEC activity index as industrial demand proxy.
13. Holiday proximity features (days before/after feriado).
14. Add transport congestion features from `transporte_utilizacion_mensual` to separate latent demand from deliverable demand in winter.

## Constraints — NEVER violate
- No data leakage: features must only use data available at prediction time
- No look-ahead: lags must be true historical lags
- Budget: 3 minutes max per experiment
- Model must be interpretable (no black-box neural nets)
- Validate on held-out last 3 months only
- Distinguish demand from deliverability: congestion may cap served volume even when latent demand is higher

## Expected progress
Baseline MAPE: TBD (establish first)
Target MAPE: <15%
Stretch target: <10%
