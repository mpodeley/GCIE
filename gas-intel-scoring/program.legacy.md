# Customer Scoring Engine — Research Program

## Current objective
Maximize Spearman correlation between model ranking and backtest oracle ranking.
Metric: EV = Spread_esperado × Volume × (1 - Prob_default) × Diversification_factor
Higher Spearman rank correlation = better client prioritization.

## IMPORTANT LIMITATION
No proprietary client history. All scoring uses:
- Public ENARGAS consumption data (aggregated by zone/segment)
- INDEC sector activity indices as proxy for industrial client health
- Hypothetical contract assumptions from contratos_venta table
Results are relative rankings, NOT absolute creditworthiness scores.

## Baseline
Simple scoring formula:
Score = (Volume_estimate × Spread_estimate) / (1 + Risk_penalty)
Risk_penalty = segment_risk_factor × zone_risk_factor

Segment risk factors (initial heuristic, to be refined):
- residential: 0.1 (regulated, stable)
- commercial: 0.2
- industrial: 0.3 (economic cycle exposure)
- gnc: 0.25 (transport policy risk)
- central_electrica: 0.35 (dispatch uncertainty)

## Research agenda

### Phase A — Baseline
1. Implement baseline scoring formula. Establish Spearman correlation on backtest.
2. Analyze ranking: which segments/zones dominate top 10 / bottom 10?

### Phase B — Volume estimation improvement
3. Use SP1 (Demand Forecast) volume estimates instead of historical averages.
4. Add seasonality adjustment: client value varies by season (industrial = stable, residential = winter-heavy).

### Phase C — Risk factor refinement
5. Add zone-level risk: zones with high HDD volatility → higher demand uncertainty.
6. Add sector EMAE proxy: industrial clients in declining sectors → higher default risk.
7. Add concentration penalty: diversification_factor = 1 / (1 + portfolio_concentration_hhi).

### Phase D — Spread integration
8. Use SP3 (Pricing Engine) per-client spread estimates instead of average spread.
9. Segment-specific spread: residential may have lower spread but lower risk.

### Phase E — Portfolio effects
10. Score incremental client value: how does adding client X change portfolio Sharpe?
    (Requires SP6 integration — deferred to Phase 3.)

## Constraints
- No proprietary data: only use public + hypothetical sources
- Budget: 2 minutes max per experiment
- Rankings must be explainable (no black-box scores)

## Expected progress
Baseline Spearman: TBD
Target: Spearman > 0.7 vs. backtest oracle
