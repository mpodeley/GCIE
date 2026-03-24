# Risk Engine — Research Program

## Current objective
Minimize CVaR at 95% of total imbalance cost + hedging cost over 12-month Monte Carlo simulation (1000 scenarios).
CVaR_95 = mean of worst 5% of scenarios.
Lower is better.

## Baseline
Monte Carlo simulation with independent calibrated distributions:
- Demand shock: N(0, sigma_demand) where sigma calibrated from Demand Forecast historical errors
- Supply shock: N(0, sigma_supply) for acquisition cost variability
- Provider failure: Bernoulli(p=0.05) per quarter → forced spot purchase at MEGSA + 20%
- Transport cut: Bernoulli(p=0.10) in winter, Bernoulli(p=0.02) otherwise
- Gas asociado ratio shock: tied to crude price (WTI proxy)

## Research agenda

### Phase A — Distribution calibration
1. Calibrate demand error distribution from SP1 results.tsv (use actual forecast errors).
2. Calibrate supply cost variability from SP2 outputs.
3. Verify baseline CVaR_95 value. Document.

### Phase B — Correlation structure
4. Add demand-supply correlation: cold snaps → higher demand AND lower free gas supply.
   Implement via Gaussian copula or simple bivariate normal.
5. Add crude price → gas asociado ratio correlation (WTI proxy).
6. Test sensitivity: how much does correlation structure change CVaR vs. independent model?

### Phase C — Stress scenarios
7. Scenario: WTI drops to $50 → less petroleum production → less gas asociado → higher acquisition price.
   Quantify impact on annual P&L.
8. Scenario: TGS/TGN maintenance (pipeline cut) during winter peak. Probability + severity calibrated from `transporte_utilizacion_mensual`.
9. Scenario: Plan Gas.Ar suspension/modification → loss of incentive prices.
10. Scenario: Extreme cold snap (July 2007 repeat) — what is cost of meeting demand?

### Phase D — Hedging strategies
11. Evaluate firm vs. spot contract mix as natural hedge (more firm = less spot price risk, more volume commitment risk).
12. Quantify value of maintaining 10% / 15% / 20% interruptible capacity reserve.
13. Model minimum inventory/linepack strategy.

### Phase E — Tail risk
14. Switch from Normal to Student-t distributions for fatter tails.
15. Test mixture distributions (regime-switching: normal market vs. crisis market).

## Constraints
- Monte Carlo: 1000 scenarios minimum for stable CVaR estimate
- Budget: 5 minutes max per experiment
- All costs in USD/MMBtu

## Expected progress
Baseline CVaR_95: TBD
Target: reduce CVaR_95 by >20% vs. baseline via better hedging recommendations
