# Portfolio Optimizer — Research Program

## Current objective
Maximize Sharpe-like ratio: Spread_total / Portfolio_volatility on 12-month backtest.
Optimizes BOTH client portfolio selection AND sourcing mix.
Higher is better.

## Baseline
Convex optimization (scipy.optimize) with constraints:
- Total volume commitment <= available transport capacity
- Firm supply >= take-or-pay obligation from sales contracts
- Max single-producer concentration: 30% of total supply

Decision variables:
- x_i: fraction of client i to include (0 or 1 for binary, relaxed to [0,1] for LP)
- y_j: volume from producer j (continuous)

Objective: maximize sum(spread_i * volume_i * x_i) - risk_penalty * volatility

## Research agenda

### Phase A — Baseline
1. Implement baseline LP/QP with scipy.optimize. Establish baseline Sharpe.
2. Analyze: which clients are always selected? Which producers?
3. Check constraint binding: is transport always the binding constraint?

### Phase B — Risk-adjusted optimization
4. Switch to mean-variance optimization: maximize E[spread] - lambda * Var[spread].
5. Test lambda values: 0 (pure return), 0.5, 1.0, 2.0 (very risk-averse).
6. Add CVaR constraint: CVaR_95 <= budget (from SP4 outputs).

### Phase C — Sourcing mix optimization
7. Optimize firm vs. spot allocation:
   - More firm: lower average cost, higher volume risk (take-or-pay)
   - More spot: higher average cost, no volume commitment
   - Find optimal mix for each season separately.
8. Multi-basin sourcing: balance Neuquina (cheap but congested in winter) vs. Austral.

### Phase D — Robust optimization
9. Worst-case optimization: maximize minimum spread across stress scenarios from SP4.
10. Scenario-based stochastic programming: optimize E[spread] subject to P(loss) <= 5%.

### Phase E — Greedy heuristics
11. Greedy client addition: add clients in order of marginal Sharpe improvement.
    Compare to optimal solution quality (should be within 5-10%).
12. Rolling re-optimization: monthly re-solve as new data arrives.

## Constraints
- Transport capacity: hard constraint (cannot exceed pipeline capacity)
- Supply reliability: P(demand met) >= 95% (from SP4 Monte Carlo)
- Single producer concentration: <= 30%
- Budget: 5 minutes max per experiment (optimization can be expensive)

## Expected progress
Baseline Sharpe: TBD
Target: Sharpe improvement >25% vs. naive equal-weight portfolio
