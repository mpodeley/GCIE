# Pricing & Spread Engine — Research Program

## Current objective
Maximize average simulated net spread ($/MMBtu) on 12-month backtest.
Spread = Price_sale - Cost_acquisition - Transport - Tolls
Constraint: sale price cannot exceed MEGSA price + 5%.
Higher is better.

## Baseline
Deterministic parametric model:
Price_sale = Cost_acquisition + Transport + Tolls + Margin_target
Initial Margin_target: 0.30 USD/MMBtu (to be calibrated from historical data).

## Research agenda

### Phase A — Baseline calibration
1. Establish baseline spread from historical data. Calculate typical transport + tolls per segment.
2. Verify MEGSA price constraint is binding in what % of periods.
3. Document baseline mean spread and volatility.

### Phase B — Seasonal adjustments
4. Add seasonal margin: winter premium (higher demand, tighter supply → more pricing power).
5. Add cold snap bonus: detect HDD spikes and apply dynamic margin adjustment.
6. Test month-specific margins vs. season-specific vs. continuous function.

### Phase C — Segment differentiation
7. Price by segment: residential (less elastic) vs. industrial (more elastic, more competitive).
8. Volume tiers: discount for large industrial clients in exchange for take-or-pay commitment.
9. GNC-specific pricing (highly price-sensitive segment).

### Phase D — Gas asociado pass-through strategy
10. When gas asociado sourcing is cheap, test 3 strategies:
    - Strategy A: pass 100% savings to client (price competitive, grow volume)
    - Strategy B: partial pass-through (keep 50% of savings as extra margin)
    - Strategy C: maintain price, maximize margin
    - Simulate each strategy's impact on total portfolio spread over 12 months.

### Phase E — MEGSA-indexed formulas
11. Formula pricing: Price = MEGSA_ref × factor + fixed_component.
    Test different factor values (0.90, 0.95, 1.00, 1.05).
12. Evaluate lagged MEGSA index vs. current (removes volatility, but less accurate).

## Constraints
- Sale price <= MEGSA price + 5% (competitiveness constraint — hard constraint)
- Must use upstream Supply Engine acquisition cost (do not override with manual values)
- Budget: 2 minutes max per experiment

## Expected progress
Baseline spread: TBD
Target: improve spread by >15% vs. naive fixed-margin baseline
