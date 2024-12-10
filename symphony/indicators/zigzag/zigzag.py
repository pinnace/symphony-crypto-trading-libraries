from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_utils import schema

# https://www.mql5.com/en/articles/2774
# http://www.fxcorporate.com/help/MS/NOTFIFO/i_ZigZag.html
@schema(filename='indicators/zigzag/zigzag.json')
def zigzag(flow: dict, depth: int = 12, deviation_perc: int = 0.015) -> dict:
    highs = IndicatorKit.get_points(flow, "high")
    lows = IndicatorKit.get_points(flow, "low")

    trend = [0]
    last_pivot_t = [0]
    zigzag_peak = [0]
    zigzag_valley = [0]

    for i, high, low in enumerate(zip(highs, lows)):
        # Init logic. Trend will only be 0 at beginning
        if i == 0:
            trend.append(0)
            last_pivot_t.append(0)
            zigzag_peak.append(0)
            zigzag_valley.append(0)

        # Brute force search for the first zigzag
        if 1.0 not in zigzag_peak and 1.0 not in zigzag_valley:
            for j in range(1, i):
                for x in range(0, j):
                    if high[j] >= high[x] * (1 + deviation_perc):
                        pass








