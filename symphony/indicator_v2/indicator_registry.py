from enum import Enum, unique


@unique
class IndicatorRegistry(Enum):
    """All indicators should be registered here

    Indicators should be registered here along with a string
    representation. This representation can be used as a column header
    for dataframes

    """

    # Demark TD Countdown
    BULLISH_PRICE_FLIP = "bullish_price_flip"
    BEARISH_PRICE_FLIP = "bearish_price_flip"
    SELL_SETUP = "sell_setup"
    BUY_SETUP = "buy_setup"
    PERFECT_SELL_SETUP = "perfect_sell_setup"
    PERFECT_BUY_SETUP = "perfect_buy_setup"
    TDST_SUPPORT = "tdst_support"
    TDST_RESISTANCE = "tdst_resistance"
    SELL_SETUP_TRUE_END_INDEX = "sell_setup_true_end_index"
    BUY_SETUP_TRUE_END_INDEX = "buy_setup_true_end_index"
    SELL_COUNTDOWN = "sell_countdown"
    BUY_COUNTDOWN = "buy_countdown"
    AGGRESSIVE_SELL_COUNTDOWN = "aggressive_sell_countdown"
    AGGRESSIVE_BUY_COUNTDOWN = "aggressive_buy_countdown"
    BUY_COMBO = "buy_combo"
    SELL_COMBO = "sell_combo"
    SELL_9_13_9 = "sell_9_13_9"
    BUY_9_13_9 = "buy_9_13_9"
    # For Countdown, Aggressive Countdown, Combo, and 9-13-9, will keep index
    # of start of pattern for use in calculating risk levels
    PATTERN_START_INDEX = "pattern_start_index"

    # Demark D-Wave
    DWAVE_UP = "dwave_up"
    DWAVE_DOWN = "dwave_down"

    # Demark REI
    TD_RANGE_EXPANSION_INDEX = "td_rei"
    TD_POQ = "td_poq"

    # Demarker I and II
    TD_DEMARKER_I = "td_demarker_i"
    TD_DEMARKER_II = "td_demarker_ii"

    # TD Pressure
    TD_PRESSURE = "td_pressure"

    # Candlesticks
    CANDLESTICK_PATTERN = "candlestick_pattern"
    CANDLESTICK_PATTERN_DIRECTION = "candlestick_pattern_direction"

    # Derivative Oscillator
    DERIVATIVE_OSCILLATOR = "derivative_oscillator"
    DERIVATIVE_OSCILLATOR_SIGNAL = "derivative_oscillator_signal"

    # Standard Indicators
    RSI = "rsi"
    MASS_INDEX = "mass_index"

    # ZigZag
    ZIGZAG = "zigzag"
    ZIGZAG_REPAINT = "zigzag_repaints"

    # Harmonics
    HARMONIC = "harmonic"

    # ATR
    ATR = "atr"
    NATR = "natr"

    # ADX
    ADX = "adx"
    PLUS_DI = "plus_di"
    MINUS_DI = "minus_di"

    # TD Differential, Reverse Differential, Anti-Differential
    TD_DIFFERENTIAL = "td_differential"
    TD_REVERSE_DIFFERENTIAL = "td_reverse_differential"
    TD_ANTI_DIFFERENTIAL = "td_anti_differential"

    # TD Waldo
    TD_WALDO = "td_waldo"

    # TD Initiation
    TD_CAMOUFLAGE = "td_camouflage"
    TD_CLOP = "td_clop"
    TD_CLOPWIN = "td_clopwin"
    TD_OPEN = "td_open"
    TD_TRAP = "td_trap"

    # Bollinger Bands
    BOLLINGER_BANDS_LOWER = "bollinger_bands_lower"
    BOLLINGER_BANDS_UPPER = "bollinger_bands_upper"
    BOLLINGER_BANDS_WIDTH = "bollinger_bands_width"
    BOLLINGER_BANDS_PERCENT = "bollinger_bands_perc"

    # SMA
    SMA_14 = "sma_14"
    SMA_20 = "sma_20"
    SMA_50 = "sma_50"
    SMA_200 = "sma_200"