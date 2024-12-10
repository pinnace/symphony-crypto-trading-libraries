from symphony.indicator_v2 import IndicatorRegistry

data_columns = [
    "IsPerfect",
    IndicatorRegistry.DWAVE_UP.value,
    IndicatorRegistry.DWAVE_DOWN.value,
    IndicatorRegistry.DERIVATIVE_OSCILLATOR.value,
    IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value,
    "DerivativeOscillatorRule",
    IndicatorRegistry.CANDLESTICK_PATTERN.value,
    IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value,
    IndicatorRegistry.MASS_INDEX.value,
    #IndicatorRegistry.NATR.value,
    IndicatorRegistry.ADX.value,
    IndicatorRegistry.RSI.value,
    IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value,
    IndicatorRegistry.TD_POQ.value,
    IndicatorRegistry.TD_DEMARKER_I.value,
    "DemarkerIOversold",
    "DemarkerIOverbought",
    IndicatorRegistry.TD_PRESSURE.value,
    "TDPressureOversold",
    "TDPressureOverbought",
    IndicatorRegistry.ZIGZAG.value,
    IndicatorRegistry.HARMONIC.value,
    IndicatorRegistry.TD_DIFFERENTIAL.value,
    IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value,
    IndicatorRegistry.TD_CLOP.value,
    IndicatorRegistry.TD_CLOPWIN.value,
    IndicatorRegistry.TD_OPEN.value,
    IndicatorRegistry.TD_TRAP.value,
    IndicatorRegistry.TD_CAMOUFLAGE.value,
    IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value,
    IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value,
    "BollingerOutsideClose",
    "Trend"
]

legacy_column_mapping = {
    "DWaveUp": IndicatorRegistry.DWAVE_UP.value,
    "DWaveDown": IndicatorRegistry.DWAVE_DOWN.value,
    "DerivativeOscillator": IndicatorRegistry.DERIVATIVE_OSCILLATOR.value,
    "DerivativeOscillatorSignal": IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value,
    "CandlestickPattern": IndicatorRegistry.CANDLESTICK_PATTERN.value,
    "CandlestickPatternDirection": IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value,
    "RSI": IndicatorRegistry.RSI.value,
    "MassIndex": IndicatorRegistry.MASS_INDEX.value,
    "NATR": IndicatorRegistry.NATR.value,
    "ADX": IndicatorRegistry.ADX.value,
    "TDREI": IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value,
    "TDPOQ": IndicatorRegistry.TD_POQ.value,
    "DemarkerI": IndicatorRegistry.TD_DEMARKER_I.value,
    "TDPressure": IndicatorRegistry.TD_PRESSURE.value,
    "IsZigZag": IndicatorRegistry.ZIGZAG.value,
    "HarmonicsPattern": IndicatorRegistry.HARMONIC.value
}
numeric_features = [
    IndicatorRegistry.DERIVATIVE_OSCILLATOR.value,
    IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value,
    IndicatorRegistry.MASS_INDEX.value,
    IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value,
    IndicatorRegistry.TD_DEMARKER_I.value,
    IndicatorRegistry.TD_PRESSURE.value,
]
categorical_features = [
    "IsPerfect",
    IndicatorRegistry.DWAVE_UP.value,
    IndicatorRegistry.DWAVE_DOWN.value,
    "DerivativeOscillatorRule",
    IndicatorRegistry.CANDLESTICK_PATTERN.value,
    IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value,
    IndicatorRegistry.TD_POQ.value,
    "DemarkerIOversold",
    "DemarkerIOverbought",
    "TDPressureOversold",
    "TDPressureOverbought",
    "IsZigZag",
    IndicatorRegistry.HARMONIC.value,

    IndicatorRegistry.TD_DIFFERENTIAL.value,
    IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value,
    IndicatorRegistry.TD_CLOP.value,
    IndicatorRegistry.TD_CLOPWIN.value,
    IndicatorRegistry.TD_OPEN.value,
    IndicatorRegistry.TD_TRAP.value,
    IndicatorRegistry.TD_CAMOUFLAGE.value,
    "BollingerOutsideClose"
]
label_column = [
    "Profitable"
]

blacklisted_symbols = [
    "EURUSDT",
    "GBPBUSD",
    "EURBUSD",
    "GBPUSDT",
    "AUDUSDT",
    "BUSDUSDT",
    "PAXGUSDT"
]