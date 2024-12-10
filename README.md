# Crypto Trading Libraries + ML
Personal automated trading libraries, fit with my favorite indicators calculated over DataFrames and sprinkled with a bit of ML

This project is a bit old, but could be easily refactored (minus the API wrappers, as the interfaces have changed)

The project abstracts a few different things, namely

1. Quoter - A class for wrapping real-time websockets Bid/Ask from arbitrary exchanges, as well as historical quoters for OHLC data points
2. Trader - Abstract class for executing orders, including autoconverting available balances into necessary asset for pair trading using a 'conversion graph'
3. Indicators - A suite of my favorite indicators, including complex ones like Harmonic indicators and Demark indicators
4. Risk Management - A position sizer for calculating safe margin sizing with fixed risk
5. ML - machine learning model ensembler for optimizing classical trading strategies

## Some Interesting Directories

### Indicators

[Supported Indicators](https://github.com/pinnace/symphony-crypto-trading-libraries/blob/main/symphony/indicator_v2/indicator_registry.py)

[indicators](https://github.com/pinnace/symphony-crypto-trading-libraries/tree/main/symphony/indicator_v2)

Many indicators. Volatility, various oscillators, candlestick patterns, ZigZag, Harmonic Patterns, and including full implementations of all DeMark indicators.


### ML

[models](https://github.com/pinnace/symphony-crypto-trading-libraries/tree/main/symphony/ml)

Experiments with model ensembling using Boosted Trees, Random Forests, and simple neural networks to predict local minima and maxima
