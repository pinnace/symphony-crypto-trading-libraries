import pandas as pd
import turicreate as tc
from turicreate import config as tcconfig
import random
import pathlib
import pickle
from typing import List, Union, Optional, Tuple, NewType, Dict, Any
from symphony.borg import Borg
from symphony.backtest.results import ResultsHelper
from symphony.enum import Column
from symphony.indicator_v2 import IndicatorRegistry
from .model_trainer import ModelTrainer, LogisticClassifier, BoostedTreesClassifier, RandomForestClassifier
from symphony.utils.aws import s3_file_exists, s3_create_folder, s3_upload_python_object, upload_dataframe_to_s3, s3_list_folders, s3_download_python_object
from .constants import data_columns, label_column, legacy_column_mapping
from symphony.utils import standardize_index
from symphony.data_classes import PriceHistory
from symphony.indicator_v2.demark import td_range_expansion_index, td_demarker_I, td_demarker_II, td_pressure
from symphony.indicator_v2.candlestick import candlesticks
from symphony.indicator_v2.oscillators import derivative_oscillator, zig_zag, get_harmonics_name, get_closest_harmonic
from symphony.indicator_v2.volatility import mass_index, atr, bollinger_bands
from symphony.indicator_v2.trend import adx, sma
from symphony.indicator_v2.demark import td_upwave, td_downwave, td_buy_setup, td_sell_setup, td_buy_countdown, td_sell_countdown, td_buy_9_13_9, td_sell_9_13_9, \
    bullish_price_flip, bearish_price_flip, td_buy_combo, td_sell_combo, td_differential, td_anti_differential, td_reverse_differential, td_clop, td_clopwin, td_open, td_trap, td_camouflage
from symphony.exceptions import MLException
from turicreate import config as tcconfig
from symphony.indicator_v2.demark.helpers import is_oversold, is_overbought
from symphony.config import ML_S3_BUCKET, AWS_REGION, ML_LOCAL_PATH
from sklearn.metrics import roc_auc_score
import concurrent.futures
from time import sleep
from concurrent.futures._base import ALL_COMPLETED
import logging


class DemarkClassifier:

    def __init__(self, use_s3: Optional[bool] = True):
        self.use_s3 = use_s3
        self.models = []
        self.symbols = []
        self.train_data = None
        self.test_data = None
        self.symbols_filename = "best_symbols"
        tcconfig.set_runtime_config('TURI_S3_REGION', AWS_REGION)
        tc.config.set_runtime_config('TURI_FILEIO_INSECURE_SSL_CERTIFICATE_CHECKS', 1)
        tc.config.set_runtime_config('TURI_CACHE_FILE_LOCATIONS', '/tmp/')

    def load_models(self, strategy_name: str, version_folder: Optional[str] = "latest") -> None:
        """
        Load the trained models from an S3 bucket or locally
        :param strategy_name: The strategy name
        :param version_folder: The model version
        :return: None
        """
        if self.use_s3:
            s3_base_path = f"s3://{ML_S3_BUCKET}/{strategy_name}/{version_folder}/saved_models/"
            s3_models_path = s3_base_path + "models/"
            s3_symbols_path = s3_base_path + self.symbols_filename + ".pkl"

            model_folders = s3_list_folders(s3_models_path)
            model_folders = [model_folder for model_folder in model_folders if model_folder.endswith(".model") or model_folder.endswith(".model/")]

            symbols = s3_download_python_object(s3_symbols_path)
            self.symbols = symbols
            print(f"Allowed symbols: {self.symbols}")

            def get_model(model_folder=None):
                print(f"Fetching {model_folder}")
                model = tc.load_model(model_folder)
                print(f"Fetched model {model_folder}")
                return model
            """
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                for model_folder in model_folders:
                    futures.append(executor.submit(get_model, model_folder=model_folder))
                concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

            self.models = [future.result() for future in futures]
            """
            for model_folder in model_folders:
                model = get_model(model_folder)
                self.models.append(model)
        else:
            base_path = pathlib.Path(ML_LOCAL_PATH) / f"{strategy_name}/" / f"{version_folder}" / "saved_models"
            models_path = base_path / "models/"
            model_paths = [path for path in sorted(models_path.iterdir()) if path.is_dir()]
            for path in model_paths:
                print(f"Loading {path}")
                self.models.append(tc.load_model(str(path)))

            symbols_path = str(base_path) + f"/{self.symbols_filename}.pkl"
            with open(symbols_path, "rb") as f:
                self.symbols = pickle.load(f)

            self.train_data = tc.SFrame(str(base_path / "train_data.sframe"))
            self.test_data = tc.SFrame(str(base_path / "test_data.sframe"))

        return

    def apply_indicators(self, price_history: PriceHistory) -> PriceHistory:
        """
        Applies necessary indicators.

        :param price_history: The symbol's price history
        :return: PriceHistory
        """
        price_history = td_range_expansion_index(price_history)
        price_history = td_demarker_I(price_history)
        price_history = td_pressure(price_history)
        price_history = candlesticks(price_history)
        price_history = derivative_oscillator(price_history)
        price_history = zig_zag(price_history)
        price_history = adx(price_history)
        price_history = mass_index(price_history)
        price_history = zig_zag(price_history)
        price_history = atr(price_history, normalized=True)
        price_history = td_upwave(price_history, log_level=logging.INFO)
        price_history = td_downwave(price_history, log_level=logging.INFO)
        price_history = td_differential(price_history)
        price_history = td_anti_differential(price_history)
        price_history = td_clop(price_history)
        price_history = td_clopwin(price_history)
        price_history = td_open(price_history)
        price_history = td_trap(price_history)
        price_history = td_camouflage(price_history)
        price_history = bollinger_bands(price_history)
        price_history = sma(price_history, period=50)
        price_history = sma(price_history, period=200)
        return price_history

    def apply_transformations(self, column_sframe: tc.SFrame) -> tc.SFrame:
        """
        Applies necessary column transformations

        :param column_sframe: Prediction column
        :return: Transformed column
        """
        # Cast the following as strings (categorical)'
        categorical_indicator_columns = [
            IndicatorRegistry.DWAVE_UP.value,
            IndicatorRegistry.DWAVE_DOWN.value,
            IndicatorRegistry.HARMONIC.value,
            IndicatorRegistry.TD_DIFFERENTIAL.value,
            IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value,
            IndicatorRegistry.TD_CLOP.value,
            IndicatorRegistry.TD_CLOPWIN.value,
            IndicatorRegistry.TD_OPEN.value,
            IndicatorRegistry.TD_TRAP.value,
            IndicatorRegistry.TD_CAMOUFLAGE.value
        ]
        for categorical_column in categorical_indicator_columns:
            if categorical_column in column_sframe.column_names():
                column_sframe[categorical_column] = column_sframe[categorical_column].astype(str)

        try:
            column_sframe[IndicatorRegistry.HARMONIC.value] = column_sframe[IndicatorRegistry.HARMONIC.value].apply(
                lambda x: "NO_PATTERN" if int(x) <= 0 else "BUY")
        except:
            pass

        return column_sframe

    def build_column(self, price_history: PriceHistory, index: Optional[Union[int, pd.Timestamp]] = -1, verbose: Optional[bool] = False) -> tc.SFrame:
        """
        Builds the columns used for prediction

        :param price_history: The price history with all indicators applied
        :param index: The index to analyze, defaults to latest
        :param verbose: Verbosity
        :return: The SFrame column
        """

        df = price_history.price_history
        index = standardize_index(price_history, index)

        derivative_oscillator_negative_and_increasing = False
        if 0.0 > df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value] > df.iloc[index - 1][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value]:
            derivative_oscillator_negative_and_increasing = True

        poq_window = 3
        harmonics_pattern = get_closest_harmonic(price_history, index=index)

        column = {
            "IsPerfect": True if price_history.price_history.iloc[index][IndicatorRegistry.PERFECT_BUY_SETUP.value] else False,
            IndicatorRegistry.DWAVE_UP.value: df.iloc[index][IndicatorRegistry.DWAVE_UP.value],
            IndicatorRegistry.DWAVE_DOWN.value: df.iloc[index][IndicatorRegistry.DWAVE_DOWN.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR.value: df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value: df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value],
            "DerivativeOscillatorRule": derivative_oscillator_negative_and_increasing,
            IndicatorRegistry.CANDLESTICK_PATTERN.value: df.iloc[index][IndicatorRegistry.CANDLESTICK_PATTERN.value],
            IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value: df.iloc[index][IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value],
            IndicatorRegistry.RSI.value: df.iloc[index][IndicatorRegistry.RSI.value],
            IndicatorRegistry.MASS_INDEX.value: df.iloc[index][IndicatorRegistry.MASS_INDEX.value],
            IndicatorRegistry.NATR.value: df.iloc[index][IndicatorRegistry.NATR.value],
            IndicatorRegistry.ADX.value: df.iloc[index][IndicatorRegistry.ADX.value],
            IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value: df.iloc[index][IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value],
            IndicatorRegistry.TD_POQ.value: any(df[IndicatorRegistry.TD_POQ.value].iloc[index-poq_window:index + 1] == ["BUY"] * (poq_window + 1)),
            IndicatorRegistry.TD_DEMARKER_I.value: df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index],
            "DemarkerIOversold": is_oversold(price_history, IndicatorRegistry.TD_DEMARKER_I, index=index),
            "DemarkerIOverbought": is_overbought(price_history, IndicatorRegistry.TD_DEMARKER_I, index=index),
            IndicatorRegistry.TD_PRESSURE.value: df[IndicatorRegistry.TD_PRESSURE.value].iloc[index],
            "TDPressureOversold": is_oversold(price_history, IndicatorRegistry.TD_PRESSURE, index=index),
            "TDPressureOverbought": is_overbought(price_history, IndicatorRegistry.TD_PRESSURE, index=index),
            IndicatorRegistry.ZIGZAG.value: True if df.iloc[index][IndicatorRegistry.ZIGZAG.value] != 0 else False,
            IndicatorRegistry.HARMONIC.value: harmonics_pattern,
            IndicatorRegistry.TD_DIFFERENTIAL.value: df.iloc[index][IndicatorRegistry.TD_DIFFERENTIAL.value],
            IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value: df.iloc[index][IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value],
            IndicatorRegistry.TD_CLOP.value: df.iloc[index][IndicatorRegistry.TD_CLOP.value],
            IndicatorRegistry.TD_CLOPWIN.value: df.iloc[index][IndicatorRegistry.TD_CLOPWIN.value],
            IndicatorRegistry.TD_OPEN.value: df.iloc[index][IndicatorRegistry.TD_OPEN.value],
            IndicatorRegistry.TD_TRAP.value: df.iloc[index][IndicatorRegistry.TD_TRAP.value],
            IndicatorRegistry.TD_CAMOUFLAGE.value: df.iloc[index][IndicatorRegistry.TD_CAMOUFLAGE.value],
            IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value: df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value],
            IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value: df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value],
            "BollingerOutsideClose": True if df.iloc[index][Column.CLOSE] < df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_LOWER.value] else False,
            "Trend": "UP" if df.iloc[index][IndicatorRegistry.SMA_50.value] > df.iloc[index][IndicatorRegistry.SMA_200.value] else "DOWN"
        }
        for key in column.keys():
            column[key] = [column[key]]
        if verbose:
            print(f"Column for {price_history.instrument.symbol} | {price_history.timeframe}: {column}")
        column_sframe = tc.SFrame(column)
        column_sframe = self.apply_transformations(column_sframe)
        return column_sframe

    def predict(self, price_history: PriceHistory, index: Optional[Union[int, pd.Timestamp]] = -1, verbose: Optional[bool] = False) -> float:
        """
        Runs the prediction pipeline and returns probability

        :param price_history: The price history
        :param index: The index to predict
        :return: The prediction
        """
        self.apply_indicators(price_history)
        column: tc.SFrame = self.build_column(price_history, index=index, verbose=verbose)
        probs = ModelTrainer.get_probs(self.models, column)
        averaged_probs = ModelTrainer.average_probabilities(probs)
        if len(averaged_probs) != 1:
            raise MLException(f"Unexpected length for averaged_probabilities: {averaged_probs}")
        return averaged_probs[0]


class DemarkBuySetupClassifier(DemarkClassifier, Borg):
    """
    DemarkBuySetupClassifier:
        Loads a saved model. Feed the PriceHistory to predict() to make a prediction
    """
    def __init__(self, use_s3: Optional[bool] = True, threshold: Optional[float] = 0.65):
        """
        Instantiate an instance. Borg'd.

        :param use_s3: Load model from S3 or local
        :param threshold: Optionally defined threshold, tuned to 0.65
        """
        Borg.__init__(self)
        super(DemarkBuySetupClassifier, self).__init__(use_s3=use_s3)
        self.threshold = threshold
        self.symbols = []
        return

    def load_models(self, strategy_name: Optional[str] = "DemarkBuySetup", version_folder: Optional[str] = "latest") -> None:
        super(DemarkBuySetupClassifier, self).load_models(strategy_name, version_folder=version_folder)
        return

    def apply_indicators(self, price_history: PriceHistory) -> PriceHistory:
        """
        Applies necessary indicators.

        :param price_history: The symbol's price history
        :return: PriceHistory
        """
        price_history = td_range_expansion_index(price_history)
        price_history = td_demarker_I(price_history)
        price_history = td_pressure(price_history)
        price_history = candlesticks(price_history)
        price_history = derivative_oscillator(price_history)
        price_history = zig_zag(price_history)
        price_history = adx(price_history)
        price_history = mass_index(price_history)
        price_history = zig_zag(price_history)
        price_history = atr(price_history, normalized=True)
        price_history = td_upwave(price_history, log_level=logging.INFO)
        price_history = td_downwave(price_history, log_level=logging.INFO)
        price_history = td_differential(price_history)
        price_history = td_anti_differential(price_history)
        price_history = td_clop(price_history)
        price_history = td_clopwin(price_history)
        price_history = td_open(price_history)
        price_history = td_trap(price_history)
        price_history = td_camouflage(price_history)
        price_history = bollinger_bands(price_history)
        price_history = sma(price_history, period=50)
        price_history = sma(price_history, period=200)
        return price_history

    def apply_transformations(self, column_sframe: tc.SFrame) -> tc.SFrame:
        """
        Applies necessary column transformations

        :param column_sframe: Prediction column
        :return: Transformed column
        """
        # Cast the following as strings (categorical)'
        categorical_indicator_columns = [
            IndicatorRegistry.DWAVE_UP.value,
            IndicatorRegistry.DWAVE_DOWN.value,
            IndicatorRegistry.HARMONIC.value,
            IndicatorRegistry.TD_DIFFERENTIAL.value,
            IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value,
            IndicatorRegistry.TD_CLOP.value,
            IndicatorRegistry.TD_CLOPWIN.value,
            IndicatorRegistry.TD_OPEN.value,
            IndicatorRegistry.TD_TRAP.value,
            IndicatorRegistry.TD_CAMOUFLAGE.value
        ]
        for categorical_column in categorical_indicator_columns:
            if categorical_column in column_sframe.column_names():
                column_sframe[categorical_column] = column_sframe[categorical_column].astype(str)

        try:
            column_sframe[IndicatorRegistry.HARMONIC.value] = column_sframe[IndicatorRegistry.HARMONIC.value].apply(
                lambda x: "NO_PATTERN" if int(x) <= 0 else "BUY")
        except:
            pass

        return column_sframe

    def build_column(self, price_history: PriceHistory, index: Optional[Union[int, pd.Timestamp]] = -1, verbose: Optional[bool] = False) -> tc.SFrame:
        """
        Builds the columns used for prediction

        :param price_history: The price history with all indicators applied
        :param index: The index to analyze, defaults to latest
        :param verbose: Verbosity
        :return: The SFrame column
        """

        df = price_history.price_history
        index = standardize_index(price_history, index)

        derivative_oscillator_negative_and_increasing = False
        if 0.0 > df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value] > df.iloc[index - 1][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value]:
            derivative_oscillator_negative_and_increasing = True

        poq_window = 3
        harmonics_pattern = get_closest_harmonic(price_history, index=index)

        column = {
            "IsPerfect": True if price_history.price_history.iloc[index][IndicatorRegistry.PERFECT_BUY_SETUP.value] else False,
            IndicatorRegistry.DWAVE_UP.value: df.iloc[index][IndicatorRegistry.DWAVE_UP.value],
            IndicatorRegistry.DWAVE_DOWN.value: df.iloc[index][IndicatorRegistry.DWAVE_DOWN.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR.value: df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value: df.iloc[index][IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value],
            "DerivativeOscillatorRule": derivative_oscillator_negative_and_increasing,
            IndicatorRegistry.CANDLESTICK_PATTERN.value: df.iloc[index][IndicatorRegistry.CANDLESTICK_PATTERN.value],
            IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value: df.iloc[index][IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value],
            IndicatorRegistry.RSI.value: df.iloc[index][IndicatorRegistry.RSI.value],
            IndicatorRegistry.MASS_INDEX.value: df.iloc[index][IndicatorRegistry.MASS_INDEX.value],
            IndicatorRegistry.NATR.value: df.iloc[index][IndicatorRegistry.NATR.value],
            IndicatorRegistry.ADX.value: df.iloc[index][IndicatorRegistry.ADX.value],
            IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value: df.iloc[index][IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value],
            IndicatorRegistry.TD_POQ.value: any(df[IndicatorRegistry.TD_POQ.value].iloc[index-poq_window:index + 1] == ["BUY"] * (poq_window + 1)),
            IndicatorRegistry.TD_DEMARKER_I.value: df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index],
            "DemarkerIOversold": is_oversold(price_history, IndicatorRegistry.TD_DEMARKER_I, index=index),
            "DemarkerIOverbought": is_overbought(price_history, IndicatorRegistry.TD_DEMARKER_I, index=index),
            IndicatorRegistry.TD_PRESSURE.value: df[IndicatorRegistry.TD_PRESSURE.value].iloc[index],
            "TDPressureOversold": is_oversold(price_history, IndicatorRegistry.TD_PRESSURE, index=index),
            "TDPressureOverbought": is_overbought(price_history, IndicatorRegistry.TD_PRESSURE, index=index),
            IndicatorRegistry.ZIGZAG.value: True if df.iloc[index][IndicatorRegistry.ZIGZAG.value] != 0 else False,
            IndicatorRegistry.HARMONIC.value: harmonics_pattern,
            IndicatorRegistry.TD_DIFFERENTIAL.value: df.iloc[index][IndicatorRegistry.TD_DIFFERENTIAL.value],
            IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value: df.iloc[index][IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value],
            IndicatorRegistry.TD_CLOP.value: df.iloc[index][IndicatorRegistry.TD_CLOP.value],
            IndicatorRegistry.TD_CLOPWIN.value: df.iloc[index][IndicatorRegistry.TD_CLOPWIN.value],
            IndicatorRegistry.TD_OPEN.value: df.iloc[index][IndicatorRegistry.TD_OPEN.value],
            IndicatorRegistry.TD_TRAP.value: df.iloc[index][IndicatorRegistry.TD_TRAP.value],
            IndicatorRegistry.TD_CAMOUFLAGE.value: df.iloc[index][IndicatorRegistry.TD_CAMOUFLAGE.value],
            IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value: df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value],
            IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value: df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value],
            "BollingerOutsideClose": True if df.iloc[index][Column.CLOSE] < df.iloc[index][IndicatorRegistry.BOLLINGER_BANDS_LOWER.value] else False,
            "Trend": "UP" if df.iloc[index][IndicatorRegistry.SMA_50.value] > df.iloc[index][IndicatorRegistry.SMA_200.value] else "DOWN"
        }
        for key in column.keys():
            column[key] = [column[key]]
        if verbose:
            print(f"Column for {price_history.instrument.symbol} | {price_history.timeframe}: {column}")
        column_sframe = tc.SFrame(column)
        column_sframe = self.apply_transformations(column_sframe)
        return column_sframe

    def predict(self, price_history: PriceHistory, index: Optional[Union[int, pd.Timestamp]] = -1, verbose: Optional[bool] = False) -> float:
        """
        Runs the prediction pipeline and returns probability

        :param price_history: The price history
        :param index: The index to predict
        :return: The prediction
        """
        self.apply_indicators(price_history)
        column: tc.SFrame = self.build_column(price_history, index=index, verbose=verbose)
        probs = ModelTrainer.get_probs(self.models, column)
        averaged_probs = ModelTrainer.average_probabilities(probs)
        if len(averaged_probs) != 1:
            raise MLException(f"Unexpected length for averaged_probabilities: {averaged_probs}")
        return averaged_probs[0]


