import pathlib
import pickle
import pandas as pd
import turicreate as tc
from turicreate import config as tcconfig
import random
from typing import List, Union, Optional, Tuple, NewType, Dict
from symphony.backtest.results import ResultsHelper
from symphony.enum.timeframe import integer_to_timeframe, Timeframe
from symphony.data.archivers import BinanceArchiver
from symphony.data_classes import PriceHistory
from symphony.indicator_v2 import IndicatorRegistry
from symphony.utils.aws import s3_file_exists, s3_create_folder, s3_upload_python_object, upload_dataframe_to_s3
from .constants import data_columns, label_column, legacy_column_mapping, blacklisted_symbols
from symphony.config import ML_S3_BUCKET, AWS_REGION, ML_LOCAL_PATH, USE_MODIN, config
from symphony.indicator_v2.demark import td_buy_setup, bullish_price_flip, bearish_price_flip, td_sell_setup, td_buy_countdown
from symphony.exceptions import MLException
from sklearn.metrics import roc_auc_score
from concurrent.futures._base import ALL_COMPLETED
import concurrent.futures
from multiprocessing import cpu_count
import sys

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

LogisticClassifier = NewType("LogisticClassifier", tc.logistic_classifier.LogisticClassifier)
BoostedTreesClassifier = NewType("BoostedTreesClassifier", tc.boosted_trees_classifier.BoostedTreesClassifier)
RandomForestClassifier = NewType("RandomForestClassifier", tc.random_forest_classifier.RandomForestClassifier)


class ModelTrainer:

    def __init__(self,
                 strategy: str,
                 use_s3: Optional[bool] = True,
                 apply_transformations_on_load: Optional[bool] = False):
        """
        Creates a model ensemble

        :param strategy: The strategy name
        :param use_s3: Pull data from S3
        :param apply_transformations_on_load: Apply the appropriate transformations to loaded SFrame on init
        """
        self.rh = ResultsHelper(strategy, use_s3=use_s3)
        self.use_s3 = use_s3
        self.strategy = strategy
        if self.use_s3:
            tcconfig.set_runtime_config('TURI_S3_REGION', AWS_REGION)
            self.sframe = tc.SFrame(self.rh.df_s3_path)
        else:
            self.sframe = tc.SFrame(str(self.rh.df_filename))

        if apply_transformations_on_load:
            self.sframe = self.apply_transformations(self.sframe)
        self.target_label = label_column[0]

        # Save targets
        self.symbols_filename = "best_symbols"
        self.__model_results_filename = "model_results"
        self.__train_data_filename = "train_data"
        self.__test_data_filename = "test_data"
        self.__backtest_results_filename = f"{strategy}_backtest"
        self.__columns_filename = "columns"
        return

    def apply_transformations(self, sframe: tc.SFrame):
        """
        Applies data transformations

        :param sframe: The SFrame to transform
        :return: The cleaned SFrame
        """
        sf = sframe
        try:
            sf.rename(legacy_column_mapping, inplace=True)
        except Exception as e:
            pass

        sf["Profitable"] = sf["Profitable"].apply(lambda x: 1 if x == "True" or x == 1 else 0)

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
            sf[categorical_column] = sf[categorical_column].astype(str)

        sf[IndicatorRegistry.HARMONIC.value] = sf[IndicatorRegistry.HARMONIC.value].apply(lambda x: "NO_PATTERN" if int(x) <= 0 else "BUY")
        sf["Profitable"] = sf.apply(lambda row: 0 if (row["Profitable"] == "False" or row["Profitable"] == 0) or ((row["Profitable"] == "True" or row["Profitable"] == 1) and row["PNLPerc"] < 0) else 1)

        if IndicatorRegistry.TD_PRESSURE.value in sf.column_names():
            sf = sf.fillna(IndicatorRegistry.TD_PRESSURE.value, 0.0)

        sf = sf[data_columns + label_column]
        return sf

    def clean_sframe(self, sframe: tc.SFrame) -> tc.SFrame:
        """
        Removes lines in the backtest data that do not conform to expectations

        :param sframe: The data
        :return: The clean sframe
        """
        print("Cleaning SFrame")

        def apply_indicators(price_history: PriceHistory) -> PriceHistory:
            if "demark" in self.strategy.lower():
                price_history = bullish_price_flip(price_history)
                price_history = bearish_price_flip(price_history)
                price_history = td_buy_setup(price_history)
                price_history = td_sell_setup(price_history)
            else:
                raise MLException(f"Unimplemented for {self.strategy}")

            if "buycountdown" in self.strategy.lower():
                price_history = td_buy_countdown(price_history)
            return price_history

        new_df = pd.DataFrame(dict(zip(sframe.column_names(), [[] for _ in range(len(sframe.column_names()))])))
        histories = {}
        symbols = list(self.sframe["Symbol"].unique())
        for symbol in symbols:
            histories[symbol] = {}
            unique_timeframes = self.sframe[self.sframe["Symbol"] == symbol]["Timeframe"].unique()
            for unique_timeframe in unique_timeframes:
                histories[symbol][integer_to_timeframe(unique_timeframe)] = None

        archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=True)
        futures = []

        def fetch_symbol(symbol: Optional[str] = "", timeframe: Optional[Timeframe] = None):
            for instrument in self.rh.instruments:
                if instrument.symbol == symbol:
                    symbol_instrument = instrument
            symbol_phistory = archiver.read(symbol_instrument, timeframe)
            symbol_phistory = apply_indicators(symbol_phistory)
            histories[symbol][timeframe] = symbol_phistory
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for symbol in histories.keys():
                for timeframe in histories[symbol].keys():
                    futures.append(executor.submit(fetch_symbol, symbol=symbol, timeframe=timeframe))
            concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

        rel_indicator: str = self.__get_strategy_indicator()
        for row in sframe:
            symbol = row['Symbol']
            timeframe = integer_to_timeframe(row["Timeframe"])
            df = histories[symbol][timeframe].price_history
            ts = pd.Timestamp(row["EntryTimestamp"], tz='utc')

            missing_key = False if ts in df.index else True
            missing_pattern = True if missing_key or df.loc[ts][rel_indicator] != 1 else False

            if not missing_pattern and not missing_key:
                new_df = new_df.append(row, ignore_index=True)

        return tc.SFrame(new_df)

    def __get_strategy_indicator(self) -> str:
        """
        Get the core indicator relevant to a strategy

        :return: IndicatorRegistry enum
        """
        if self.strategy == "DemarkBuySetup":
            return IndicatorRegistry.BUY_SETUP.value
        elif self.strategy == "DemarkSellSetup":
            return IndicatorRegistry.SELL_SETUP.value
        elif self.strategy == "DemarkBuyCountdown":
            return IndicatorRegistry.BUY_COUNTDOWN.value
        else:
            raise MLException(f"Unknown core indicator for strategy: {self.strategy}")

    # noinspection PyTypeChecker
    def train_ensemble(self,
                       train_data,
                       num_models: Optional[int] = 10,
                       verbose: Optional[bool] = False,
                       max_iterations: Optional[int] = 10,
                       bagging: Optional[bool] = True,
                       bagging_split: Optional[float] = 0.9,
                       early_stopping_rounds: Optional[int] = 3,
                       ) -> List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]]:
        """
        Trains an ensemble of num_models * 3. Contains Logistic, Random Forest, and BG Trees

        :param train_data: The data to train on
        :param num_models: The number of each model to train
        :param verbose: Verbosity
        :param max_iterations: Maximum number of iterations
        :param early_stopping_rounds: Stop training after this number of rounds if metric doesnt increase
        :param bagging: Whether to bag and train each model on random subset of train data split according to `bagging_split`
        :param bagging_split: % of train data to take for each model if bagging specified
        :return: The list of models
        """

        def get_sub_train_data():
            if bagging:
                sub_train_data, _ = train_data.random_split(bagging_split)
            else:
                sub_train_data = train_data
            return sub_train_data

        base_kwargs = {
            "target": self.target_label,
            "class_weights": 'auto',
            "features": data_columns,
            "verbose": verbose
        }
        addtl_kwargs = {
            "column_subsample": True,
            "max_iterations": max_iterations
        }
        logistic_kwargs = {**base_kwargs}
        bt_kwargs = {**logistic_kwargs, **addtl_kwargs, **{"early_stopping_rounds": early_stopping_rounds}}
        rf_kwargs = {**logistic_kwargs, **addtl_kwargs}

        models = [
                     tc.logistic_classifier.create(get_sub_train_data(),
                                                   **logistic_kwargs)
                     for _ in range(num_models)
                 ] + [
                     tc.boosted_trees_classifier.create(get_sub_train_data(),
                                                        **bt_kwargs)
                     for _ in range(num_models)
                 ] + [
                     tc.random_forest_classifier.create(get_sub_train_data(),
                                                        **rf_kwargs)
                     for _ in range(num_models)
                 ]
        return models

    def train_ensemble_on_symbols(self,
                                  symbols: List[str],
                                  data: tc.SFrame,
                                  split_perc: Optional[float] = 0.8,
                                  num_models: Optional[int] = 10,
                                  early_stopping_rounds: Optional[int] = 3,
                                  verbose: Optional[bool] = False
                                  ) -> Tuple[List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]], tc.SFrame, tc.SFrame]:
        """
        Trains an ensemble on a subset of symbols

        :param symbols: List of symbols to train on
        :param data: The Backtest data
        :param split_perc: The train / test split
        :param num_models: Number of models to train
        :param early_stopping_rounds: Stop training after this number of rounds if metric doesnt increase
        :param verbose: Verbosity
        :return: List of trained models, train data, test data
        """
        if not len(symbols):
            raise Exception(f"Symbols array cannot be empty")

        filtered_data = data.filter_by(symbols, 'Symbol')
        filtered_data = self.apply_transformations(filtered_data)
        train_data, test_data = filtered_data.random_split(split_perc)
        models = self.train_ensemble(train_data, num_models=num_models, verbose=verbose)
        return models, train_data, test_data

    @staticmethod
    def get_probs(models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]],
                  test_data: tc.SFrame) -> List[List[float]]:
        """
        Gets model outputs for the test data, as probability

        :param models: The trained models
        :param test_data: The data to evaluate
        :return: List of lists of probabilities
        """

        def predict(model) -> List[float]:
            return model.predict(test_data, output_type='probability', missing_value_action='error')

        return [
            predict(model) for model in models
        ]

    @staticmethod
    def evaluate_model(averaged_probs: List[float],
                       test_data: tc.SFrame,
                       threshold=0.6) -> Dict[str, Union[int, Dict[str, int]]]:
        """
        Evaluates a model. Calculates precision, recall, and other statistics

        :param averaged_probs: The averaged model outputs on test set.
        :param test_data: The test set
        :param threshold: The threshold for which a prediction can be considered True
        :return: The evaluation results
        """
        threshold = threshold
        labels = {
            "False/False": 0,
            "False/True": 0,
            "True/True": 0,
            "True/False": 0
        }
        results = {
            "precision": None,
            "recall": None,
            "specificity": None,
            "auc": None,
            "f1_score": None,
            "num_targets": None,
            "labels": None
        }

        def label_is_true(index) -> bool:
            if test_data["Profitable"][index] == "True" or test_data["Profitable"][index] == 1:
                return True
            return False

        target_labels = list(test_data["Profitable"])
        for i in range(len(averaged_probs)):
            prob = averaged_probs[i]

            if prob > threshold:
                if label_is_true(i):
                    labels["True/True"] += 1
                else:
                    labels["False/True"] += 1
            else:
                if label_is_true(i):
                    labels["True/False"] += 1
                else:
                    labels["False/False"] += 1

        total_predicted_true = labels["True/True"] + labels["False/True"]
        total_actually_true = (labels["True/True"] + labels["True/False"])
        total_false = labels["False/False"] + labels["False/True"]

        results["precision"] = 0 if total_predicted_true == 0 else labels["True/True"] / (labels["True/True"] + labels["False/True"])
        results["recall"] = 0 if total_actually_true == 0 else labels["True/True"] / (labels["True/True"] + labels["True/False"])
        if all(target_labels):
            auc_score = 1.0
        elif not any(target_labels):
            auc_score = 0.0
        else:
            auc_score = roc_auc_score(target_labels, averaged_probs)

        results["auc"] = auc_score
        results["f1_score"] = 0 if not results["precision"] and not results["recall"] else 2 * (
                (results["precision"] * results["recall"]) / (results["precision"] + results["recall"]))
        results["specificity"] = 1 if total_false == 0 else labels["False/False"] / (total_false)
        results["num_targets"] = len(target_labels)
        results["labels"] = labels
        return results

    @staticmethod
    def average_probabilities(probabilities: List[List[float]]) -> List[float]:

        averaged_probs = []
        for i in range(len(probabilities[0])):
            averaged_probs.append(sum([pred_prob[i] for pred_prob in probabilities]) / len(probabilities))
        return averaged_probs

    def evaluate_generalization(self,
                                data: tc.SFrame,
                                num_models: Optional[int] = 10,
                                split_perc: Optional[float] = 0.8,
                                early_stopping_rounds: Optional[int] = 3,
                                threshold: Optional[float] = 0.6,
                                min_examples: Optional[int] = 11,
                                verbose: Optional[bool] = False) -> Dict[str, Union[int, Dict[str, int]]]:
        """
        Holds out symbols and evaluates ensemble performance on the holdout symbol.

        :param data: Backtest data SFrame
        :param num_models: Number of models of each class to train
        :param split_perc: train/test split
        :param early_stopping_rounds: Stop training after this number of rounds if metric doesnt increase
        :param threshold: Threshold for probability to be considered true
        :param min_examples: Minimum number of training examples
        :param verbose: Verbosity
        :return:
        """
        symbols = data["Symbol"].unique()
        results = dict(zip(symbols, [None] * len(symbols)))
        for i, symbol in enumerate(symbols):

            test_data = data[data["Symbol"] == symbol]

            if symbol in blacklisted_symbols or len(test_data) < min_examples:
                del results[symbol]
                continue

            sf = data[data["Symbol"] != symbol]
            assert (len(test_data) > 0)

            train_data, _ = sf.random_split(split_perc)
            assert (len(train_data[train_data["Symbol"] == symbol]) == 0)

            test_data = self.apply_transformations(test_data)
            train_data = self.apply_transformations(train_data)

            models = self.train_ensemble(train_data, num_models=num_models, early_stopping_rounds=early_stopping_rounds, verbose=verbose)
            probs = ModelTrainer.get_probs(models, test_data)
            averaged_probs = ModelTrainer.average_probabilities(probs)
            eval_results = ModelTrainer.evaluate_model(averaged_probs, test_data, threshold=threshold)
            results[symbol] = {
                "precision": eval_results["precision"],
                "recall": eval_results["recall"],
                "auc": eval_results["auc"],
                "f1_score": eval_results["f1_score"],
                "specificity": eval_results["specificity"],
                "num_targets": eval_results["num_targets"],
                "real_success_rate": len(test_data[test_data["Profitable"] == 1]) / len(test_data)
            }

            print(f"[{i + 1}/{len(symbols)}] Evaluated {symbol}, Results: {results[symbol]}")
        return results

    def select_best_symbols(self,
                            generalization_results: Dict[str, Dict[str, Union[float, Dict[str, float]]]],
                            min_precision: Optional[float] = 0.62,
                            min_baseline_perc: Optional[float] = 0.65,
                            min_model_improvement_perc: Optional[float] = 0.15,
                            min_recall: Optional[float] = 0.1,
                            min_examples: Optional[int] = 11,
                            verbose: Optional[bool] = False) -> List[str]:
        """
        Selects the best symbols from the generalization with tuned defaults.

        :param generalization_results: The results from the generalization run
        :param min_precision: Minimum model precision for the symbol
        :param min_baseline_perc: Minimum baseline performance to be accepted as-is
        :param min_model_improvement_perc: Minimum gain over baseline
        :param min_recall: Minimum model recall
        :param min_examples: Minimum number of examples
        :param verbose: Verbosity
        :return: List of best symbols
        """
        best_symbols = []
        for symbol in generalization_results.keys():
            results = generalization_results[symbol]
            precision = results["precision"]
            recall = results["recall"]
            real_success_rate = results["real_success_rate"]

            # Skip any symbols with not enough training examples
            if results["num_targets"] < min_examples:
                continue

            # Skip any symbols where the model performs worse than real life
            if precision <= real_success_rate:
                if verbose:
                    print(f"[{symbol}] Precision less than success rate, skipping")
                continue
            else:
                model_improvement_perc = (precision - real_success_rate) / real_success_rate

            if verbose:
                print(
                    f"[{symbol}] <> Precision: {round(precision, 2)} <> Recall: {round(recall, 2)} <> Orig Accuracy: {round(real_success_rate * 100.0, 2)}% <> Baseline Improvement: {round(model_improvement_perc * 100.0, 2)}%")

            # If the symbol has good results for baseline trades and the model doesn't make precision worse, then add
            if real_success_rate >= min_baseline_perc and precision >= min_baseline_perc:
                pass
            # Skip if criteria are not met
            if precision < min_precision or recall < min_recall or model_improvement_perc < min_model_improvement_perc:
                continue

            if verbose:
                print(f"Selected [{symbol}]")
            best_symbols.append(symbol)
        return best_symbols

    def run_evaluation_pipeline(self,
                                generalization_cycles: Optional[int] = 3,
                                threshold: Optional[float] = 0.6,
                                min_precision: Optional[float] = 0.6,
                                min_model_improvement_perc: Optional[float] = 0.15,
                                min_recall: Optional[float] = 0.1,
                                min_examples: Optional[int] = 11,
                                split_perc: Optional[float] = 0.8,
                                num_models: Optional[int] = 10,
                                early_stopping_rounds: Optional[int] = 3,
                                clean_sframe: Optional[bool] = False,
                                verbose: Optional[bool] = False
                                ) -> Tuple[
        List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]], List[str], Dict[str, Union[int, Dict[str, Union[int, float]]]], tc.SFrame, tc.SFrame]:
        """
        Runs a complete evaluation pipeline.

        :param generalization_cycles: Number of generalization runs
        :param threshold: Threshold for results
        :param min_precision: Minimum precision a generalization run must reach for symbol to be included
        :param min_model_improvement_perc: Minimum model improvement % above baseline
        :param min_recall: Minimum recall
        :param min_examples: Minimum number of examples
        :param split_perc: Train / test split
        :param num_models: Number of each class of model
        :param early_stopping_rounds: Stop training after this number of rounds if metric doesnt increase
        :param clean_sframe: Cleans the SFrame. Removes non-conforming entries
        :param verbose: Verbosity
        :return: The trained models, optimal symbols, results on test set, train data, test data
        """
        if clean_sframe:
            self.sframe = self.clean_sframe(self.sframe)

        data = self.sframe
        all_generalization_results = []
        averaged_generalization_results = {}

        for _ in range(generalization_cycles):
            generalization_results = self.evaluate_generalization(data, num_models=num_models, early_stopping_rounds=early_stopping_rounds, min_examples=min_examples, verbose=verbose, threshold=threshold)
            all_generalization_results.append(generalization_results)
        """
        tc.data_structures.serialization.enable_sframe_serialization("/tmp/")
        
        futures = []
        with concurrent.futures.ProcessPoolExecutor(cpu_count() - 1) as executor:
            for _ in range(generalization_cycles):
                futures.append(executor.submit(self.evaluate_generalization, data, num_models=num_models, early_stopping_rounds=early_stopping_rounds, verbose=verbose))
            concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)
            all_generalization_results = [future.result() for future in futures]

        breakpoint()
        """

        for symbol in all_generalization_results[0].keys():
            if isinstance(all_generalization_results[0][symbol], type(None)):
                continue

            sum_precision, sum_recall, sum_specificity, sum_real_success_rate, sum_auc, sum_f1_score, sum_num_targets = 0, 0, 0, 0, 0, 0, 0
            for gen_result in all_generalization_results:

                sum_precision += gen_result[symbol]["precision"]
                sum_recall += gen_result[symbol]["recall"]
                sum_specificity += gen_result[symbol]["specificity"]
                sum_real_success_rate += gen_result[symbol]["real_success_rate"]
                sum_auc += gen_result[symbol]["auc"]
                sum_f1_score += gen_result[symbol]["f1_score"]
                sum_num_targets += gen_result[symbol]["num_targets"]


            if symbol not in averaged_generalization_results.keys():
                averaged_generalization_results[symbol] = {}
            averaged_generalization_results[symbol]["precision"] = sum_precision / len(all_generalization_results)
            averaged_generalization_results[symbol]["recall"] = sum_recall / len(all_generalization_results)
            averaged_generalization_results[symbol]["specificity"] = sum_specificity / len(all_generalization_results)
            averaged_generalization_results[symbol]["real_success_rate"] = sum_real_success_rate / len(all_generalization_results)
            averaged_generalization_results[symbol]["auc"] = sum_auc / len(all_generalization_results)
            averaged_generalization_results[symbol]["f1_score"] = sum_f1_score / len(all_generalization_results)
            averaged_generalization_results[symbol]["num_targets"] = sum_num_targets / len(all_generalization_results)

        best_symbols = self.select_best_symbols(averaged_generalization_results, min_precision=min_precision, min_model_improvement_perc=min_model_improvement_perc,
                                                min_recall=min_recall, verbose=verbose, min_examples=min_examples)
        models_trained_on_best_symbols, train_data, test_data = self.train_ensemble_on_symbols(best_symbols, data, split_perc=split_perc, num_models=num_models,
                                                                                               early_stopping_rounds=early_stopping_rounds, verbose=verbose)
        probs = ModelTrainer.get_probs(models_trained_on_best_symbols, test_data)
        averaged_probs = ModelTrainer.average_probabilities(probs)
        model_results = ModelTrainer.evaluate_model(averaged_probs, test_data, threshold=threshold)
        model_results["threshold"] = threshold
        return models_trained_on_best_symbols, best_symbols, model_results, train_data, test_data

    def save(self,
             models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]],
             strategy_name: str,
             version_folder: Optional[str] = "latest",
             symbols: Optional[List[str]] = None,
             columns: Optional[List[str]] = None,
             results: Optional[Dict[str, Union[int, Dict[str, int]]]] = None,
             train_data: Optional[tc.SFrame] = None,
             test_data: Optional[tc.SFrame] = None,
             backtest_df: Optional[pd.DataFrame] = None,
             overwrite_models: Optional[bool] = True
             ) -> None:
        """
        Saves Models and associated objects. Will save to S3 if self.use_s3 is true

        :param models: The ensemble
        :param strategy_name: The strategy name
        :param version_folder: The model version, defaults to 'latest'
        :param symbols: The symbols the model was trained on
        :param columns: Columns used in training
        :param results: The results at threshold
        :param train_data: The train data
        :param test_data: Test data.
        :param backtest_df: The backtest data
        :param overwrite_models: Whether to overwrite existing models, defaults to True
        :return: None
        """
        if self.use_s3:
            self.save_to_s3(models=models, strategy_name=strategy_name, s3_version_folder=version_folder,
                            symbols=symbols, columns=columns, results=results, train_data=train_data, test_data=test_data,
                            backtest_df=backtest_df, overwrite_models=overwrite_models)
        else:
            base_path = pathlib.Path(ML_LOCAL_PATH) / f"{strategy_name}/" / f"{version_folder}"
            models_path = base_path / "saved_models" / "models/"
            if not models_path.is_dir():
                models_path.mkdir(parents=True, exist_ok=True)

            max_digits = len(str(len(models)))
            for i, model in enumerate(models):
                model_formatted_num = format(i, f'0{max_digits}d')
                key = "Model" + model_formatted_num

                file_path = models_path / f"{key}.model"
                if not overwrite_models and pathlib.Path(file_path).exists():
                    continue

                model.save(str(file_path))

            if not isinstance(symbols, type(None)):
                symbols_path = str(base_path) + f"/{self.symbols_filename}.pkl"
                with open(symbols_path, "wb") as f:
                    pickle.dump(symbols, f)

            if not isinstance(columns, type(None)):
                columns_path = str(base_path) + f"/{self.__columns_filename}.pkl"
                with open(columns_path, "wb") as f:
                    pickle.dump(columns, f)

            if not isinstance(results, type(None)):
                results_path = str(base_path) + f"/{self.__model_results_filename}.pkl"
                with open(results_path, "wb") as f:
                    pickle.dump(results, f)

            if not isinstance(train_data, type(None)):
                train_data_path = str(base_path) + f"/{self.__train_data_filename}.sframe"
                train_data.save(train_data_path, format='binary')

            if not isinstance(test_data, type(None)):
                test_data_path = str(base_path) + f"/{self.__test_data_filename}.sframe"
                test_data.save(test_data_path, format='binary')

            if not isinstance(backtest_df, type(None)):
                backtest_df_path = str(base_path) + f"/{self.__backtest_results_filename}.csv.gz"
                backtest_df.to_csv(backtest_df_path, compression='gzip', index=False)

            return

    def save_to_s3(self,
                   models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]],
                   strategy_name: str,
                   s3_version_folder: Optional[str] = "latest",
                   symbols: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None,
                   results: Optional[Dict[str, Union[int, Dict[str, int]]]] = None,
                   train_data: Optional[tc.SFrame] = None,
                   test_data: Optional[tc.SFrame] = None,
                   backtest_df: Optional[pd.DataFrame] = None,
                   overwrite_models: Optional[bool] = True
                   ) -> None:
        """
        Uploads the models and associated outputs to S3

        :param strategy_name:
        :param models: The trained models
        :param s3_version_folder: The version of the results. E.g. `latest`
        :param symbols: Symbols the models were trained on
        :param columns: Columns used in training
        :param results: Model results
        :param train_data: Train data
        :param test_data: Test data
        :param backtest_df: DataFrame from the backtest
        :param overwrite_models: Whether to overwrite existing models, defaults to True
        :return:
        """
        s3_base_path = f"s3://{ML_S3_BUCKET}/{strategy_name}/{s3_version_folder}/saved_models/"
        s3_models_path = s3_base_path + "models/"

        if not s3_file_exists(s3_models_path):
            s3_create_folder(s3_models_path)

        max_digits = len(str(len(models)))
        for i, model in enumerate(models):
            model_formatted_num = format(i, f'0{max_digits}d')
            key = "Model" + model_formatted_num

            s3_file_path = s3_models_path + key + ".model"
            if not overwrite_models and s3_file_exists(s3_file_path):
                continue

            model.save(s3_file_path)

        if not isinstance(symbols, type(None)):
            symbols_path = s3_base_path + f"{self.symbols_filename}.pkl"
            s3_upload_python_object(symbols_path, symbols)

        if not isinstance(columns, type(None)):
            symbols_path = s3_base_path + f"{self.__columns_filename}.pkl"
            s3_upload_python_object(symbols_path, columns)

        if not isinstance(results, type(None)):
            results_path = s3_base_path + f"{self.__model_results_filename}.pkl"
            s3_upload_python_object(results_path, results)

        if not isinstance(train_data, type(None)):
            train_data_path = s3_base_path + f"{self.__train_data_filename}.sframe"
            train_data.save(train_data_path, format='binary')

        if not isinstance(test_data, type(None)):
            test_data_path = s3_base_path + f"{self.__test_data_filename}.sframe"
            test_data.save(test_data_path, format='binary')

        if not isinstance(backtest_df, type(None)):
            backtest_df_path = s3_base_path + f"{self.__backtest_results_filename}.csv.gz"
            upload_dataframe_to_s3(backtest_df, backtest_df_path)

        return
