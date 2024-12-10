from symphony.config import USE_MODIN, BACKTEST_DIR, ML_S3_BUCKET, BACKTEST_S3_FOLDER, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import pathlib
from typing import Optional, Dict, Any, Union
from symphony.data_classes import Instrument
from symphony.enum import Exchange
from symphony.utils.aws import get_s3_resource, get_s3_path, s3_file_exists, upload_dataframe_to_s3, get_dataframe_from_s3
import boto3
import botocore
import gzip

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class ResultsHelper:

    def __init__(self,
                 strategy_class: str,
                 results_name: Optional[str] = "",
                 use_s3: Optional[bool] = False,
                 s3_folder: Optional[str] = "latest",
                 sync_on_flush: Optional[bool] = True):
        """
        Manages backtest results

        :param strategy_class: The strategy class name
        :param results_name: The optional file name of the results
        :param use_s3: Optionally load and write to S3
        :param s3_folder: The S3 folder to write to. Bucket name is globally configured.
        :param sync_on_flush: If writing to S3, optionally fetch the S3 dataframe again and merge with instance
                dataframe.  Allows for running parallel backtests.
        """

        self.use_s3 = use_s3
        self.s3_folder = s3_folder
        self.strategy_class = strategy_class
        self.results_name = results_name
        self.sync_on_flush = sync_on_flush

        instruments_file = pathlib.Path(BACKTEST_DIR + "test_data/" + "instruments.pkl")
        if instruments_file.is_file():
            import pickle
            fh = instruments_file.open("rb+")
            self.instruments = pickle.load(fh)
        else:
            self.instruments = None


        self.results_dir = pathlib.Path(BACKTEST_DIR + "results/" + strategy_class + "/")
        if results_name:
            self.df_filename = self.results_dir / (results_name + ".csv.gz")
        else:
            self.df_filename = self.results_dir / (strategy_class + ".csv.gz")

        if not self.results_dir.exists():
            self.results_dir.mkdir()

        self.df_s3_path = get_s3_path(ML_S3_BUCKET, [self.s3_folder, BACKTEST_S3_FOLDER]) + "/" + strategy_class + ".csv.gz"

        if not self.use_s3:
            if self.df_filename.exists():
                self.results_df: pd.DataFrame = pd.read_csv(str(self.df_filename), compression='gzip', index_col=False)
            else:
                self.results_df: pd.DataFrame = None
        else:
            if s3_file_exists(self.df_s3_path):
                self.results_df = ResultsHelper.load_from_s3(ml_bucket=ML_S3_BUCKET, ml_folder=self.s3_folder, strategy=self.strategy_class)
            else:
                self.results_df = None

        return

    def get_instrument(self, symbol: str) -> Instrument:
        """
        Gets an instrument from a saved object.

        :param symbol: Instrument symbol
        :return: The instrument if found
        """
        if not isinstance(self.instruments, type(None)):
            for instrument in self.instruments:
                if instrument.symbol == symbol:
                    return instrument
        return Instrument(symbol=self.symbol.replace("-", ""), exchange=Exchange.BINANCE)

    def append_result(self, row: Dict) -> None:
        """
        Appends a result to the dataframe

        :param row: Row as Dict
        :return: None
        """
        if isinstance(self.results_df, type(None)):
            self.results_df = pd.DataFrame(row, index=[0])
            return

        self.results_df = self.results_df.append(row, ignore_index=True)
        return

    def flush(self) -> None:
        """
        Write out the results to disk, and optionally to S3

        :return: None
        """

        if self.use_s3 and self.sync_on_flush:
            if s3_file_exists(self.df_s3_path):
                s3_df = ResultsHelper.load_from_s3(ml_bucket=ML_S3_BUCKET, ml_folder=self.s3_folder, strategy=self.strategy_class)
                self.results_df = pd.concat([self.results_df, s3_df], ignore_index=True)

        # Not super efficient to concat and then drop dupes instead of merging, but dropping dupes anyways
        self.results_df = self.results_df.drop_duplicates()

        if self.use_s3:
            ResultsHelper.write_to_s3(self.results_df, ml_folder=self.s3_folder, strategy=self.strategy_class)

        self.results_df.to_csv(str(self.df_filename), compression='gzip', index=False)
        return

    @staticmethod
    def write_to_s3(df: pd.DataFrame,
                    ml_bucket: Optional[str] = ML_S3_BUCKET,
                    ml_folder: Optional[str] = "latest",
                    s3_resource: Optional[Any] = None,
                    strategy: Optional[str] = "demark") -> None:

        resource, s3_path = ResultsHelper.__get_resource_and_s3_path(
            ml_bucket=ml_bucket,
            ml_folder=ml_folder,
            s3_resource=s3_resource,
            strategy=strategy
        )

        upload_dataframe_to_s3(df, s3_path, s3_resource=resource)
        return

    @staticmethod
    def load_from_s3(ml_bucket: Optional[str] = ML_S3_BUCKET,
                     ml_folder: Optional[str] = "latest",
                     s3_resource: Optional[Any] = None,
                     strategy: Optional[str] = "demark") -> Union[None, pd.DataFrame]:

        resource, s3_path = ResultsHelper.__get_resource_and_s3_path(
            ml_bucket=ml_bucket,
            ml_folder=ml_folder,
            s3_resource=s3_resource,
            strategy=strategy
        )

        if not s3_file_exists(s3_path, s3_resource=resource):
            return None

        return get_dataframe_from_s3(s3_path)

    @staticmethod
    def __get_resource_and_s3_path(
            ml_bucket: Optional[str] = ML_S3_BUCKET,
            ml_folder: Optional[str] = "latest",
            s3_resource: Optional[Any] = None,
            strategy: Optional[str] = "demark") -> Union[Any, str]:
        if isinstance(s3_resource, type(None)):
            resource = get_s3_resource()
        else:
            resource = s3_resource

        s3_path = get_s3_path(ml_bucket, [ml_folder, BACKTEST_S3_FOLDER])
        s3_path += "/" + strategy + ".csv.gz"

        return resource, s3_path
