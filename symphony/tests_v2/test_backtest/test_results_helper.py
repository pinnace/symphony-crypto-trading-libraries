import unittest
import sys
import logging
from typing import List
from symphony.backtest.results import ResultsHelper
from symphony.config import USE_MODIN, ML_S3_BUCKET

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

class ResultsHelperTest(unittest.TestCase):


    def test_write_to_s3(self):
        rh = ResultsHelper("Demark", use_s3=False)

        ResultsHelper.write_to_s3(
            rh.results_df, ml_bucket=ML_S3_BUCKET, ml_folder=rh.s3_folder, strategy="demark"
        )
        df = ResultsHelper.load_from_s3(
            ml_bucket=ML_S3_BUCKET, ml_folder=rh.s3_folder, strategy="demark"
        )
        self.assertIsInstance(df, pd.DataFrame)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("ResultsHelperTest.test_write_to_s3").setLevel(logging.DEBUG)
    unittest.main()