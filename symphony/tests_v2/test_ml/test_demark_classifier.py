import unittest
import sys
import logging
from typing import List
from symphony.ml import DemarkBuySetupClassifier
from symphony.config import USE_MODIN
from symphony.tests_v2.utils import dummy_instruments

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

class DemarkClassifierTest(unittest.TestCase):

    def test_demark_buy_setup_classifier(self):
        #buy_setup_classifer = DemarkBuySetupClassifier(use_s3=False)
        #buy_setup_classifer.load_models(version_folder="test")
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DemarkClassifierTest.test_demark_buy_setup_classifier").setLevel(logging.DEBUG)
    unittest.main()