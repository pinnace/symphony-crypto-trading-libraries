import unittest
import sys
import logging
from typing import List
from symphony.ml import ModelTrainer, DemarkBuySetupClassifier
from symphony.ml.constants import data_columns
from symphony.config import USE_MODIN
from symphony.tests_v2.utils import dummy_instruments

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

run = True
class ModelTrainerTest(unittest.TestCase):

    def test_evaluation_pipeline(self):
        if run:
            #classifier = DemarkBuySetupClassifier(use_s3=False)
            #classifier.load_models("DemarkBuySetup")

            trainer = ModelTrainer("DemarkBuyCountdown", use_s3=True)
            models_trained_on_best_symbols, best_symbols, model_results, train_data, test_data = trainer.run_evaluation_pipeline(generalization_cycles=3, clean_sframe=False, threshold=0.55, min_precision=0.58)

            """
            trainer.clean_sframe(trainer.sframe)
            models, train_data, test_data = trainer.train_ensemble_on_symbols(classifier.symbols, trainer.sframe)
            probs = ModelTrainer.get_probs(models, test_data)
            avg_probs = ModelTrainer.average_probabilities(probs)
            breakpoint()
            results = trainer.evaluate_model(avg_probs, test_data, threshold=0.6)
            """
            #models = trainer.train_ensemble(trainer.sframe)
            #trainer.save_to_s3(models_trained_on_best_symbols, "DemarkBuySetup", s3_version_folder="test", symbols=best_symbols, results=model_results, train_data=train_data, test_data=test_data, backtest_df=trainer.rh.results_df)
            #trainer.save(models_trained_on_best_symbols, "DemarkBuySetup", version_folder="latest", symbols=best_symbols, results=model_results, train_data=train_data,
            #               test_data=test_data, backtest_df=trainer.rh.results_df, columns=data_columns)
            breakpoint()

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("ModelTrainerTest.test_evaluation_pipeline").setLevel(logging.DEBUG)
    unittest.main()