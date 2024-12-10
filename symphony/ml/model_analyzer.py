
from symphony.ml import DemarkBuySetupClassifier
from turicreate.toolkits.classifier.boosted_trees_classifier import BoostedTreesClassifier
from turicreate.toolkits.classifier.random_forest_classifier import RandomForestClassifier
from turicreate.toolkits.classifier.logistic_classifier import LogisticClassifier

if __name__ == "__main__":
    setup_classifer = DemarkBuySetupClassifier(use_s3=False)
    setup_classifer.load_models(version_folder="no-td-initiation-no-candlesticks")

    base = None
    base_logistic = None
    logistic_models = []
    for model in setup_classifer.models:
        if type(model) == BoostedTreesClassifier or type(model) == RandomForestClassifier:
            if isinstance(base, type(None)):
                base = model.get_feature_importance().sort(["name", "index"]).to_dataframe()
                continue
            model_sframe = model.get_feature_importance().sort(["name", "index"])
            for i, row in enumerate(model_sframe):
                base["count"].iloc[i] += row["count"]
        else:
            if isinstance(base_logistic, type(None)):
                base_logistic = model.coefficients.sort(["name", "index"]).to_dataframe()
                continue
            model_sframe = model.coefficients.sort(["name", "index"])
            for i, row in enumerate(model_sframe):
                base_logistic["value"].iloc[i] += row["value"]

    base = base.sort_values(by="count", ascending=False)
    base_logistic = base_logistic.sort_values(by="value", ascending=False)
    breakpoint()