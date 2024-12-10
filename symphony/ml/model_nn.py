import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, WeightedRandomSampler
from torch.autograd import Variable
import turicreate as tc
from symphony.ml import DemarkBuySetupClassifier, ModelTrainer
from symphony.config import ML_S3_BUCKET, ML_LOCAL_PATH
from symphony.utils.aws import s3_file_exists, s3_create_folder, s3_upload_python_object, s3_download_python_object
from typing import Optional, Callable, NewType, List, Union, Dict
import pathlib
import io
import wandb
from concurrent.futures._base import ALL_COMPLETED
import concurrent.futures
from multiprocessing import cpu_count

LogisticClassifier = NewType("LogisticClassifier", tc.logistic_classifier.LogisticClassifier)
BoostedTreesClassifier = NewType("BoostedTreesClassifier", tc.boosted_trees_classifier.BoostedTreesClassifier)
RandomForestClassifier = NewType("RandomForestClassifier", tc.random_forest_classifier.RandomForestClassifier)
wandb.init(project="buy-setup-optimizer")

class MetaDataset(Dataset):
    def __init__(self, sf: tc.SFrame, label_column: str = "Profitable"):
        self.df = sf.to_dataframe()
        self.labels = torch.from_numpy(
            self.df[label_column].to_numpy()
        )
        self.X = torch.from_numpy(
            self.df.drop(columns = [label_column], axis = 1).to_numpy()
        )

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        label = self.labels[idx]
        ensemble_outputs = self.X[idx]
        return [ensemble_outputs.float(), label]

# https://towardsdatascience.com/designing-your-neural-networks-a5e4617027ed
# https://machinelearningmastery.com/stacking-ensemble-for-deep-learning-neural-networks/

class DNNBinaryClassifier(nn.Module):
    def __init__(self, input_features=30, batch_size=64):
        super(DNNBinaryClassifier, self).__init__()

        hidden_nodes2 = 64
        hidden_nodes3 = 32
        hidden_nodes4 = 32
        hidden_nodes5 = 16
        hidden_nodes6 = 16
        hidden_nodes7 = 8
        dropout_ratio = 0.0
        self.fc1 = nn.Linear(input_features, batch_size)
        self.bn1d1 = nn.BatchNorm1d(batch_size, input_features)
        self.relu1 = nn.SELU()
        self.dout1 = nn.AlphaDropout(dropout_ratio)

        self.fc2 = nn.Linear(batch_size, hidden_nodes2)
        self.bn1d2 = nn.BatchNorm1d(hidden_nodes2, batch_size)
        self.relu2 = nn.SELU()
        self.dout2 = nn.AlphaDropout(dropout_ratio)

        self.fc3 = nn.Linear(hidden_nodes2, hidden_nodes3)
        self.bn1d3 = nn.BatchNorm1d(hidden_nodes3, hidden_nodes2)
        self.relu3 = nn.SELU()
        self.dout3 = nn.AlphaDropout(dropout_ratio)


        self.fc4 = nn.Linear(hidden_nodes3, hidden_nodes4)
        self.relu4 = nn.SELU()

        self.fc5 = nn.Linear(hidden_nodes4, hidden_nodes5)
        self.relu5 = nn.SELU()


        self.fc6 = nn.Linear(hidden_nodes5, hidden_nodes6)
        self.relu6 = nn.SELU()

        self.fc7 = nn.Linear(hidden_nodes6, hidden_nodes7)
        self.relu7 = nn.SELU()

        self.prelu = nn.PReLU(1)
        self.out = nn.Linear(hidden_nodes7, 1)

        # self.dout2 = nn.Dropout(0.3)
        # self.bn1d2 = nn.BatchNorm1d(hidden_nodes2, batch_size)
        # self.fc3 = nn.Linear(hidden_nodes1, hidden_nodes2)
        # self.out_act = nn.Softmax()

    def forward(self, inputs):

        x = self.fc1(inputs)
        #x = self.bn1d1(x)
        x = self.relu1(x)
        x = self.dout1(x)


        x = self.fc2(x)
        #x = self.bn1d2(x)
        x = self.relu2(x)
        x = self.dout2(x)

        x = self.fc3(x)
        #x = self.bn1d3(x)
        x = self.relu3(x)
        x = self.dout3(x)



        x = self.fc4(x)
        x = self.relu4(x)


        x = self.fc5(x)
        x = self.relu5(x)


        x = self.fc6(x)
        x = self.relu6(x)


        x = self.fc7(x)
        x = self.relu7(x)

        #x = self.prelu(x)
        x = self.out(x)

        return x

class NNHarness:
    BATCH_SIZE = 32
    EPOCHS = 200
    LEARNING_RATE = 1e-3

    def __init__(self, models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]]):
        self.models = models
        self.meta_model = None
        self.meta_model_train_data = None
        self.meta_model_test_data = None
        self.losses = None

    def transform_ensemble_data(self, train_data: tc.SFrame, test_data: Optional[tc.SFrame] = None, pre_transformation_function: Optional[Callable] = None) -> None:
        """
        High-lever function for transforming the original train data into data used to train the neural network

        :param train_data: Original train data
        :param test_data: Original test data
        :param pre_transformation_function: Optionally apply a function to transform the data before generating the NN train data
        :return: None. Stores the data in class variables. Access via meta_model_train_data / meta_model_test_data
        """

        is_none = lambda thing: isinstance(thing, type(None))

        if not is_none(pre_transformation_function):
            train_data = pre_transformation_function(train_data)
            if not is_none(test_data):
                test_data = pre_transformation_function(test_data)

        self.meta_model_train_data = NNHarness.create_meta_model_sframe(train_data, self.models)
        if not is_none(test_data):
            self.meta_model_test_data = NNHarness.create_meta_model_sframe(test_data, self.models)

        return

    @staticmethod
    def create_meta_model_sframe(target_sf: tc.SFrame, models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]]) -> tc.SFrame:
        new_sf = tc.SFrame()
        meta_rows = [NNHarness._create_pred_sframe_row(row, models) for row in target_sf]
        for row in meta_rows:
            new_sf = new_sf.append(row)
        return new_sf

    @staticmethod
    def _create_pred_sframe_row(row, models: List[Union[LogisticClassifier, RandomForestClassifier, BoostedTreesClassifier]], label_column: Optional[str] = "Profitable"):
        sf_row = {}
        max_digits = len(str(len(models)))
        for i, model in enumerate(models):
            model_formatted_num = format(i, f'0{max_digits}d')
            key = "Model" + model_formatted_num
            pred = model.predict(row, output_type='probability')[0]
            sf_row[key] = [pred]
        sf_row[label_column] = [row[label_column]]
        return tc.SFrame(sf_row)

    def train_epoch(self, meta_model: DNNBinaryClassifier, opt: torch.optim, criterion: nn.modules.loss, loader: DataLoader) -> List[List[float]]:
        """
        Trains one epoch

        :param meta_model: The meta model
        :param opt: Optimizer
        :param criterion: Loss criterion
        :param loader: DataLoader
        :return: The losses
        """
        meta_model.train()
        losses = []
        for batch_idx, (x_batch, y_batch) in enumerate(loader):
            x_batch = Variable(x_batch)
            y_batch = Variable(y_batch)

            y_batch = y_batch.float()
            opt.zero_grad()
            # (1) Forward
            y_hat = meta_model(x_batch)
            # (2) Compute diff
            loss = criterion(y_hat, y_batch.unsqueeze(1))
            # (3) Compute gradients
            loss.backward()
            #torch.nn.utils.clip_grad_norm_(meta_model.parameters(), 3)
            # (4) update weights
            opt.step()

            correct = 0
            for index, i in enumerate(y_hat):
                #i = torch.sigmoid(i)
                if i >= 0.5 and y_batch[index] == 1:
                    correct += 1

            if batch_idx % 100 == 0:
                wandb.log({"loss": loss})
            acc = correct / len(y_batch)
            losses.append(loss.data.numpy())
            print(f"Accuracy: {acc}")
        return losses

    def train_network(self, meta_model_train_data: tc.SFrame, use_sampler: Optional[bool] = False) -> DNNBinaryClassifier:
        """
        Train the Meta Model

        :param meta_model_train_data: The training data
        :param use_sampler: Optionally generated a class-weighted sampler and use it in training
        :return: The NN
        """
        df = meta_model_train_data.to_dataframe()
        # Tensor-ize dataset
        md = MetaDataset(meta_model_train_data)

        # Determine class weighting
        class_counts = [
            len(meta_model_train_data[meta_model_train_data["Profitable"] == 1]),
            len(meta_model_train_data[meta_model_train_data["Profitable"] == 0])
        ]
        num_samples = sum(class_counts)
        labels = \
            list(meta_model_train_data[meta_model_train_data["Profitable"] == 1]["Profitable"]) + \
            list(meta_model_train_data[meta_model_train_data["Profitable"] == 0]["Profitable"])
        class_weights = [num_samples / class_counts[i] for i in range(len(class_counts))]
        weights = [class_weights[labels[i]] for i in range(int(num_samples))]
        sampler = WeightedRandomSampler(torch.DoubleTensor(weights), int(num_samples))

        if use_sampler:
            loader = DataLoader(md, batch_size=NNHarness.BATCH_SIZE, shuffle=False, sampler=sampler)
        else:
            loader = DataLoader(md, batch_size=NNHarness.BATCH_SIZE, shuffle=True)

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        input_features = len(self.models)
        model = DNNBinaryClassifier(input_features=input_features, batch_size=NNHarness.BATCH_SIZE)
        wandb.watch(model, log_freq=100)
        model.to(device)

        e_losses = []
        criterion = nn.BCEWithLogitsLoss()
        optimizer = optim.AdamW(model.parameters(), lr=NNHarness.LEARNING_RATE)

        for e in range(NNHarness.EPOCHS):
            e_losses += self.train_epoch(model, optimizer, criterion, loader)

        self.meta_model = model
        self.losses = e_losses
        return model

    @staticmethod
    def predictions_for_nn_meta_model(data, meta_model, verbose=False):
        meta_model.eval()

        def make_prediction(index):
            tensor = torch.from_numpy(data.to_numpy()[index][:-1].astype(float)).float()
            pred = float(torch.sigmoid(meta_model(tensor.unsqueeze(0))))
            label = data[index]["Profitable"]
            if verbose:
                print(f"Label {'True' if label else 'False'}, Confidence: {pred}")
            return pred

        predictions = list(map(make_prediction, [i for i in range(len(data))]))
        return predictions

    @staticmethod
    def evaluate_nn_model(data: tc.SFrame, model: DNNBinaryClassifier, threshold: Optional[bool] = 0.6, verbose: Optional[bool] = False) -> Dict:
        predictions = NNHarness.predictions_for_nn_meta_model(data, model, verbose=verbose)
        return ModelTrainer.evaluate_model(predictions, data, threshold=threshold)

    def show_losses(self) -> None:
        import matplotlib.pyplot as plt
        import os
        plt.plot(self.losses)
        plt.savefig("fig.png")
        os.system("open fig.png")

    @staticmethod
    def save_model(model: DNNBinaryClassifier, strategy_name: str, version_folder: Optional[str] = "latest", use_s3: Optional[bool] = False) -> None:

        meta_models_path = NNHarness._get_save_path(strategy_name, version_folder=version_folder, use_s3=use_s3)
        if use_s3:
            if not s3_file_exists(meta_models_path):
                s3_create_folder(meta_models_path)
            buffer = io.BytesIO()
            torch.save(model.state_dict(), buffer)
            s3_upload_python_object(meta_models_path + "meta_model.pt", buffer.getvalue())
        else:
            if not meta_models_path.is_dir():
                meta_models_path.mkdir(parents=True, exist_ok=True)

            meta_model_path = meta_models_path / "meta_model.pt"
            torch.save(model.state_dict(), meta_model_path)
        return

    @staticmethod
    def load_model(strategy_name: str, version_folder: Optional[str] = "latest", use_s3: Optional[bool] = False) -> DNNBinaryClassifier:
        meta_models_path = NNHarness._get_save_path(strategy_name, version_folder=version_folder, use_s3=use_s3)
        model = DNNBinaryClassifier()
        if use_s3:
            model.load_state_dict(
                torch.load(s3_download_python_object(meta_models_path + "meta_model.pt"))
            )
        else:
            model.load_state_dict(torch.load(meta_models_path / "meta_model.pt"))
        model.eval()
        return model

    @staticmethod
    def _get_save_path(strategy_name: str, version_folder: Optional[str] = "latest", use_s3: Optional[bool] = False) -> Union[pathlib.Path, str]:
        if use_s3:
            s3_version_folder = version_folder
            s3_base_path = f"s3://{ML_S3_BUCKET}/{strategy_name}/{s3_version_folder}/saved_models/"
            s3_meta_models_path = s3_base_path + "meta_models/"
            return s3_meta_models_path
        else:
            base_path = pathlib.Path(ML_LOCAL_PATH) / f"{strategy_name}/" / f"{version_folder}"
            meta_models_path = base_path / "saved_models" / "meta_models/"
            return meta_models_path





if __name__ == "__main__":

    classifier = DemarkBuySetupClassifier(use_s3=False)
    classifier.load_models("DemarkBuySetup", version_folder="numeric-only")
    #classifier.train_data = classifier.apply_transformations(classifier.train_data)
    #classifier.train_data = classifier.apply_transformations(classifier.test_data)

    harness = NNHarness(classifier.models)
    #harness.transform_ensemble_data(classifier.train_data, classifier.test_data, pre_transformation_function=classifier.apply_transformations)

    meta_model_train_data = tc.SFrame("./meta_model_train_data_numeric.sframe")
    meta_model_test_data = tc.SFrame("./meta_model_test_data_numeric.sframe")
    harness.meta_model_train_data = meta_model_train_data
    harness.meta_model_test_data = meta_model_test_data
    harness.train_network(harness.meta_model_train_data, use_sampler=False)
    harness.show_losses()

    breakpoint()
    train_results = NNHarness.evaluate_nn_model(harness.meta_model_train_data, harness.meta_model)
    test_results = NNHarness.evaluate_nn_model(harness.meta_model_test_data, harness.meta_model)

    """
    
    nn_harness = 
    
    #meta_model_train_data = dt.create_meta_model_sframe(classifier.train_data)
    #meta_model_test_data = dt.create_meta_model_sframe(classifier.test_data)
    breakpoint()


    train_predictions = DNNBinaryClassifier.predictions_for_nn_meta_model(meta_model_train_data, model, verbose=False)
    breakpoint()
    train_results = ModelTrainer.evaluate_model(train_predictions, meta_model_train_data)


    test_predictions = DNNBinaryClassifier.predictions_for_nn_meta_model(meta_model_test_data, model, verbose=False)
    test_results = ModelTrainer.evaluate_model(test_predictions, meta_model_test_data)
    """
    breakpoint()



