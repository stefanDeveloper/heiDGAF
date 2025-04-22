from abc import ABCMeta, abstractmethod
import hashlib
import pickle
import sys
import os
import tempfile

from sklearn.metrics import make_scorer
import xgboost as xgb
import optuna
import torch
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score


sys.path.append(os.getcwd())
from src.train.feature import Processor
from src.base.log_config import get_logger
from src.train.dataset import Dataset

logger = get_logger("train.model")

SEED = 108
N_FOLDS = 5
CV_RESULT_DIR = "./results"


class Pipeline:
    """Pipeline for training models."""

    def __init__(
        self, processor: Processor, model: str, dataset: Dataset, model_output_path: str
    ):
        """Initializes preprocessors, encoder, and model.

        Args:
            processor (processor): Processor to transform input data into features.
            mean_imputer (Imputer): Mean imputer to handle null values.
            target_encoder (TargetEncoder): Target encoder for non-numeric values.
            clf (torch.nn.Modul): torch.nn.Modul for training.
        """
        self.processor = processor
        self.dataset = dataset
        self.model_output_path = model_output_path
        logger.info("Start data set transformation.")
        x_train = self.processor.transform(x=self.dataset.X_train)
        logger.info(f"End data set transformation with shape {x_train.shape}.")

        self.x_train = x_train.to_numpy()
        self.y_train = self.dataset.Y_train.to_numpy().ravel()
        match model:
            case "rf":
                self.model = RandomForestModel(
                    processor=self.processor, x_train=self.x_train, y_train=self.y_train
                )
            case "xg":
                self.model = XGBoostModel(
                    processor=self.processor, x_train=self.x_train, y_train=self.y_train
                )
            case _:
                raise NotImplementedError(f"Model not implemented!")

    def fit(self):
        """Fits models to training data.

        Args:
            x_train (np.array): X data.
            y_train (np.array): Y labels.
        """
        if not os.path.exists(CV_RESULT_DIR):
            os.mkdir(CV_RESULT_DIR)

        study = optuna.create_study(direction="maximize")
        study.optimize(self.model.objective, n_trials=20, timeout=600)

        logger.info(f"Number of finished trials: {len(study.trials)}")
        logger.info("Best trial:")
        trial = study.best_trial

        logger.info(f"  Value: {trial.value}")
        logger.info(f"  Params: ")
        for key, value in trial.params.items():
            logger.info(f"    {key}: {value}")

        self.model.train(trial, self.model_output_path)

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        return self.model.predict(x)


class Model(metaclass=ABCMeta):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        self.processor = processor
        self.x_train = x_train
        self.y_train = y_train
        # setting device on GPU if available, else CPU
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")

        if torch.cuda.is_available():
            logger.info("GPU detected")
            logger.info(f"\t{torch.cuda.get_device_name(0)}")

        if self.device.type == "cuda":
            logger.info("Memory Usage:")
            logger.info(
                f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
            )
            logger.info(
                f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
            )
            self.device = "gpu"
        else:
            self.device = "cpu"

    def sha256sum(self, file_path: str) -> str:
        """Return a SHA265 sum check to validate the model.

        Args:
            file_path (str): File path of model.

        Returns:
            str: SHA256 sum
        """
        h = hashlib.sha256()

        with open(file_path, "rb") as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest()

    @abstractmethod
    def objective(self, trial):
        pass

    @abstractmethod
    def predict(self, x):
        pass

    @abstractmethod
    def train(self, trial, output_path):
        pass


class XGBoostModel(Model):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        super().__init__(processor, x_train, y_train)

    def fdr_metric(self, preds: np.ndarray, dtrain: xgb.DMatrix) -> tuple[str, float]:
        """
        Custom FDR metric to evaluate model performance based on False Discovery Rate.

        Args:
            preds (np.ndarray): The predicted values.
            dtrain (xgb.DMatrix): The training data matrix.

        Returns:
            tuple: A tuple containing the metric name ("fdr") and its value.
        """
        # Get the true labels
        labels = dtrain.get_label()

        # Threshold predictions to get binary outcomes (assuming binary classification with 0.5 threshold)
        preds_binary = (preds > 0.5).astype(int)

        # Calculate False Positives (FP) and True Positives (TP)
        FP = np.sum((preds_binary == 1) & (labels == 0))
        TP = np.sum((preds_binary == 1) & (labels == 1))

        # Avoid division by zero
        if FP + TP == 0:
            fdr = 0.0
        else:
            fdr = FP / (FP + TP)

        # Return the result in the format (name, value)
        return (
            "fdr",
            1 - fdr,
        )  # -1 is essentiell since XGBoost wants a scoring value (higher is better). However, FDR represents a loss function.

    def objective(self, trial):
        """
        Optimizes the XGBoost model hyperparameters using cross-validation.

        Args:
            trial: A trial object from the optimization framework (e.g., Optuna).

        Returns:
            float: The best FDR value after cross-validation.
        """
        dtrain = xgb.DMatrix(self.x_train, label=self.y_train)

        param = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "device": self.device,
            "booster": trial.suggest_categorical(
                "booster", ["gbtree", "gblinear", "dart"]
            ),
            "lambda": trial.suggest_float("lambda", 1e-8, 1.0, log=True),
            "alpha": trial.suggest_float("alpha", 1e-8, 1.0, log=True),
            # sampling ratio for training data.
            "subsample": trial.suggest_float("subsample", 0.2, 1.0),
            # sampling according to each tree.
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.2, 1.0),
        }

        if param["booster"] == "gbtree" or param["booster"] == "dart":
            param["max_depth"] = trial.suggest_int("max_depth", 1, 9)
            # minimum child weight, larger the term more conservative the tree.
            param["min_child_weight"] = trial.suggest_int("min_child_weight", 2, 10)
            param["eta"] = trial.suggest_float("eta", 1e-8, 1.0, log=True)
            param["gamma"] = trial.suggest_float("gamma", 1e-8, 1.0, log=True)
            param["grow_policy"] = trial.suggest_categorical(
                "grow_policy", ["depthwise", "lossguide"]
            )

        if param["booster"] == "dart":
            param["sample_type"] = trial.suggest_categorical(
                "sample_type", ["uniform", "weighted"]
            )
            param["normalize_type"] = trial.suggest_categorical(
                "normalize_type", ["tree", "forest"]
            )
            param["rate_drop"] = trial.suggest_float("rate_drop", 1e-8, 1.0, log=True)
            param["skip_drop"] = trial.suggest_float("skip_drop", 1e-8, 1.0, log=True)

        xgb_cv_results = xgb.cv(
            params=param,
            dtrain=dtrain,
            num_boost_round=10000,
            nfold=N_FOLDS,
            stratified=True,
            early_stopping_rounds=100,
            seed=SEED,
            verbose_eval=False,
            custom_metric=self.fdr_metric,
        )

        # Set n_estimators as a trial attribute; Accessible via study.trials_dataframe().
        trial.set_user_attr("n_estimators", len(xgb_cv_results))

        # Save cross-validation results.
        filepath = os.path.join(CV_RESULT_DIR, "{}.csv".format(trial.number))
        xgb_cv_results.to_csv(filepath, index=False)

        # Extract the best score.
        best_fdr = xgb_cv_results["test-auc-mean"].values[-1]
        return best_fdr

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.processor.transform(x=x)
        # dtest = xgb.DMatrix(x.to_numpy())
        return self.clf.predict(x)

    def train(self, trial, output_path):
        """
        Trains the XGBoost model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        logger.info("Number of estimators: {}".format(trial.user_attrs["n_estimators"]))

        # dtrain = xgb.DMatrix(self.x_train, label=self.y_train)

        params = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "device": self.device,
        }

        self.clf = xgb.XGBClassifier(
            n_estimators=trial.user_attrs["n_estimators"], **trial.params, **params
        )
        self.clf.fit(self.x_train, self.y_train)

        logger.info("Save trained model to a file.")
        with open(
            os.path.join(tempfile.gettempdir(), f"xg_{trial.number}.pickle"), "wb"
        ) as fout:
            pickle.dump(self.clf, fout)

        sha256sum = self.sha256sum(
            os.path.join(tempfile.gettempdir(), f"xg_{trial.number}.pickle")
        )
        with open(os.path.join(output_path, f"xg_{sha256sum}.pickle"), "wb") as fout:
            pickle.dump(self.clf, fout)


class RandomForestModel(Model):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        super().__init__(processor, x_train, y_train)

    # Define the custom FDR metric
    def fdr_metric(self, y_true: np.ndarray, y_pred: np.ndarray):
        """
        Custom FDR metric to evaluate the performance of the Random Forest model.

        Args:
            y_true (np.ndarray): The true labels.
            y_pred (np.ndarray): The predicted labels.

        Returns:
            float: The False Discovery Rate (FDR).
        """
        # False Positives (FP): cases where the model predicted 1 but the actual label is 0
        FP = np.sum((y_pred == 1) & (y_true == 0))

        # True Positives (TP): cases where the model correctly predicted 1
        TP = np.sum((y_pred == 1) & (y_true == 1))

        # Compute FDR, avoiding division by zero
        if FP + TP == 0:
            fdr = 0.0
        else:
            fdr = FP / (FP + TP)

        return fdr

    def objective(self, trial):
        """
        Optimizes the Random Forest model hyperparameters using cross-validation.

        Args:
            trial: A trial object from the optimization framework (e.g., Optuna).

        Returns:
            float: The best FDR value after cross-validation.
        """
        # Define hyperparameters to optimize
        n_estimators = trial.suggest_int("n_estimators", 50, 300)
        max_depth = trial.suggest_int("max_depth", 2, 20)
        min_samples_split = trial.suggest_int("min_samples_split", 2, 20)
        min_samples_leaf = trial.suggest_int("min_samples_leaf", 1, 20)
        max_features = trial.suggest_categorical("max_features", ["sqrt", "log2", None])

        # Create model with suggested hyperparameters
        classifier_obj = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            random_state=SEED,
        )

        # Create a scorer using make_scorer, setting greater_is_better to False since lower FDR is better
        fdr_scorer = make_scorer(self.fdr_metric, greater_is_better=False)

        score = cross_val_score(
            classifier_obj,
            self.x_train,
            self.y_train,
            n_jobs=-1,
            cv=N_FOLDS,
            scoring=fdr_scorer,
        )
        fdr = score.mean()
        return fdr

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.processor.transform(x=x)
        return self.clf.predict(x)

    def train(self, trial, output_path):
        """
        Trains the Random Forest model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        self.clf = RandomForestClassifier(**trial.params)
        self.clf.fit(self.x_train, self.y_train)

        logger.info("Save trained model to a file.")
        with open(
            os.path.join(tempfile.gettempdir(), f"rf_{trial.number}.pickle"), "wb"
        ) as fout:
            pickle.dump(self.clf, fout)

        sha256sum = self.sha256sum(
            os.path.join(tempfile.gettempdir(), f"rf_{trial.number}.pickle")
        )
        with open(os.path.join(output_path, f"rf_{sha256sum}.pickle"), "wb") as fout:
            pickle.dump(self.clf, fout)
