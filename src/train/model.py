from abc import ABCMeta, abstractmethod
import pickle
import sys
import os

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
N_FOLDS = 3
CV_RESULT_DIR = "./results"


class Pipeline:
    """Pipeline for training models."""

    def __init__(self, processor: Processor, model: str, dataset: Dataset):
        """Initializes preprocessors, encoder, and model.

        Args:
            processor (processor): Processor to transform input data into features.
            mean_imputer (Imputer): Mean imputer to handle null values.
            target_encoder (TargetEncoder): Target encoder for non-numeric values.
            clf (torch.nn.Modul): torch.nn.Modul for training.
        """
        self.processor = processor
        self.dataset = dataset
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

        # setting device on GPU if available, else CPU
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")

        if torch.cuda.is_available():
            logger.debug("GPU detected")
            logger.debug(f"\t{torch.cuda.get_device_name(0)}")

        if self.device.type == "cuda":
            logger.debug("Memory Usage:")
            logger.debug(
                f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
            )
            logger.debug(
                f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
            )
            self.device = "gpu"
        else:
            self.device = "cpu"

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

        self.model.train(trial)

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

    @abstractmethod
    def objective(self, trial):
        pass

    @abstractmethod
    def predict(self, x):
        pass

    @abstractmethod
    def train(self, trial):
        pass


class XGBoostModel(Model):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        super().__init__(processor, x_train, y_train)

    def objective(self, trial):
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
        )

        # Set n_estimators as a trial attribute; Accessible via study.trials_dataframe().
        trial.set_user_attr("n_estimators", len(xgb_cv_results))

        # Save cross-validation results.
        filepath = os.path.join(CV_RESULT_DIR, "{}.csv".format(trial.number))
        xgb_cv_results.to_csv(filepath, index=False)

        # Extract the best score.
        best_score = xgb_cv_results["test-auc-mean"].values[-1]
        return best_score

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.processor.transform(x=x)
        dtest = xgb.DMatrix(x.to_numpy())
        return self.clf.predict(dtest)

    def train(self, trial):
        logger.info("Number of estimators: {}".format(trial.user_attrs["n_estimators"]))

        dtrain = xgb.DMatrix(self.x_train, label=self.y_train)

        params = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "auc",
        }

        self.clf = xgb.train(
            {**trial.params, **params}, dtrain, trial.user_attrs["n_estimators"]
        )

        logger.info("Save trained model to a file.")
        with open(f"{trial.number}.pickle", "wb") as fout:
            pickle.dump(self.clf, fout)


class RandomForestModel(Model):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        super().__init__(processor, x_train, y_train)

    def objective(self, trial):
        max_depth = trial.suggest_int("max_depth", 2, 32, log=True)
        n_estimators = trial.suggest_int("n_estimators", 2, 32, log=True)
        classifier_obj = RandomForestClassifier(
            max_depth=max_depth, n_estimators=n_estimators
        )

        score = cross_val_score(
            classifier_obj,
            self.x_train,
            self.y_train,
            n_jobs=-1,
            cv=3,
            scoring="roc_auc",
        )
        accuracy = score.mean()
        return accuracy

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.processor.transform(x=x)
        return self.clf.predict(x)

    def train(self, trial):
        self.clf = RandomForestClassifier(**trial.params)
        self.clf.fit(self.x_train, self.y_train)

        logger.info("Save trained model to a file.")
        with open(f"{trial.number}.pickle", "wb") as fout:
            pickle.dump(self.clf, fout)
