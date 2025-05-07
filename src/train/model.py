from abc import ABCMeta, abstractmethod
import hashlib
import pickle
import re
import sys
import os
import tempfile
import sklearn.model_selection
from sklearn.metrics import make_scorer
import xgboost as xgb
import optuna
from imblearn.under_sampling import ClusterCentroids
import torch
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.utils import class_weight
import lightgbm as lgb

sys.path.append(os.getcwd())
from src.train.feature import Processor
from src.base.log_config import get_logger
from src.train.dataset import Dataset
from src.train.explainer import PC, Explainer
from src.train import RESULT_FOLDER

logger = get_logger("train.model")

SEED = 108
N_FOLDS = 5
CV_RESULT_DIR = f"./{RESULT_FOLDER}"


class Pipeline:
    """Pipeline for training models."""

    def __init__(
        self,
        processor: Processor,
        model: str,
        datasets: list[Dataset],
        model_output_path: str,
        scaler,
    ) -> None:
        """Initializes preprocessors, encoder, and model.

        Args:
            processor (processor): Processor to transform input data into features.
            mean_imputer (Imputer): Mean imputer to handle null values.
            target_encoder (TargetEncoder): Target encoder for non-numeric values.
            clf (torch.nn.Modul): torch.nn.Modul for training.
        """
        self.processor = processor
        self.datasets = datasets
        self.pc = PC()
        self.explainer = Explainer()
        self.scaler = scaler
        self.model_output_path = model_output_path

        self.ds_X = []
        self.ds_y = []
        logger.info("Start data set transformation.")
        for ds in self.datasets:
            data = self.processor.transform(x=ds.data)
            X = data.drop("class").to_numpy()
            y = data.select("class").to_numpy().reshape(-1)
            X = scaler.fit_transform(X)
            self.ds_X.append(X)
            self.ds_y.append(y)
        logger.info(f"End data set transformation with shape {data.shape}.")

        X = self.ds_X[0]
        y = self.ds_y[0]

        # Clean column names
        self.columns = [self._clean_column_name(col) for col in data.columns]

        # Create and save feature mapping
        self.feature_mapping = self._create_feature_mapping(self.columns)

        # Store column names
        self.feature_columns = data.columns
        self.feature_columns.remove("class")
        logger.info(f"Columns: {self.feature_columns}.")

        self.pc.create_plots(X=X, y=y)
        df_data = data.to_pandas()
        # Assuming your data is in a DataFrame called 'df' with a 'condition' column
        condition1_data = df_data[df_data["class"] == 1]
        condition2_data = df_data[df_data["class"] == 0]

        # List of measurements (you can use all or a subset)
        measurements = df_data.columns.tolist()[
            1:
        ]  # [1:] to drop the condition column in the beginning
        self.pc.analyse_data(
            data_condition1=condition1_data,
            data_condition2=condition2_data,
            measurements=measurements,
            condition1_name="Benign",
            condition2_name="Malicious",
        )

        # lower data
        logger.info(X.shape)
        X, _, y, _ = train_test_split(
            X, y, train_size=0.1, stratify=y, random_state=SEED
        )
        logger.info(X.shape)

        cc = ClusterCentroids(random_state=SEED)
        X, y = cc.fit_resample(X, y)

        self.x_train, self.x_val, self.x_test, self.y_train, self.y_val, self.y_test = (
            self.train_test_val_split(X=X, Y=y)
        )

        logger.info(self.x_train.shape)

        match model:
            case "rf":
                self.model = RandomForestModel(
                    processor=self.processor, x_train=self.x_train, y_train=self.y_train
                )
            case "xg":
                self.model = XGBoostModel(
                    processor=self.processor, x_train=self.x_train, y_train=self.y_train
                )
            case "gbm":
                self.model = LightGBMModel(
                    processor=self.processor, x_train=self.x_train, y_train=self.y_train
                )
            case _:
                raise NotImplementedError(f"Model not implemented!")

    def _clean_column_name(self, column: str) -> str:
        """
        Clean column names to be compatible with ML models.

        Args:
            column (str): Original column name

        Returns:
            str: Cleaned column name
        """
        # Replace spaces and hyphens with underscores
        cleaned = re.sub(r"[\s\-]+", "_", column)
        # Remove any remaining non-alphanumeric characters
        cleaned = re.sub(r"[^A-Za-z0-9_]", "", cleaned)
        # Ensure the column name doesn't start with a number
        if cleaned[0].isdigit():
            cleaned = "f_" + cleaned
        return cleaned

    def _create_feature_mapping(self, original_columns: list[str]) -> None:
        """
        Create and save mapping between original and cleaned column names.

        Args:
            original_columns (list): Original column names

        Returns:
            dict: Mapping of cleaned names to original names
        """
        mapping = {self._clean_column_name(col): col for col in original_columns}

        # Save mapping for future reference
        os.makedirs(os.path.join(CV_RESULT_DIR, "metadata"), exist_ok=True)
        with open(
            os.path.join(CV_RESULT_DIR, "metadata", "feature_mapping.pickle"), "wb"
        ) as f:
            pickle.dump(mapping, f)

        # Also save as readable text file
        with open(
            os.path.join(CV_RESULT_DIR, "metadata", "feature_mapping.txt"), "w"
        ) as f:
            for clean, original in mapping.items():
                f.write(f"{clean} -> {original}\n")

    def train_test_val_split(
        self,
        X: np.ndarray,
        Y=np.ndarray,
        train_frac: float = 0.8,
        random_state: int = SEED,
    ) -> tuple[
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
    ]:
        """Splits data set in train, test, and validation set

        Args:
            train_frac (float, optional): Training fraction. Defaults to 0.8.
            random_state (int, optional): Random state. Defaults to None.

        Returns:
            tuple[list, list, list, list, list, list]: X_train, X_val, X_test, Y_train, Y_val, Y_test
        """

        logger.info("Create train, validation, and test split.")

        X_train, X_tmp, Y_train, Y_tmp = sklearn.model_selection.train_test_split(
            X, Y, train_size=train_frac, random_state=random_state, stratify=Y
        )

        X_val, X_test, Y_val, Y_test = sklearn.model_selection.train_test_split(
            X_tmp, Y_tmp, train_size=0.5, random_state=random_state, stratify=Y_tmp
        )

        return X_train, X_val, X_test, Y_train, Y_val, Y_test

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

    def explain(self, x, y):
        """Explains models

        Args:
            x (np.array): X data
            y (np.array): Y data
        """
        self.explainer.interpret_model(
            self.model.clf,
            x,
            y,
            self.feature_columns,
            self.model.model_name,
            self.scaler,
        )


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
        self.model_name = "xgboost"

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
        neg = np.sum(self.y_train == 0)
        pos = np.sum(self.y_train == 1)
        scale_pos_weight = neg / pos

        dtrain = xgb.DMatrix(self.x_train, label=self.y_train)

        param = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "device": self.device,
            "scale_pos_weight": scale_pos_weight,
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
        return self.clf.predict(x)

    def train(self, trial, output_path):
        """
        Trains the XGBoost model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        logger.info("Number of estimators: {}".format(trial.user_attrs["n_estimators"]))
        neg = np.sum(self.y_train == 0)
        pos = np.sum(self.y_train == 1)
        scale_pos_weight = neg / pos

        params = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "device": self.device,
            "scale_pos_weight": scale_pos_weight,
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
        self.model_name = "rf"

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
            # scoring=fdr_scorer,
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
        return self.clf.predict(x)

    def train(self, trial, output_path):
        """
        Trains the Random Forest model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        classes_weights = class_weight.compute_sample_weight(
            class_weight="balanced", y=self.y_train
        )
        self.clf = RandomForestClassifier(**trial.params)
        self.clf.fit(self.x_train, self.y_train, sample_weight=classes_weights)

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


class LightGBMModel(Model):
    def __init__(
        self, processor: Processor, x_train: np.ndarray, y_train: np.ndarray
    ) -> None:
        super().__init__(processor, x_train, y_train)
        self.model_name = "gbm"

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
        num_leaves = trial.suggest_int("num_leaves", 20, 200)
        max_depth = trial.suggest_int("max_depth", 3, 12)
        learning_rate = trial.suggest_float("learning_rate", 0.5, 5)
        n_estimators = trial.suggest_float("n_estimators", 50, 300)
        subsample = trial.suggest_float("subsample", 0.1, 1.0)
        colsample_bytree = trial.suggest_float("colsample_bytree", 0.1, 1.0)

        # Create model with suggested hyperparameters
        classifier_obj = lgb.LGBMClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            num_leaves=num_leaves,
        )

        # Create a scorer using make_scorer, setting greater_is_better to False since lower FDR is better
        fdr_scorer = make_scorer(self.fdr_metric, greater_is_better=False)

        score = cross_val_score(
            classifier_obj,
            self.x_train,
            self.y_train,
            n_jobs=-1,
            cv=N_FOLDS,
            # scoring=fdr_scorer,
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
        return self.clf.predict(x)

    def train(self, trial, output_path):
        """
        Trains the Random Forest model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        classes_weights = class_weight.compute_sample_weight(
            class_weight="balanced", y=self.y_train
        )
        self.clf = lgb.LGBMClassifier(**trial.params)

        self.clf.fit(self.x_train, self.y_train, sample_weight=classes_weights)

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
