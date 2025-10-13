from abc import ABCMeta, abstractmethod
import re
import sys
import os
import joblib
from sklearn.exceptions import NotFittedError
from sklearn.utils.validation import check_is_fitted
import sklearn.model_selection
from sklearn.metrics import confusion_matrix, make_scorer
import xgboost as xgb
import optuna
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
from src.train.explainer import Plotter, Explainer
from src.train import RESULT_FOLDER, SEED

logger = get_logger("train.model")

N_FOLDS = 5
CV_RESULT_DIR = f"./{RESULT_FOLDER}"


class Pipeline:
    """Manages end-to-end machine learning pipeline for DGA detection model training.

    Orchestrates data preprocessing, feature engineering, model training, and evaluation
    for domain generation algorithm detection. Supports multiple datasets, model types,
    and handles data scaling, splitting, and persistence operations.
    """

    def __init__(
        self,
        model: str,
        datasets: list[Dataset],
        model_output_path: str,
        scaler=None,
    ) -> None:
        """Initializes complete ML pipeline with datasets and model configuration.

        Sets up feature processing, data loading, train/validation/test splitting,
        and model instantiation based on specified algorithm type. Handles data
        persistence and visualization setup.

        Args:
            model (str): Model type identifier ('rf', 'xg', 'gbm').
            datasets (list[Dataset]): List of datasets for training and evaluation.
            model_output_path (str): Directory path for saving trained models.
            scaler: Optional data scaler for feature normalization.

        Raises:
            NotImplementedError: If specified model type is not supported.
        """
        self.plotting = False
        self.processor = Processor(
            features_to_drop=[
                "query",
                "labels",
                "thirdleveldomain",
                "secondleveldomain",
                "fqdn",
                "tld",
            ]
        )
        self.datasets = datasets
        self.plotter = Plotter()
        self.explainer = Explainer()
        self.scaler = scaler
        self.model_output_path = model_output_path

        self.ds_X = []
        self.ds_y = []
        logger.info("Start data set transformation.")
        for ds in self.datasets:
            try:
                X, y = self._load_npy(ds.name)
            except FileNotFoundError:
                data = self.processor.transform(x=ds.data)
                X = data.drop("class").to_numpy()
                encoded, _, _ = self._label_encoder(data["class"].to_list())
                y = np.asarray(encoded).reshape(-1)

                self._save_npy(X, y, ds.name)
            self.ds_X.append(X)
            self.ds_y.append(y)
        logger.info(f"End data set transformation.")

        X = self.ds_X[0]
        try:
            columns = self._load_column_list()
        except FileNotFoundError:
            columns = data.columns
            self._save_column_list(columns)

        if scaler:
            try:
                check_is_fitted(scaler)
                scaler.transform(X)
            except NotFittedError:
                scaler.fit(X)

        for X1 in self.ds_X:
            X1 = scaler.transform(X1)

        if self.plotting:
            logger.info("Start plotting.")
            self.plotter.create_plots_multiclass(
                ds_X=self.ds_X, ds_y=self.ds_y, data=self.datasets
            )
            logger.info(f"End plotting.")

        for y1 in self.ds_y:
            y1[y1 != 0] = 1
        y = self.ds_y[0]

        if self.plotting:
            logger.info("Start plotting.")
            self.plotter.create_plots_binary(
                ds_X=self.ds_X, ds_y=self.ds_y, data=self.datasets
            )
            logger.info(f"End plotting.")

        # Clean column names
        self.feature_columns = [self._clean_column_name(col) for col in columns]
        logger.info(f"Columns: {self.feature_columns}.")

        # lower data
        logger.info(X.shape)
        X, _, y, _ = train_test_split(
            X, y, train_size=0.25, stratify=y, random_state=SEED
        )
        logger.info(X.shape)

        self.x_train, self.x_val, self.x_test, self.y_train, self.y_val, self.y_test = (
            self.train_test_val_split(X=X, Y=y)
        )
        logger.info(f"Final data size for training {self.x_train.shape}")

        match model:
            case "rf":
                self.model = RandomForestModel()
            case "xg":
                self.model = XGBoostModel()
            case "gbm":
                self.model = LightGBMModel()
            case _:
                raise NotImplementedError(f"Model not implemented!")

    def _load_npy(
        self,
        ds_name: str,
        output_path: str = f"./{RESULT_FOLDER}/data",
    ) -> tuple[np.ndarray, np.ndarray]:
        """Loads preprocessed feature matrices and labels from cached NumPy files.

        Args:
            ds_name (str): Name of the dataset to load.
            output_path (str): Directory containing cached dataset files.

        Returns:
            tuple[np.ndarray, np.ndarray]: Feature matrix and label array.

        Raises:
            FileNotFoundError: If cached files don't exist for the dataset.
        """
        if os.path.exists(
            os.path.join(output_path, ds_name, "X.npy")
        ) and os.path.exists(os.path.join(output_path, ds_name, "y.npy")):
            X = np.load(os.path.join(output_path, ds_name, "X.npy"))
            y = np.load(os.path.join(output_path, ds_name, "y.npy"))
            return X, y
        else:
            logger.warning(f"Data for {ds_name} not loaded yet.")
            raise FileNotFoundError("Data does not exist")

    def _save_npy(
        self,
        X: np.ndarray,
        y: np.ndarray,
        ds_name: str,
        output_path: str = f"./{RESULT_FOLDER}/data",
    ) -> None:
        """Caches processed feature matrices and labels as NumPy files.

        Args:
            X (np.ndarray): Processed feature matrix to cache.
            y (np.ndarray): Processed label array to cache.
            ds_name (str): Name of the dataset for file organization.
            output_path (str): Directory to store cached files.
        """
        os.makedirs(os.path.join(output_path, ds_name), exist_ok=True)
        np.save(os.path.join(output_path, ds_name, "X.npy"), X)
        np.save(os.path.join(output_path, ds_name, "y.npy"), y)

    def _label_encoder(
        self, labels: list[str], legit_label: str = "legit"
    ) -> tuple[list[int], dict, dict]:
        """Encodes string labels to numeric values for model training.

        Maps string class labels to integers with legitimate domains assigned to 0
        and malicious domain families assigned sequential positive integers.
        Creates bidirectional mappings for label conversion.

        Args:
            labels (list[str]): String class labels (e.g., ["legit", "DGA", "tuns"]).
            legit_label (str): Label identifier for legitimate domains.

        Returns:
            tuple[list[int], dict, dict]:   encoded, label_to_index, index_to_label
                                            Encoded: [0, 1, 2, 0, 3]
                                            Label - Index: {'legit': 0, 'fraud1': 1, 'fraud2': 2, 'fraud3': 3}
                                            Index - Label: {0: 'legit', 1: 'fraud1', 2: 'fraud2', 3: 'fraud3'}
        """
        # Unique labels excluding "legit"
        unique_labels = sorted(set(label for label in labels if label != legit_label))

        label_to_index = {legit_label: 0}
        label_to_index.update(
            {label: idx + 1 for idx, label in enumerate(unique_labels)}
        )

        # Inverse mapping
        index_to_label = {idx: label for label, idx in label_to_index.items()}

        # Encode
        encoded = [label_to_index[label] for label in labels]

        return encoded, label_to_index, index_to_label

    def _save_column_list(
        self, column_list: list, output_path: str = f"./{RESULT_FOLDER}/data"
    ) -> None:
        try:
            joblib.dump(column_list, os.path.join(output_path, "columns.pickle"))
            logger.info("Column list saved.")
        except Exception as e:
            logger.info(f"Failed to save column list: {e}")

    def _load_column_list(self, output_path: str = f"./{RESULT_FOLDER}/data"):
        try:
            column_list = joblib.load(os.path.join(output_path, "columns.pickle"))
            logger.info("Column list loaded.")
            logger.warning(column_list)
            return column_list
        except Exception as e:
            logger.info(f"Failed to load column list: {e}")
            raise FileNotFoundError("Columns does not exist")

    def _clean_column_name(self, column: str) -> str:
        """Sanitizes column names for machine learning model compatibility.

        Replaces spaces and special characters with underscores, removes invalid
        characters, and ensures column names don't start with digits.

        Args:
            column (str): Original column name to clean.

        Returns:
            str: ML-compatible column name.
        """
        # Replace spaces and hyphens with underscores
        cleaned = re.sub(r"[\s\-]+", "_", column)
        # Remove any remaining non-alphanumeric characters
        cleaned = re.sub(r"[^A-Za-z0-9_]", "", cleaned)
        # Ensure the column name doesn't start with a number
        if cleaned[0].isdigit():
            cleaned = "f_" + cleaned
        return cleaned

    def train_test_val_split(
        self,
        X: np.ndarray,
        Y: np.ndarray,
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
        """Splits dataset into training, validation, and test sets with stratification.

        Creates stratified splits maintaining class distribution across all subsets.
        Training set gets specified fraction, validation and test sets split remaining data equally.

        Args:
            X (np.ndarray): Feature matrix to split.
            Y (np.ndarray): Label array to split.
            train_frac (float): Proportion of data for training set. Default: 0.8
            random_state (int): Random seed for reproducible splits.

        Returns:
            tuple: X_train, X_val, X_test, Y_train, Y_val, Y_test arrays.
        """

        logger.info("Create train, validation, and test split.")

        X_train, X_tmp, Y_train, Y_tmp = sklearn.model_selection.train_test_split(
            X, Y, train_size=train_frac, random_state=random_state, stratify=Y
        )

        X_val, X_test, Y_val, Y_test = sklearn.model_selection.train_test_split(
            X_tmp, Y_tmp, train_size=0.5, random_state=random_state, stratify=Y_tmp
        )

        return X_train, X_val, X_test, Y_train, Y_val, Y_test

    def hyperparam_fit(self) -> None:
        """Performs hyperparameter optimization and model training.

        Uses Optuna to search optimal hyperparameters through Bayesian optimization,
        then trains the model with best parameters found during the search process.
        """
        if not os.path.exists(CV_RESULT_DIR):
            os.mkdir(CV_RESULT_DIR)

        self.model.X = self.x_train
        self.model.y = self.y_train
        study = optuna.create_study(direction="maximize")
        study.optimize(self.model.objective, n_trials=50, timeout=600)

        logger.info(f"Number of finished trials: {len(study.trials)}")
        logger.info("Best trial:")
        self.trial = study.best_trial

        logger.info(f"  Value: {self.trial.value}")
        logger.info(f"  Params: ")
        for key, value in self.trial.params.items():
            logger.info(f"    {key}: {value}")

        self.model.train(
            trial=self.trial,
            X=self.x_train,
            y=self.y_train,
        )

    def predict(self, x: np.ndarray) -> np.ndarray:
        """Generates predictions for input feature matrix.

        Args:
            x (np.ndarray): Feature matrix for prediction.

        Returns:
            np.ndarray: Model predictions.
        """
        return self.model.predict(x)

    def explain(self, x: np.ndarray, y: np.ndarray) -> list[str]:
        """Generates interpretable explanations for trained model decisions.

        Creates human-readable decision rules and feature importance explanations
        for supported model types (XGBoost, Random Forest).

        Args:
            x (np.ndarray): Feature matrix for explanation generation.
            y (np.ndarray): True labels for explanation context.

        Returns:
            list[str]: List of interpretable decision rules and explanations.
        """
        if isinstance(self.model.clf, xgb.XGBClassifier) or isinstance(
            self.model.clf, RandomForestClassifier
        ):
            return self.explainer.interpret_model(
                self.model.clf,
                x,
                y,
                self.feature_columns,
                self.scaler,
            )
        else:
            logger.warning(
                f"Model of instance {type(self.model.clf)} is not supported!"
            )


class Model(metaclass=ABCMeta):
    def __init__(self) -> None:
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

        self.X = None
        self.y = None
        self.clf = None

    def fdr_metric(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Custom FDR metric to evaluate the performance of the Random Forest model.

        Args:
            y_true (np.ndarray): The true labels.
            y_pred (np.ndarray): The predicted labels.

        Returns:
            float: The False Discovery Rate (FDR).
        """
        _, FP, _, TP = confusion_matrix(y_true, y_pred).ravel()

        # Compute FDR, avoiding division by zero
        if FP + TP == 0:
            fdr = 0.0
        else:
            fdr = FP / (FP + TP)
        return fdr - 1  # Important: negative because lower FDR is better

    @abstractmethod
    def objective(self, trial):
        pass

    def predict(self, X: np.ndarray):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        return self.clf.predict(X)

    @abstractmethod
    def train(self, trial, X: np.ndarray, y: np.ndarray):
        pass


class XGBoostModel(Model):
    def __init__(self) -> None:
        super().__init__()
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

        cm = confusion_matrix(labels, preds_binary)
        TP = cm[1, 1]
        FP = cm[0, 1]

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
        neg = np.sum(self.y == 0)
        pos = np.sum(self.y == 1)
        scale_pos_weight = neg / pos

        dtrain = xgb.DMatrix(self.X, label=self.y)

        param = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
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

        return xgb_cv_results["test-fdr-mean"].min()

    def train(self, trial, X: np.ndarray, y: np.ndarray):
        """
        Trains the XGBoost model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
        """
        logger.info("Number of estimators: {}".format(trial.user_attrs["n_estimators"]))
        neg = np.sum(y == 0)
        pos = np.sum(y == 1)
        scale_pos_weight = neg / pos

        params = {
            "verbosity": 0,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "device": self.device,
            "scale_pos_weight": scale_pos_weight,
        }

        self.clf = xgb.XGBClassifier(
            n_estimators=trial.user_attrs["n_estimators"], **trial.params, **params
        )
        self.clf.fit(X, y)


class RandomForestModel(Model):
    def __init__(self) -> None:
        super().__init__()
        self.model_name = "rf"
        # Create a scorer using make_scorer, setting greater_is_better to False since lower FDR is better
        self.fdr_scorer = make_scorer(self.fdr_metric, greater_is_better=False)

    def objective(self, trial):
        """
        Optimizes the Random Forest model hyperparameters using cross-validation.

        Args:
            trial: A trial object from the optimization framework (e.g., Optuna).

        Returns:
            float: The best FDR value after cross-validation.
        """
        neg = np.sum(self.y == 0)
        pos = np.sum(self.y == 1)
        total = pos + neg

        class_weights = {0: total / (2 * neg), 1: total / (2 * pos)}

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
            class_weight=class_weights,
        )

        score = cross_val_score(
            classifier_obj,
            self.X,
            self.y,
            n_jobs=-1,
            cv=N_FOLDS,
            scoring=self.fdr_scorer,
        )
        fdr = score.mean()
        return fdr

    def train(self, trial, X: np.ndarray, y: np.ndarray):
        """
        Trains the Random Forest model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        classes_weights = class_weight.compute_sample_weight(
            class_weight="balanced", y=y
        )
        self.clf = RandomForestClassifier(**trial.params)
        self.clf.fit(X, y, sample_weight=classes_weights)


class LightGBMModel(Model):
    def __init__(self) -> None:
        super().__init__()
        self.model_name = "gbm"
        # Create a scorer using make_scorer, setting greater_is_better to False since lower FDR is better
        self.fdr_scorer = make_scorer(self.fdr_metric, greater_is_better=False)

    def objective(self, trial):
        """
        Optimizes the Random Forest model hyperparameters using cross-validation.

        Args:
            trial: A trial object from the optimization framework (e.g., Optuna).

        Returns:
            float: The best FDR value after cross-validation.
        """
        neg = np.sum(self.y == 0)
        pos = np.sum(self.y == 1)
        scale_pos_weight = neg / pos

        # Define hyperparameters to optimize
        param = {
            "objective": "binary",
            "verbosity": -1,
            "metric": "logloss",
            "boosting_type": "gbdt",
            "device": self.device,
            "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.2, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 16, 256),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 100),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            "scale_pos_weight": scale_pos_weight,
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
            "max_bin": trial.suggest_categorical("max_bin", [63, 127, 255]),
        }

        # Create model with suggested hyperparameters
        classifier_obj = lgb.LGBMClassifier(**param)

        score = cross_val_score(
            classifier_obj,
            self.X,
            self.y,
            n_jobs=-1,
            cv=N_FOLDS,
            scoring=self.fdr_scorer,
        )
        fdr = score.mean()
        return fdr

    def train(self, trial, X: np.ndarray, y: np.ndarray):
        """
        Trains the Random Forest model and saves the trained model to a file.

        Args:
            trial: A trial object from the optimization framework.
            output_path (str): The directory path to save the trained model.
        """
        classes_weights = class_weight.compute_sample_weight(
            class_weight="balanced", y=y
        )

        self.clf = lgb.LGBMClassifier(**trial.params)

        self.clf.fit(X, y, sample_weight=classes_weights)
