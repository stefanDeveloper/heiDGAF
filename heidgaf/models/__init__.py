from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.ensemble import RandomForestClassifier

from heidgaf.feature import Preprocessor
from heidgaf.models.lr import LogisticRegression


class Pipeline:
    """Pipeline for training models.
    """
    def __init__(
        self,
        preprocessor: Preprocessor,
        mean_imputer: Imputer,
        target_encoder: TargetEncoder,
        clf: LogisticRegression,
    ):
        """Initializes preprocessors, encoder, and model.

        Args:
            preprocessor (Preprocessor): Preprocessor to transform input data into features.
            mean_imputer (Imputer): Mean imputer to handle null values.
            target_encoder (TargetEncoder): Target encoder for non-numeric values.
            clf (torch.nn.Modul): torch.nn.Modul for training.
        """
        self.preprocessor = preprocessor
        self.mean_imputer = mean_imputer
        self.target_encoder = target_encoder
        self.clf = clf

    def fit(self, x_train, y_train):
        """Fits models to training data.

        Args:
            x_train (np.array): X data.
            y_train (np.array): Y labels.
        """
        x_train = self.preprocessor.transform(x=x_train)
        x_train = self.target_encoder.fit_transform(x=x_train, y=y_train)
        x_train = self.mean_imputer.fit_transform(x=x_train)
        self.clf.fit(X=x_train.to_numpy(), y=y_train.to_numpy().reshape(-1, 1))

    def predict(self, x):
        """Predicts given X.

        Args:
            x (np.array): X data

        Returns:
            np.array: Model output.
        """
        x = self.preprocessor.transform(x=x)
        x = self.target_encoder.transform(x=x)
        x = self.mean_imputer.transform(x=x)
        return self.clf.predict(X=x.to_numpy())

params = {
    'num_rounds': 100,
    'max_depth': 8,
    'max_leaves': 2**8,
    'alpha': 0.9,
    'eta': 0.1,
    'gamma': 0.1,
    'subsample': 1,
    'reg_lambda': 1,
    'scale_pos_weight': 2,
    'objective': 'binary:logistic',
    'verbose': True,
    'gpu_id': 0,
    'tree_method': 'hist',
    # 'device': 'cuda'
}
        
random_forest_model = RandomForestClassifier()
