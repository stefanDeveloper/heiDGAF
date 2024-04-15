from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.ensemble import RandomForestClassifier

from heidgaf.models.lr import LogisticRegression
from heidgaf.feature import Preprocessor


class Pipeline:
    def __init__(
        self,
        preprocessor: Preprocessor,
        mean_imputer: Imputer,
        target_encoder: TargetEncoder,
        clf: LogisticRegression,
    ):
        self.preprocessor = preprocessor
        self.mean_imputer = mean_imputer
        self.target_encoder = target_encoder
        self.clf = clf

    def fit(self, x_train, y_train):
        x_train = self.preprocessor.transform(x=x_train)
        x_train = self.target_encoder.fit_transform(x=x_train, y=y_train)
        x_train = self.mean_imputer.fit_transform(x=x_train)
        self.clf.fit(X=x_train.to_numpy(), y=y_train.to_numpy().reshape(-1, 1))

    def predict(self, x):
        x = self.preprocessor.transform(x=x)
        x = self.target_encoder.transform(x=x)
        x = self.mean_imputer.transform(x=x)
        return self.clf.predict(X=x.to_numpy())


random_forest_model = RandomForestClassifier()
