import polars as pl
import xgboost as xgb

from heidgaf.models import Model


class XGBoost(Model):
    def __init__(self, pre_trained_model: str, data: pl.DataFrame, train=True) -> None:
        super().__init__()
        self.model = xgb.XGBClassifier(tree_method="hist", early_stopping_rounds=2)

        if train:
            self.__train()

    def __train(self, df: pl.DataFrame) -> None:
        self.model.fit()
        self.model.save_model("clf.json")

    def __evaluate(self) -> None:
        pass
