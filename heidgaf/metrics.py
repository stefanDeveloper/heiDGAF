import logging

from sklearn.metrics import classification_report

from heidgaf.models import Pipeline


class Evaluation():
    def __init__(self, x_val, y_val) -> None:
        self.x_val = x_val
        self.y_val = y_val

    def evaluate(self, model_pipeline: Pipeline):
        y_pred = model_pipeline.predict(x=self.x_val)
        logging.info(classification_report(y_pred=y_pred, y_true=self.y_val.to_numpy().squeeze()))



