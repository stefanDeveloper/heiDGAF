import numpy
import polars as pl
import torch


class LogisticRegression(torch.nn.Module):
    """Logistic Regression in PyTorch."""

    def __init__(
        self,
        input_dim: int = 9,
        output_dim: int = 1,
        epochs: int = 5000,
        loss_func=torch.nn.BCELoss(),
    ):
        """Init.
        Args:
            input_dim (int): input dimension
            output_dim (int): output dimension
            epochs (int, optional): Number of training epochs. 
                                        Defaults to 5000.
            loss_func: Loss function.
        """
        super(LogisticRegression, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.loss_func = loss_func
        self.epochs = epochs
        self.linear = torch.nn.Linear(self.input_dim, self.output_dim)
        self.optimizer = torch.optim.Adam(self.parameters(), lr=0.01)

    def forward(self, x):
        """Forward pass."""
        y_pred = torch.sigmoid(self.linear(x))
        return y_pred

    def fit(self, x: pl.DataFrame, y: pl.Series):
        """Fit.
            
        Args:
            x (pl.DataFrame): training dataframe
            y (pl.Series): target series
            
        Returns:
            None
        """
        y = y.to_numpy().squeeze()

        x = torch.from_numpy(x.astype(numpy.float32))
        y = torch.from_numpy(y.astype(numpy.float32))[:, None]

        iter = 0
        epochs = self.epochs
        for epoch in range(0, epochs):
            pred_y = self.forward(x)

            # Compute and print loss
            loss = self.loss_func(pred_y, y)

            # Zero gradients, perform a backward pass,
            # and update the weights.
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            iter += 1
            if iter % 500 == 0:
                print(f"epoch {epoch}, loss {loss.item()}")
        return None

    def predict_proba(self, x: numpy.ndarray):
        """Return probability of class.
            
        Args:
            x (numpy.ndarray): dataframe to infer
            
        Returns:
            numpy.ndarray: probability of survival
            """
        x = torch.from_numpy(x.astype(numpy.float32))

        y_proba = self.forward(x)
        return y_proba.flatten().detach().numpy()

    def predict(self, x: numpy.ndarray, threshold: float = 0.5):
        """Predict survival score.
        Args:
            x (numpy.ndarray): dataframe to infer

        Returns:
            numpy.ndarray: score prediction
        """
        y_pred = self.predict_proba(x)
        y_pred[y_pred > threshold] = 1
        y_pred[y_pred <= threshold] = 0
        return y_pred