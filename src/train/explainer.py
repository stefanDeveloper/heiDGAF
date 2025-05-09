import sys
import os
from scipy.stats import ks_2samp
from scipy.stats import wasserstein_distance
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
from te2rules.explainer import ModelExplainer
import string
import polars as pl


sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from src.train import RESULT_FOLDER, SEED
from src.train.dataset import Dataset

logger = get_logger("train.explainer")


class Plotter:
    def __init__(self, output_path: str = f"./{RESULT_FOLDER}/data"):
        """
        Initialize the Plotter class for PCA visualization.

        Args:
            output_path (str): Path to save the figures. Defaults to './results'.
        """
        self.output_path = output_path

    def _plot_pca_2d(self, X: np.ndarray, y: np.ndarray, name: str) -> None:
        """
        Perform PCA and plot the first two principal components in 2D.

        Args:
            X (np.ndarray): Feature matrix.
            y (np.ndarray): Label array.
        """
        pca = PCA(n_components=2)

        X_pca = pca.fit_transform(X)

        Xax = X_pca[:, 0]
        Yax = X_pca[:, 1]

        cdict = {0: "red", 1: "green"}
        labl = {0: "C1", 1: "C2"}
        marker = {0: "*", 1: "o"}
        alpha = {0: 0.3, 1: 0.5}

        fig = plt.figure(figsize=(7, 5))
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor("white")
        # plt.scatter(X[:,0],X[:,1],c=y,cmap='plasma')
        for l in np.unique(y):
            ix = np.where(y == l)
            ax.scatter(
                Xax[ix],
                Yax[ix],
                c=cdict[l],
                s=40,
                label=labl[l],
                marker=marker[l],
                alpha=alpha[l],
            )
        plt.xlabel("First principal component")
        plt.ylabel("Second Principal Component")
        plt.show()

        output_path = os.path.join(self.output_path, name)
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(os.path.join(output_path, f"pca_2d.pdf"))

        plt.close()

    def _plot_pca_3d(self, X: np.ndarray, y: np.ndarray, name: str) -> None:
        """
        Perform PCA and plot the first three principal components in 3D.

        Args:
            X (np.ndarray): Feature matrix.
            y (np.ndarray): Label array.
        """
        pca = PCA(n_components=3)
        pca.fit(X)
        X_pca = pca.transform(X)

        ex_variance = np.var(X_pca, axis=0)
        ex_variance_ratio = ex_variance / np.sum(ex_variance)
        ex_variance_ratio

        Xax = X_pca[:, 0]
        Yax = X_pca[:, 1]
        Zax = X_pca[:, 2]

        cdict = {0: "red", 1: "green"}
        labl = {0: "C1", 1: "C2"}
        marker = {0: "*", 1: "o"}
        alpha = {0: 0.3, 1: 0.5}

        fig = plt.figure(figsize=(7, 5))
        ax = fig.add_subplot(111, projection="3d")

        fig.patch.set_facecolor("white")
        for l in np.unique(y):
            ix = np.where(y == l)
            ax.scatter(
                Xax[ix],
                Yax[ix],
                Zax[ix],
                c=cdict[l],
                s=40,
                label=labl[l],
                marker=marker[l],
                alpha=alpha[l],
            )
        # for loop ends
        ax.set_xlabel("First Principal Component", fontsize=14)
        ax.set_ylabel("Second Principal Component", fontsize=14)
        ax.set_zlabel("Third Principal Component", fontsize=14)
        ax.legend()
        plt.show()

        output_path = os.path.join(self.output_path, name)
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(os.path.join(output_path, f"pca_3d.pdf"))
        plt.close()

    def _plot_tsne(
        self,
        X: np.ndarray,
        y: np.ndarray,
        ds_name: str,
        title: str = "t-SNE Plot",
        random_state: int = SEED,
    ):
        # Reduce dimensionality to 2D with t-SNE
        tsne = TSNE(n_components=2, random_state=random_state)
        X_tsne = tsne.fit_transform(X)

        # Plot
        plt.figure(figsize=(10, 8))
        for label, color, name in zip(
            [0, 1], ["blue", "orange"], ["Benign", "Malicious"]
        ):
            idx = y == label
            plt.scatter(X_tsne[idx, 0], X_tsne[idx, 1], c=color, label=name, alpha=0.7)
        plt.title(title)
        plt.xlabel("t-SNE Component 1")
        plt.ylabel("t-SNE Component 2")
        plt.xticks([])
        plt.yticks([])
        plt.grid(False)
        plt.legend()
        plt.tight_layout()
        plt.show()

        output_path = os.path.join(self.output_path, ds_name)
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(os.path.join(output_path, f"tsne.pdf"))
        plt.close()

    def _plot_label_distribution(self, data: pl.DataFrame, name: str) -> None:
        """Plots label distribution.

        Args:
            data (pl.DataFrame): DataFrame with all features.
        """
        label_counts = data["class"].value_counts()
        logger.info(label_counts)
        label_distribution = dict(zip(label_counts["class"], label_counts["count"]))
        logger.info(label_distribution)

        # Plot using matplotlib
        labels = list(label_distribution.keys())
        counts = list(label_distribution.values())

        plt.figure(figsize=(6, 4))
        plt.bar(labels, counts, color=["skyblue", "salmon"])
        plt.xlabel("Label", labelpad=30)
        plt.ylabel("Count")
        plt.yscale("log")
        plt.title("Label Distribution")
        plt.xticks(rotation=90, ha="center")
        plt.grid(axis="y", linestyle="--", alpha=0.6)
        plt.tight_layout()
        plt.show()

        output_path = os.path.join(self.output_path, name)
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(os.path.join(output_path, f"label_distribution.pdf"))
        plt.close()

    def _remove_feature(
        self, component: int, X: np.ndarray, y: np.ndarray, pca: PCA, name: str
    ) -> None:
        """
        Visualize data after removing the projection onto a specific principal component.

        Args:
            component (int): Index of the principal component to remove (0-based).
            X (np.ndarray): Feature matrix.
            y (np.ndarray): Label array.
            pca (PCA): Pre-fitted PCA object.
        """
        # Remove PC1
        Xmean = X - X.mean(axis=0)
        value = Xmean @ pca.components_[component]
        pc1 = value.reshape(-1, 1) @ pca.components_[component].reshape(1, -1)
        Xremove = X - pc1
        plt.figure(figsize=(8, 6))
        plt.scatter(Xremove[:, 0], Xremove[:, 1], c=y)
        plt.xlabel("0")
        plt.ylabel("1")
        plt.title("Two features from the dataset after removing PC1")
        plt.show()

        output_path = os.path.join(self.output_path, name)
        os.makedirs(output_path, exist_ok=True)
        plt.savefig(os.path.join(output_path, f"pca_pc{component + 1}.pdf"))
        plt.close()

    def create_plots_binary(
        self, ds_X: list[np.ndarray], ds_y: list[np.ndarray], data: list[Dataset]
    ) -> None:
        """
        Generate 2D and 3D PCA plots, and visualizations after removing PC1, PC2, and PC3.

        Args:
            X (np.ndarray): Feature matrix.
            y (np.ndarray): Label array.
        """
        for X, y, ds in zip(ds_X, ds_y, data):
            if "heicloud" in ds.name:
                tsne_x = X
                tsne_y = y

        for X, y, ds in zip(ds_X, ds_y, data):
            if "dgarchive" in ds.name:
                logger.info(f"Plot data for {ds.name}")
                self._plot_tsne(
                    np.concatenate((X, tsne_x), axis=0),
                    np.concatenate((y, tsne_y), axis=0),
                    ds_name=f"{ds.name}",
                )

        for X, y, ds in zip(ds_X, ds_y, data):
            if not "dgarchive" in ds.name:
                logger.info(f"Plot data for {ds.name}")
                self._plot_pca_2d(X=X, y=y, name=ds.name)
                self._plot_pca_3d(X=X, y=y, name=ds.name)
                # Show the principal components
                pca = PCA().fit(X)
                logger.info("Principal components:")
                logger.info(pca.components_)
                # Remove PC1
                self._remove_feature(0, X, y, pca, name=ds.name)
                # Remove PC2
                self._remove_feature(1, X, y, pca, name=ds.name)
                # Remove PC3
                self._remove_feature(2, X, y, pca, name=ds.name)
                # print the explained variance ratio
                logger.info("Explainedd variance ratios:")
                logger.info(pca.explained_variance_ratio_)

                self._plot_label_distribution(data, name=ds.name)
                self._plot_tsne(X, y, ds.name)

            # df_data = data.to_pandas()
            # # Assuming your data is in a DataFrame called 'df' with a 'condition' column
            # condition1_data = df_data[df_data["class"] == 1]
            # condition2_data = df_data[df_data["class"] == 0]

            # # List of measurements (you can use all or a subset)
            # measurements = df_data.columns.tolist()[
            #     1:
            # ]  # [1:] to drop the condition column in the beginning
            # self._plot_data_distribution(
            #     data_condition1=condition1_data,
            #     data_condition2=condition2_data,
            #     measurements=measurements,
            #     condition1_name="Benign",
            #     condition2_name="Malicious",
            #     name=ds.name
            # )

    def create_plots_multiclass(
        self, ds_X: list[np.ndarray], ds_y: list[np.ndarray], data: list[Dataset]
    ) -> None:
        """Create Plots for multiclass.

        Args:
            ds_X (list[np.ndarray]): X
            ds_y (list[np.ndarray]): y
            data (list[Dataset]): pl.DataFrame
        """
        # Plot label distribution from DGArchive
        df_dgarchive_list = []
        for X, y, ds in zip(ds_X, ds_y, data):
            if "dgarchive" in ds.name:
                df_dgarchive_list.append(data)
        df_dgarchive = pl.concat(df_dgarchive_list)
        self._plot_label_distribution(df_dgarchive, name="dgarchive")

    def _plot_data_distribution(
        self,
        data_condition1,
        data_condition2,
        measurements,
        ds_name,
        condition1_name="Condition 1",
        condition2_name="Condition 2",
        exclude_outliers=True,
        percentile=99,
    ) -> None:
        """
        Analyzes the distributions of network traffic between two conditions.
        Calculates the Kolmogorov-Smirnov (KS) statistic and Earth Mover's Distance (EMD)
        for each measurement, and plots the distributions with outlier exclusion.
        Saves plots to a directory.

        Parameters:
            data_condition1 (pd.DataFrame): Data for first condition
            data_condition2 (pd.DataFrame): Data for second condition
            measurements (list): List of measurement names to analyze
            condition1_name (str): Name of first condition for plotting
            condition2_name (str): Name of second condition for plotting
            exclude_outliers (bool): Whether to exclude outliers based on percentile (default: True)
            percentile (int): Percentile threshold for outlier exclusion (default: 99)
        """

        os.makedirs(self.output_path, exist_ok=True)

        results = []

        # Calculate number of rows needed for subplots (6 columns)
        n_rows = int(np.ceil(len(measurements) / 6))

        # Create figures for density plots and histograms
        fig_density, axs_density = plt.subplots(n_rows, 6, figsize=(36, 8 * n_rows))
        fig_hist, axs_hist = plt.subplots(n_rows, 6, figsize=(36, 8 * n_rows))

        # Flatten axes arrays for easier indexing
        axs_density = axs_density.flatten() if n_rows > 1 else [axs_density]
        axs_hist = axs_hist.flatten() if n_rows > 1 else [axs_hist]

        for i, measurement in enumerate(measurements):
            # Extract measurement data for both conditions
            arr1 = data_condition1[measurement].values
            arr2 = data_condition2[measurement].values

            # Replace inf values with NaN
            arr1 = np.where(np.isinf(arr1), np.nan, arr1)
            arr2 = np.where(np.isinf(arr2), np.nan, arr2)

            # Exclude outliers if requested
            if exclude_outliers:
                threshold1 = np.nanpercentile(arr1, percentile)
                threshold2 = np.nanpercentile(arr2, percentile)
                arr1 = np.where(arr1 > threshold1, np.nan, arr1)
                arr2 = np.where(arr2 > threshold2, np.nan, arr2)

            # Remove NaN values for statistical calculations
            arr1_clean = arr1[~np.isnan(arr1)]
            arr2_clean = arr2[~np.isnan(arr2)]

            # Calculate statistics if there's enough data
            if len(arr1_clean) > 0 and len(arr2_clean) > 0:
                ks_stat, ks_p_value = ks_2samp(arr1_clean, arr2_clean)
                emd = wasserstein_distance(arr1_clean, arr2_clean)
            else:
                ks_stat = ks_p_value = emd = np.nan

            # Store results
            results.append(
                {
                    "Measurement": measurement,
                    "KS_Statistic": ks_stat,
                    "KS_p_value": ks_p_value,
                    "EMD": emd,
                    "N_condition1": len(arr1_clean),
                    "N_condition2": len(arr2_clean),
                }
            )

            # Create density plot
            ax_density = axs_density[i]
            if len(arr1_clean) > 0:
                sns.kdeplot(
                    arr1_clean,
                    fill=True,
                    color="blue",
                    label=condition1_name,
                    ax=ax_density,
                    warn_singular=False,
                )
            if len(arr2_clean) > 0:
                sns.kdeplot(
                    arr2_clean,
                    fill=True,
                    color="orange",
                    label=condition2_name,
                    ax=ax_density,
                    warn_singular=False,
                )
            ax_density.set_title(f"{measurement}", fontsize=10)
            ax_density.set_xlabel("Value", fontsize=8)
            ax_density.set_ylabel("Density", fontsize=8)
            ax_density.legend(fontsize=8)

            # Create histogram
            ax_hist = axs_hist[i]
            if len(arr1_clean) > 0:
                ax_hist.hist(
                    arr1_clean,
                    bins=30,
                    color="blue",
                    alpha=0.5,
                    label=condition1_name,
                )
            if len(arr2_clean) > 0:
                ax_hist.hist(
                    arr2_clean,
                    bins=30,
                    color="orange",
                    alpha=0.5,
                    label=condition2_name,
                )
            ax_hist.set_title(f"{measurement}", fontsize=10)
            ax_hist.set_xlabel("Value", fontsize=8)
            ax_hist.set_ylabel("Frequency", fontsize=8)
            ax_hist.legend(fontsize=8)

        # Remove empty subplots if any
        for j in range(i + 1, len(axs_density)):
            fig_density.delaxes(axs_density[j])
            fig_hist.delaxes(axs_hist[j])

        # Adjust layout and save plots
        fig_density.tight_layout()
        fig_density.subplots_adjust(hspace=0.4, wspace=0.3)
        fig_hist.tight_layout()
        fig_hist.subplots_adjust(hspace=0.4, wspace=0.3)
        plt.show()

        output_path = os.path.join(self.output_path, ds_name)
        os.makedirs(output_path, exist_ok=True)
        density_plot_path = os.path.join(output_path, "density_plots.pdf")
        hist_plot_path = os.path.join(output_path, "histogram_plots.pdf")
        fig_density.savefig(density_plot_path, dpi=300)
        fig_hist.savefig(hist_plot_path, dpi=300)
        logger.info(f"Density plots saved to: {density_plot_path}")
        logger.info(f"Histogram plots saved to: {hist_plot_path}")
        plt.close()


class Explainer:
    """Explainer class to interpret sklearn.ensemble or XGBClassifier models after training."""

    def __init__(self, output_path: str = f"./{RESULT_FOLDER}"):
        self.output_path = output_path

    def __rescale_rule(self, rule: str, scaler, feature_names: list[str]) -> str:
        """
        Rescale feature thresholds in a rule back to their original (pre-scaled) values.

        Args:
            rule (str): A rule string (e.g., "feature1 > 0.5 and feature2 <= 1.3").
            scaler (sklearn.preprocessing): A fitted scaler object with an inverse_transform method.
            feature_names (list[str]): List of original feature names.

        Returns:
            str: Rule with scaled thresholds replaced by original (unscaled) values.
        """
        # If scaler is none, no rescaling is needed
        if scaler is None:
            return rule

        # Parse the rule to extract numeric values
        for i, feature in enumerate(feature_names):
            # Replace scaled values with rescaled values
            if feature in rule:
                # Find the numeric value associated with this feature in the rule
                # Example format: "feature > value" or "feature <= value"
                parts = rule.split()
                for j, part in enumerate(parts):
                    if (
                        part == feature
                        and j + 2 < len(parts)
                        and parts[j + 1] in {">", "<=", "=", ">=", "<"}
                    ):
                        # Extract and rescale the numeric value
                        try:
                            scaled_value = float(parts[j + 2])

                            # Create a placeholder for the scaler with the same shape as x_test
                            placeholder = [0] * len(feature_names)
                            placeholder[i] = (
                                scaled_value  # Set the scaled value for the current feature
                            )

                            # Inverse transform using the scaler
                            original_value = scaler.inverse_transform([placeholder])[
                                0, i
                            ]

                            # Round the rescaled value to 3 digits
                            rounded_value = round(original_value, 3)

                            # Replace the scaled value in the rule with the original value
                            parts[j + 2] = str(rounded_value)
                        except ValueError:
                            continue  # Skip if the value is not numeric

                # Reconstruct the rule with rescaled values
                rule = " ".join(parts)
        return rule

    def interpret_model(
        self,
        model,
        x_test: np.ndarray,
        y_test: np.ndarray,
        df_cols: list[str],
        scaler=None,
    ) -> list[str]:
        """
        Interpret a trained model by extracting decision rules and optionally rescaling them.

        Args:
            model (sklearn.ensemble.BaseEnsemble | XGBClassifier): Trained ML model.
            x_test (np.ndarray): Test set features.
            y_test (np.ndarray): Test set labels.
            df_cols (list[str]): Column names of the features.
            model_name (str): Name used for saving output files.
            scaler (optional): Scaler used in preprocessing, e.g., StandardScaler. Defaults to None.
        """
        print(df_cols)
        # Create TE2RULES explainer
        explainer = ModelExplainer(model, feature_names=df_cols)

        # Generate explanation
        rules = explainer.explain(x_test, y_test.tolist())

        # Rescale values in rules if scaler is provided
        if scaler is not None:
            rules = [self.__rescale_rule(rule, scaler, df_cols) for rule in rules]

        return rules
