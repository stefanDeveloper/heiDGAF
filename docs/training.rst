Training
~~~~~~~~

Overview
========

In total, we support ``RandomForest``, and ``XGBoost``.
The :class:`DetectorTraining` resembles the main function to fit any model.
After initialisation,

It supports various data sets:

- ``all``: Includes all available data sets
- ``cic``: Train on the CICBellDNS2021 data set
- ``dgta``: Train on the DTGA Benchmarking data set
- ``dgarchive``: Train on the DGArchive data set

For hyperparameter optimisation we use ``optuna``.
It offers GPU support to get the best parameters.


Training Parameters
-------------------

.. list-table::
   :header-rows: 1

   * - Parameter
     - Type
     - Default
     - Description
   * - ``--dataset``
     - ``[combine|dgarchive|cic|dgta]``
     - ``combine``
     - Data set to train model, choose between all available datasets.
   * - ``--dataset_path``
     - ``Path``
     -
     - Dataset path, follow folder structure.
   * - ``--dataset_max_rows``
     - ``int``
     - ``-1``
     - Maximum rows to load from each dataset.
   * - ``--model``
     - ``[xg|rf|gbm]``
     -
     - Model to train, choose between XGBoost, RandomForest, or GBM.
   * - ``--model_output_path``
     - ``Path``
     - ``./results/model``
     - Path to store model. Output is ``{MODEL}_{SHA256}.pickle``.

Testing Parameters
------------------

.. list-table::
   :header-rows: 1

   * - Parameter
     - Type
     - Default
     - Description
   * - ``--dataset``
     - ``[combine|dgarchive|cic|dgta]``
     - ``combine``
     - Data set to test model.
   * - ``--dataset_path``
     - ``Path``
     -
     - Dataset path, follow folder structure.
   * - ``--dataset_max_rows``
     - ``int``
     - ``-1``
     - Maximum rows to load from each dataset.
   * - ``--model``
     - ``[xg|rf|gbm]``
     -
     - Model architecture to test.
   * - ``--model_path``
     - ``Path``
     -
     - Path to trained model.

Explanation Parameters
----------------------

.. list-table::
   :header-rows: 1

   * - Parameter
     - Type
     - Default
     - Description
   * - ``--dataset``
     - ``[combine|dgarchive|cic|dgta]``
     - ``combine``
     - Data set to explain model predictions.
   * - ``--dataset_path``
     - ``Path``
     -
     - Dataset path, follow folder structure.
   * - ``--dataset_max_rows``
     - ``int``
     - ``-1``
     - Maximum rows to load from each dataset.
   * - ``--model``
     - ``[xg|rf|gbm]``
     -
     - Model architecture to explain.
   * - ``--model_path``
     - ``Path``
     -
     - Path to trained model.
