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


TODO: add configuration parameters for the training, test and explanation process
------------------------------------------------------------------------------------