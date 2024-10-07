Logging Configuration
.....................

The following parameters control the logging behavior.

.. list-table:: ``logging`` Parameters
   :header-rows: 1
   :widths: 15 50

   * - Parameter
     - Description
   * - base
     - The ``debug`` field enables debug-level logging if set to ``true`` for all files, that do not contain the main modules.
   * - modules
     - For each module, the ``debug`` field can be set to show debug-level logging messages.

If a ``debug`` field is set to ``false``, only info-level logging is shown. By default, all the fields are set to ``false``.


Pipeline Configuration
......................

The following parameters control the behavior of each stage of the heiDGAF pipeline, including the
functionality of the modules.

``pipeline.log_storage``
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``logserver`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - input_kafka_topic
     - ``"LogServer"``
     - Kafka topic for incoming log lines.
   * - input_file
     - ``"/opt/file.txt"``
     - File for appending new log lines continuously.

       Keep this setting unchanged if using Docker; modify the ``MOUNT_PATH`` in ``docker/.env`` instead.
   * - max_number_of_connections
     - ``1000``
     - Maximum number of simultaneous connections for sending and receiving.

``pipeline.log_collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``collector`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - logline_format
     - Defines the expected format for incoming log lines. See the TODO section for more details.

.. list-table:: ``batch_handler`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - batch_size
     - ``1000``
     - TODO
   * - batch_timeout
     - ``20.0``
     - TODO
   * - subnet.subnet_bits
     - ``24``
     - The number of bits to trim from the client's IPv4 address for use as ``subnet_id``.

``pipeline.data_inspection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``inspector`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - mode
     - ``univariate``
     - TODO
   * - ensemble.model
     - ``WeightEnsemble``
     - TODO
   * - ensemble.module
     - ``streamad.process``
     - TODO
   * - ensemble.model_args
     -
     - TODO
   * - models.model
     - ``ZScoreDetector``
     - TODO
   * - models.module
     - ``streamad.model``
     - TODO
   * - models.model_args
     -
     - TODO
   * - models.model_args.is_global
     - ``false``
     - TODO
   * - anomaly_threshold
     - ``0.01``
     - TODO
   * - score_threshold
     - ``0.5``
     - TODO
   * - time_type
     - ``ms``
     - TODO
   * - time_range
     - ``20``
     - TODO

``pipeline.data_analysis``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``detector`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - model
     - ``xg``
     - TODO
   * - checksum
     -
     - TODO
   * - base_url
     - https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/
     - TODO
   * - threshold
     - ``0.5``
     - TODO

Environment Configuration
.........................

The following parameters control the infrastructure of the software.

.. list-table:: ``environment`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - timestamp_format
     - ``"%Y-%m-%dT%H:%M:%S.%fZ"``
     - TODO
   * - kafka_brokers
     - TODO
     - TODO
   * - logserver.hostname
     - ``172.27.0.8``
     - The hostname or IP address that the :class:`LogServer` will use to start and bind to the network interface.
   * - logserver.port_in
     - ``9998``
     - The port on which the :class:`LogServer` will listen for incoming log lines.
   * - logserver.port_out
     - ``9999``
     - The port on which the :class:`LogServer` is available for collecting instances. Any instance connecting to this port will receive the latest log line stored on the server.
