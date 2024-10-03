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

.. list-table:: ``pipeline.stage_1`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - input_kafka_topic
     - ``"/opt/file.txt"``
     - File name of a file to which new log lines are appended continuously. If no such file should be used, keep the default setting. If Docker is used, also keep this setting unchanged. Instead, the ``docker/.env`` variable ``MOUNT_PATH`` must be changed.
   * - input_kafka_topic
     - ``"LogServer"``
     - Topic on which the :class:`LogServer` checks for incoming log lines via Kafka.
   * - max_number_of_server_connections
     - 1000
     - Maximum number of simultaneous connections that the :class:`LogServer` allows for both sending and receiving.

.. list-table:: ``pipeline.stage_2`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - logline_format
     - TODO
     - TODO
   * - batch_size
     - 1000
     - TODO
   * - batch_timeout
     - 20.0
     - TODO
   * - subnet.subnet_bits
     - 24
     - The number of bits, after which to cut off the client’s IPv4 address to use as `subnet_id`.

.. list-table:: ``pipeline.stage_4.inspector`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - mode
     - univariate
     - TODO
   * - ensemble.model
     - WeightEnsemble
     - TODO
   * - ensemble.module
     - ``streamad.process``
     - TODO
   * - ensemble.model_args
     -
     - TODO
   * - models.model
     - ZScoreDetector
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
     - 0.01
     - TODO
   * - score_threshold
     - 0.5
     - TODO
   * - time_type
     - ms
     - TODO
   * - time_range
     - 20
     - TODO

.. list-table:: ``pipeline.stage_5.detector`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - model
     - xg
     - TODO
   * - checksum
     - 21d1f40c9e186a08e9d2b400cea607f4163b39d187a9f9eca3da502b21cf3b9b
     - TODO
   * - base_url
     - https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/
     - TODO
   * - threshold
     - 0.5
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
