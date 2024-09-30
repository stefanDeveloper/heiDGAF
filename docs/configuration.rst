Logging Configuration
.....................

The following parameters control the logging behavior.

.. list-table:: ``logging`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - debug
     - ``false``
     - Enables debug-level logging if set to ``true``.


Software Configuration
......................

The following parameters control the behavior of heiDGAF, including the
functionality of the modules.

.. list-table:: ``heidgaf.logserver`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - hostname
     - ``172.27.0.8``
     - The hostname or IP address that the :class:`LogServer` will use to start and bind to the network interface. Should not be changed, because Docker uses this address for the internal network.
   * - port_in
     - ``9998``
     - The port on which the :class:`LogServer` will listen for incoming log lines.
   * - port_out
     - ``9999``
     - The port on which the :class:`LogServer` is available for collecting instances. Any instance connecting to this port will receive the latest log line stored on the server.
   * - max_number_of_connections
     - 1000
     - Maximum number of simultaneous connections that the :class:`LogServer` allows for both sending and receiving.
   * - listen_on_topic
     - ``"LogServer"``
     - Topic on which the :class:`LogServer` checks for incoming log lines via Kafka.

.. list-table:: ``heidgaf.inspector`` Parameters
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

.. list-table:: ``heidgaf.detector`` Parameters
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

.. list-table:: Other ``heidgaf`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - subnet.subnet_bits
     - 24
     - The number of bits, after which to cut off the client’s IP address to use as `subnet_id`.
   * - timestamp_format
     - ``"%Y-%m-%dT%H:%M:%S.%fZ"``
     - TODO
