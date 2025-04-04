Logline format configuration
............................

Users can define the format and fields of their DNS server loglines. For this, change the
``pipeline.log_collection.collector.logline_format`` parameter:

For example, a logline might look like this:

.. code-block:: console

   2025-04-04T14:45:32.458Z NXDOMAIN 192.168.3.152 10.10.0.3 test.com AAAA 192.168.15.34 196b

Each list entry of the parameter defines one field of the input logline, and the order of the entries corresponds to the
order of the values in each logline. Each list entry itself consists of a list with
three or four entries: For example, a field definition might look like this:

.. code-block:: console

   [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]

The first entry always corresponds to the name of the field. Some field values must exist in the logline, as they are
used by the modules. Some field names are cannot be used, as they are defined for internal communication.

.. list-table:: Required and forbidden field names
   :header-rows: 0
   :widths: 15 50

   * - Required
     - ``timestamp``, ``status_code``, ``client_ip``, ``record_type``, ``domain_name``
   * - Forbidden
     - ``logline_id``, ``batch_id``

The second entry specifies the type of the field. Depending on the type defined here, the method for defining the
possible values varies. The third and fourth entry change depending on the type.
Please check the following table for more information on the types.

There are three types to choose from:

.. list-table:: Field types
   :header-rows: 1
   :widths: 20 20 20 30

   * - Field type
     - Format of 3rd entry
     - Format of 4th entry
     - Description
   * - ``RegEx`` (Regular Expression)
     - RegEx pattern as string
     -
     - The logline field is checked against the pattern. If the pattern is met, the field is valid.
   * - ``ListItem``
     - List of values
     - List of values (optional)
     - If the logline field value is in the first list, it is valid. If it is also in the second list, it is relevant
       for the inspection and detection algorithm. All values in the second list must also be in the first list, not
       vice versa. If this entry is not specified, all values are deemed relevant.
   * - ``IpAddress``
     -
     -
     - If the logline field value is an IPv4 or IPv6 address, it is valid.


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
   * - input_file
     - ``"/opt/file.txt"``
     - Path of the input file, to which data is appended during usage.

       Keep this setting unchanged when using Docker; modify the ``MOUNT_PATH`` in ``docker/.env`` instead.

``pipeline.log_collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``collector`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - logline_format
     - Defines the expected format for incoming log lines. See the :ref:`Logline format configuration` section for more details.

.. list-table:: ``batch_handler`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - batch_size
     - ``10000``
     - Number of entries in a Batch, at which it is sent due to reaching the maximum fill state.
   * - batch_timeout
     - ``30.0``
     - Time after which a Batch is sent. Mainly relevant for Batches that only contain a small number of entries, and
       do not reach the size limit for a longer time period.
   * - subnet_id.ipv4_prefix_length
     - ``24``
     - The number of bits to trim from the client's IPv4 address for use as `Subnet ID`.
   * - subnet_id.ipv6_prefix_length
     - ``64``
     - The number of bits to trim from the client's IPv6 address for use as `Subnet ID`.

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
     - ``rf``
     - TODO
   * - checksum
     - Not given here
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
     - Timestamp format used by the Inspector. Will be removed soon.
   * - kafka_brokers
     - ``hostname: kafka1, port: 8097``, ``hostname: kafka2, port: 8098``, ``hostname: kafka3, port: 8099``
     - Hostnames and ports of the Kafka brokers, given as list.
   * - kafka_topics
     - Not given here
     - Kafka topic names given as strings. These topics are used for the data transfer between the modules.
   * - monitoring.clickhouse_server.hostname
     - ``clickhouse-server``
     - Hostname of the ClickHouse server. Used by Grafana.
