Logline format configuration
............................

Configure the format and validation rules for DNS server loglines through flexible field definitions that
support timestamps, IP addresses, regular expressions, and list-based validation.

Configuration Overview
^^^^^^^^^^^^^^^^^^^^^^

Users can define the format and fields of their DNS server loglines through the
``pipeline.log_collection.collector.logline_format`` parameter. This configuration allows complete customization
of field types, validation rules, and filtering criteria for incoming log data.

For example, a logline might look like this:

.. code-block:: console

   2025-04-04T14:45:32.458123Z NXDOMAIN 192.168.3.152 10.10.0.3 test.com AAAA 192.168.15.34 196b

Field Definition Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each list entry of the parameter defines one field of the input logline, and the order of the entries corresponds to the
order of the values in each logline. Each list entry itself consists of a list with
two to four entries depending on the field type. For example, a field definition might look like this:

.. code-block:: console

   [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]

Field Names and Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first entry of each field definition always corresponds to the name of the field. Certain field names are required
for proper pipeline operation, while others are forbidden as they are reserved for internal use.

.. list-table:: Required and forbidden field names
   :header-rows: 1
   :widths: 15 50

   * - Category
     - Field Names
   * - **Required**
     - ``timestamp``, ``status_code``, ``client_ip``, ``record_type``, ``domain_name``
   * - **Forbidden**
     - ``logline_id``, ``batch_id``

**Required fields** must be present in the configuration as they are essential for pipeline processing.
**Forbidden fields** are reserved for internal communication and cannot be used as custom field names.

Field Types and Validation
^^^^^^^^^^^^^^^^^^^^^^^^^^

The second entry specifies the type of the field. Depending on the type defined, the method for defining
validation parameters varies. The third and fourth entries change depending on the type.

There are four field types available:

.. list-table:: Field types
   :header-rows: 1
   :widths: 20 25 20 35

   * - Field type
     - Format of 3rd entry
     - Format of 4th entry
     - Description
   * - ``Timestamp``
     - Timestamp format string
     - *(not used)*
     - Validates timestamp fields using Python's strptime format. Automatically converts to ISO format for internal processing.
       Example: ``"%Y-%m-%dT%H:%M:%S.%fZ"``
   * - ``IpAddress``
     - *(not used)*
     - *(not used)*
     - Validates IPv4 and IPv6 addresses. No additional parameters required.
   * - ``RegEx`` (Regular Expression)
     - RegEx pattern as string
     - *(not used)*
     - Validates field content against a regular expression pattern. If the pattern matches, the field is valid.
   * - ``ListItem``
     - List of allowed values
     - List of relevant values *(optional)*
     - Validates field values against an allowed list. Optionally defines relevant values for filtering in later pipeline stages.
       All relevant values must also be in the allowed list. If not specified, all allowed values are deemed relevant.

Configuration Examples
^^^^^^^^^^^^^^^^^^^^^

Here are examples for each field type:

.. code-block:: yaml

   logline_format:
     - [ "timestamp", Timestamp, "%Y-%m-%dT%H:%M:%S.%fZ" ]
     - [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]
     - [ "client_ip", IpAddress ]
     - [ "domain_name", RegEx, '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)' ]
     - [ "record_type", ListItem, [ "A", "AAAA" ] ]


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
     - Defines the expected format for incoming log lines. See the :ref:`Logline format configuration` section for more
       details.

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
     - ``univariate`` (options: ``multivariate``, ``ensemble``)
     - Mode of operation for the data inspector.
   * - ensemble.model
     - ``WeightEnsemble``
     -  Model to use when inspector mode is ``ensemble``.
   * - ensemble.module
     - ``streamad.process``
     - Python module for the ensemble model.
   * - ensemble.model_args
     -
     - Additional Arguments for the ensemble model.
   * - models.model
     - ``ZScoreDetector``
     - Model to use for data inspection
   * - models.module
     - ``streamad.model``
     - Base python module for inspection models
   * - models.model_args
     -
     - Additional arguments for the model
   * - models.model_args.is_global
     - ``false``
     -
   * - anomaly_threshold
     - ``0.01``
     - Threshold for classifying an observation as an anomaly.
   * - score_threshold
     - ``0.5``
     - Threshold for the anomaly score.
   * - time_type
     - ``ms``
     - Unit of time used in time range calculations.
   * - time_range
     - ``20``
     - Time window for data inspection

``pipeline.data_analysis``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``detector`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - model
     - ``rf`` option: ``XGBoost``
     - Model to use for the detector
   * - checksum
     - Not given here
     - Checksum for the model file to ensure integrity
   * - base_url
     - https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/
     - Base URL for downloading the model if not present locally
   * - threshold
     - ``0.5``
     - Threshold for the detector's classification.

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
