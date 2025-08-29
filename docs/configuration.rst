Logline format configuration
............................

Users can define logcollector formats by adding a new log collector to the main configuration file (``config.yaml``). 
This enables users to let messages be forwarded to a new Kafka topic for a prefilter, inspector and detector to consume. 
This might be necessary if your detector is relying on information that is not in the required fields of preexisting log formats.
For changes, adapt the
``pipeline.log_collection.collectors.[collector_name].required_log_information`` parameter.

For example, a logline for the DNS protocol might look like this:
TODO: add new logline 

.. code-block:: console
  2025-04-04T14:45:32.458Z NXDOMAIN 192.168.3.152 10.10.0.3 test.com AAAA 192.168.15.34 196b

Each list entry of the parameter defines one field of the input logline, and the order of the entries corresponds to the
order of the values in each logline. Each list entry itself consists of a list with
three or four entries: For example, a field definition might look like this:

.. code-block:: console

   [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]

The first entry always corresponds to the name of the field. Some field values must exist in the logline, as they are
used by the modules. Some field names cannot be used, as they are defined for internal communication.

.. list-table:: Required and forbidden field names
   :header-rows: 0
   :widths: 15 50

   * - Required
     - ``ts``, ``src_ip``
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

.. list-table:: ``collectors`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - name
     - A unique name amongst the ``collectors``configurations top identify the collector instance.
   * - protocol_base
     - The lowercase protocol name to ingest data from. Currently supported: ``dns`` and ``http``.
   * - required_log_information
     - Defines the expected format for incoming log lines. See the :ref:`Logline format configuration` section for more
       details.

Each log_collector has a BatchHandler instance. Default confgurations for all Batch handlers are defined in ``pipeline.log_collection.default_batch_handler_config``.
You can override these values for each logcollector instance by adjusting the values inside the ``pipeline.log_collection.collectors.[collector_instance].batch_handler_config_override``.
The following list shows the available configuration options.

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




``pipeline.log_filtering``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``prefilter`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - name
     - A unique name amongst the prefilter configurations top identify the prefitler instance.
   * - relevance_method
     - The name of the method used to to check if a given logline is relevant for further inspection. 
       This check can be skipped by choosing ``"no_relevance_check"``. 
       Avalable configurations are: ``"no_relevance_check"``, ``"check_dga_relevance"``
   * - collector_name
     - The name of the collector configuration the prefilter consumes data from. The same collector name can be referenced in multiple prefilter configurations. 

``pipeline.data_inspection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``inspector`` Parameters
   :header-rows: 1
   :widths: 30 70
   * - Parameter
     - Description
   * - name
     - A unique name amongst the inspector configurations top identify the inspector instance.
   * - prefilter_name
     - The name of the prefitler configuration the inspector consumes data from. The same prefilter name can be referenced in multiple inspector configurations. 
   * - inspector_module_name
     - Name of the python file in ``"src/inspector/plugins/"`` the inspector should use. 
   * - inspector_class_name
     - Name of the class inside the ``inspector_module`` to use.



Inspectors can be added easily by implementing the base class for an inspector. More information is available at :ref:`inspection_stage`.
Each inspector might be needing additional configurations. These are also documented at :ref:`inspection_stage`.

To entirely skip the anomaly detection phase, you can set ``inspector_module_name: "no_inspector"`` and ``inspector_class_name: "NoInspector"``.

``pipeline.data_analysis``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``detector`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - name
     -
     - A unique name amongst the detector configurations top identify the detector instance.
   * - inspector_name
     -
     - The name of the inspector configuration the detector consumes data from. The same inspector name can be referenced in multiple detector configurations. 
   * - detector_module_name
     -
     - Name of the python file in ``"src/detector/plugins/"`` the detector should use. 
   * - detector_class_name
     -
     - Name of the class inside the ``detector_module`` to use.
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


``pipeline.zeek``
^^^^^^^^^^^^^^^^^

To configure the Zeek sensors to ingest data, an entry in ther ``pipeline.zeek.sensors`` must be adapted or added.
Each of the configured sensores is meant to run on a different machine or network interface to collect data.
Each instance configured needs to be setup using the ``docker-compose.yaml``. The dictionary name needs to exactly correspond with the 
name of the instance configured there. 
Each sensore has the following configuration parameters:

.. list-table:: ``zeek`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - static_analysis
     - A bool to indicate whether or not a static analysis should be executed. If ``true``, the PCAPs from ``"data/test_pcaps"`` which are mounted to 
       each Zeek instance are analyzed. If set to ``false``, a network analysis is executed on the configured network interfaces.
   * - protocols
     - List of lowercase names of protocols the Zeek sensor should be monitoring and sending in the Kafka Queues. Currently supported: ``"dns"`` and ``http``.
   * - interfaces
     - List of network interface names for a network analysis to monitor. As the Zeek containers run in ``host`` mode, all network interfaces of the node are automatically mounted and ready to be scraped.

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
     - ``hostname: kafka1, port: 8097, node_ip: 0.0.0.0``, ``hostname: kafka2, port: 8098, node_ip: 0.0.0.0``, ``hostname: kafka3, port: 8099, node_ip: 0.0.0.0``
     - Hostnames and ports of the Kafka brokers, given as list. The node ip is crucial and needs to be set to the actual IP of the system where the Kafka broker will be running on.
   * - kafka_topics_prefix
     - Not given here
     - Kafka topic name prefixes given as strings. These prefix name are used to construct the actual topic names based on the instance name (e.g. a collector instance name) that produces for the given stage.
       (e.g. a prefilter instance name is added as suffix to the prefilter_to_inspector prefix for the inspector to know where to consume.)
   * - monitoring.clickhouse_server.hostname
     - ``clickhouse-server``
     - Hostname of the ClickHouse server. Used by Grafana.

