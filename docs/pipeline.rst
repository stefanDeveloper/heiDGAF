Pipeline
~~~~~~~~

Overview
========

The core component of the software's architecture is its data pipeline. It consists of five stages/modules, and data
traverses through it using Apache Kafka.

.. image:: media/heidgaf_architecture.png


Stage 1: Log Aggregation
========================

The Log Aggregation stage harnesses multiple Zeek sensors to ingest data from static (i.e. PCAP files) and dynamic sources (i.e. traffic from network interfaces).
The traffic is split protocolwise into Kafka topics and send to the Logserver for the Log Storage phase.

Overview
--------

The :class:`ZeekConfigurationHandler` takes care of the setup of a containerized Zeek sensor. It reads in the main configuration file and 
adjusts the protocols to listen on, the logformats of incoming traffic, and the Kafka queues to send to.

The :class:`ZeekAnalysisHandler` starts the actual Zeek instance. Based on the configuration it either starts Zeek in a cluster for specified network interfaces 
or in a single node instance for static analyses. 

Main Classes
------------

.. py:currentmodule:: src.zeek.zeek_config_handler
.. autoclass:: ZeekConfigurationHandler

.. py:currentmodule:: src.zeek.zeek_analysis_handler
.. autoclass:: ZeekAnalysisHandler

Usage and configuration
-----------------------

An analysis can be performed via tapping network inerfaces and by injecting pcap files. 
To adjust this, adapt the ``pipeline.zeek.sensors.[sensor_name].static_analysis`` value to True or false. 

- **``pipeline.zeek.sensors.[sensor_name].static_analysis``** set to True:

  - An static analysis is executed. The PCAP files are extracted from within the GitHub root directory under ``/data/test_pcaps"`` and mounted into the zeek container. All files ending in .PCAP are then read and analyzed by Zeek.
    Please Note that we do not recommend to use several Zeek instances for a static analysis, as the data will be read in multiple times, which impacts the benchmarks accordingly. 

- **``pipeline.zeek.sensors.[sensor_name].static_analysis``** set to False:

  - A network analysis is performed for the interfaces listed in ``pipeline.zeek.sensors.[sensor_name].interfaces``


You can start multiple instances of Zeek by adding more entries to the dictionary ``pipeline.zeek.sensors``.
Necessary attributes are: 
- ``pipeline.zeek.sensors.[sensor_name].static_analysis`` : **bool**
- if not static analysis: ``pipeline.zeek.sensors.[sensor_name].interfaces`` : **list**
- ``pipeline.zeek.sensors.[sensor_name].protocols`` : **list**

Stage 2: Log Storage
====================

This stage serves as the central ingestion point for all data. 

Overview
--------

The :class:`LogServer` class is the core component of this stage. It reads the Zeek Sensors inputs and directs them via Kafka to the following stages.

Main Class
----------

.. py:currentmodule:: src.logserver.server
.. autoclass:: LogServer

Usage and configuration
-----------------------

Currently, the :class:`LogServer` reads from the Kafka Queues specified by Zeek. These have a common prefix, specified in ``environment.kafka_topics_prefix.pipeline.logserver_in``. The suffix is the protocol name in lower case of the traffic.
The Logserver currently has no further configuration. 

Stage 3: Log Collection
=======================

The `Log Collection` stage is responsible for retrieving loglines from the :ref:`Log Storage<Stage 2: Log Storage>`,
parsing their information fields, and validating the data. Each field is checked to ensure it is of the correct type
and format. This stage ensures that all data is accurate, reducing the need for further verification in subsequent
stages. Any loglines that do not meet the required format are immediately discarded to maintain data integrity. Valid
loglines are then buffered and transmitted in batches after a pre-defined timeout or when the buffer reaches its
capacity. This minimizes the number of messages sent to the next stage and optimizes performance. The client's IP
address is retrieved from the logline and used to create the ``subnet_id`` with the number of subnet bits specified in
the configuration. The functionality of the buffer is detailed in the subsection, :ref:`Buffer Functionality`.

Overview
--------

The `Log Collection` stage comprises three main classes:

1. :class:`LogCollector`: Connects to the :class:`LogServer` to retrieve and parse loglines, validating their format
   and content. Adds ``subnet_id`` that it retrieves from the client's IP address in the logline.
2. :class:`BufferedBatch`: Buffers validated loglines with respect to their ``subnet_id``. Maintains the timestamps for
   accurate processing and analysis per key (``subnet_id``). Returns sorted batches.
3. :class:`BufferedBatchSender`: Adds messages to the data structure :class:`BufferedBatch`, maintains the timer
   and checks the fill level of the key-specific batches. Sends the key's batches if full, sends all batches at timeout.

Main Classes
------------

.. py:currentmodule:: src.logcollector.collector
.. autoclass:: LogCollector

.. py:currentmodule:: src.logcollector.batch_handler
.. autoclass:: BufferedBatch

.. py:currentmodule:: src.logcollector.batch_handler
.. autoclass:: BufferedBatchSender

Usage
-----

LogCollector
............

The :class:`LogCollector` connects to the :class:`LogServer` to retrieve one logline, which it then processes and
validates. The logline is parsed into its respective fields, each checked for correct type and format.
For each configuration of a logg collector in ``pipeline.logcollection.collectors``, a process is spun up in the resulting docker container
allowing for multiprocessing and threading. 

- **Field Validation**:

  - Checks include data type verification and value range checks (e.g., verifying that an IP address is valid).
  - Only loglines meeting the criteria are forwarded to the :class:`CollectorKafkaBatchSender`.

- **Subnet Identification**:

  - The configuration file specifies the number n of bits in a subnet (e.g. 24). The client's IP address serves as a
    base for the ``subnet_id``. For this, the initial IP address is cut off after n bits, the rest is filled with
    zeros, and ``_n`` is added to the end of the ``subnet_id``. For example:

    +------------------------+------------------------------------------------+
    | **Client IP address**  | **Subnet ID**                                  |
    +========================+================================================+
    | ``171.154.4.17``       | ``171.154.4.0_24``                             |
    +------------------------+------------------------------------------------+

- **Connection to LogServer**:

  - The :class:`LogCollector` establishes a connection to the :class:`LogServer` and retrieves loglines when they
    become available.

- **Log Line Format**:

  As the log information differs for each protocol, there is a default format per protocol. 
  This can be either adapted or a completely new one can be added as well. For more information
  please reffer to section :ref:`Logline format configuration`.

    .. code-block::

        DNS default logline format

        TS STATUS SRC_IP DNS_IP HOST_DOMAIN_NAME RECORD_TYPE RESPONSE_IP SIZE

    +----------------------+------------------------------------------------+
    | **Field**            | **Description**                                |
    +======================+================================================+
    | ``TS``        | The date and time when the log entry was              |
    |                      | recorded. Formatted as                         |
    |                      | ``YYYY-MM-DDTHH:MM:SS.sssZ``.                  |
    |                      |                                                |
    |                      | - **Format**: ``%Y-%m-%dT%H:%M:%S.%f`` (with   |
    |                      |   microseconds truncated to milliseconds).     |
    |                      | - **Time Zone**: ``Z``                         |
    |                      |   indicates Zulu time (UTC).                   |
    |                      | - **Example**: ``2024-07-28T14:45:30.123Z``    |
    |                      |                                                |
    |                      | This format closely resembles ISO 8601, with   |
    |                      | milliseconds precision.                        |
    +----------------------+------------------------------------------------+
    | ``STATUS``           | The status of the DNS query, e.g., ``NOERROR``,|
    |                      | ``NXDOMAIN``.                                  |
    +----------------------+------------------------------------------------+
    | ``SRC_IP``        | The IP address of the client that made the        |
    |                      | request.                                       |
    +----------------------+------------------------------------------------+
    | ``DNS_IP``           | The IP address of the DNS server processing    |
    |                      | the request.                                   |
    +----------------------+------------------------------------------------+
    | ``HOST_DOMAIN_NAME`` | The domain name being queried.                 |
    +----------------------+------------------------------------------------+
    | ``RECORD_TYPE``      | The type of DNS record requested, such as ``A``|
    |                      | or ``AAAA``.                                   |
    +----------------------+------------------------------------------------+
    | ``RESPONSE_IP``      | The IP address returned in the DNS response.   |
    +----------------------+------------------------------------------------+
    | ``SIZE``             | The size of the DNS query response in bytes.   |
    |                      | Represented in the format like ``150b``, where |
    |                      | the number indicates the size and ``b`` denotes|
    |                      | bytes.                                         |
    +----------------------+------------------------------------------------+



    .. code-block::

        HTTP default logline format

        TS SRC_IP SRC_PORT DST_IP DST_PORT METHOD URI STATUS_CODE REQUEST_BODY RESPONSE_BODY

    +----------------------+------------------------------------------------+
    | **Field**            | **Description**                                |
    +======================+================================================+
    | ``TS``        | The date and time when the log entry was              |
    |                      | recorded. Formatted as                         |
    |                      | ``YYYY-MM-DDTHH:MM:SS.sssZ``.                  |
    |                      |                                                |
    |                      | - **Format**: ``%Y-%m-%dT%H:%M:%S.%f`` (with   |
    |                      |   microseconds truncated to milliseconds).     |
    |                      | - **Time Zone**: ``Z``                         |
    |                      |   indicates Zulu time (UTC).                   |
    |                      | - **Example**: ``2024-07-28T14:45:30.123Z``    |
    |                      |                                                |
    |                      | This format closely resembles ISO 8601, with   |
    |                      | milliseconds precision.                        |
    +----------------------+------------------------------------------------+
    | ``SRC_IP``           | The IP address of the client that made the     |
    |                      | request.                                       |
    +----------------------+------------------------------------------------+
    | ``SRC_PORT``         | The source port of the cliend making the       |
    |                      | request                                        |
    +----------------------+------------------------------------------------+
    | ``DST_IP``           | The IP address of the target server for the    |
    |                      | request.                                       |
    +----------------------+------------------------------------------------+
    | ``DST_PORT``         | The port of the target server                  |
    +----------------------+------------------------------------------------+
    | ``METHOD``           | The HTTP method used (e.g. ``GET, POST``)      |
    +----------------------+------------------------------------------------+
    | ``URI``              | Path accessed in the request (e.g. ``/admin``) |
    +----------------------+------------------------------------------------+
    | ``STATUS_CODE``      | The HTTP status code returned (e.g. ``500``)   |
    +----------------------+------------------------------------------------+
    | ``REQUEST_BODY``     | The HTTP request payload (might be encrypted)  |
    +----------------------+------------------------------------------------+
    | ``RESPONSE_BODY``    | The HTTP response body (might be encrypted)    |
    +----------------------+------------------------------------------------+

BufferedBatch
.............

The :class:`BufferedBatch` manages the buffering of validated loglines as well as their timestamps:

- **Batching Logic and Buffering Strategy**:

  - Collects log entries into a ``batch`` dictionary, with the ``subnet_id`` as key.
  - Uses a ``buffer`` per key to concatenate and send both the current and previous batches together.
  - This approach helps detect errors or attacks that may occur at the boundary between two batches when analyzed in
    :ref:`Data-Inspection<inspection_stage>` and :ref:`Data-Analysis<detection_stage>`.
  - All batches get sorted by their timestamps at completion to ensure correct chronological order.
  - A `begin_timestamp` and `end_timestamp` per key are extracted and send as metadata (needed for analysis). These
    are taken from the chronologically first and last message in a batch.

CollectorKafkaBatchSender
.........................

The :class:`CollectorKafkaBatchSender` manages the sending of validated loglines stored in the :class:`BufferedBatch`:

- Starts a timer upon receiving the first log entry.
- When a batch reaches the configured size (e.g., 1000 entries), the current and previous
  batches of this key are concatenated and sent to the Kafka Broker(s) with topic ``Prefilter``.
- Upon timer expiration, the currently stored batches of all keys are sent. Serves as backup if batches don't reach
  the configured size.
- If no messages are present when the timer expires, nothing is sent.

Configuration
-------------

The instances of the class :class:`LogCollector` check the validity of incoming loglines. For this, they use the ``required_log_information`` configured
in the ``config.yaml``.

Configurations can arbitrarily added, adjusted and removed. This is especially useful if certain detectors need specialized log fields.
The following convention needs to be sticked to:

- Each entry in the ``required_log_information``  needs to be a list
- The first item is the name of the datafield as adjusted in Zeek
- The second item is the Class name the value should be mapped to for validation
- Depending on the class, the third item is a list of valid inputs 
- Depending on the class, the fourth item is a list of relevant inputs 


Buffer Functionality
--------------------

The :class:`BufferedBatch` class manages the batching and buffering of messages associated with specific keys, along
with the corresponding timestamps. The class ensures efficient data processing by maintaining two sets of messages -
those currently being batched and those that were part of the previous batch. It also tracks the necessary timestamps
to manage the timing of message processing.

Class Overview
..............

- **Batch**: Stores the latest incoming messages associated with a particular key.

- **Buffer**: Stores the previous batch of messages associated with a particular key, including the timestamps.

Key Procedures
..............

1. **Message Arrival and Addition**:

  - When a new message arrives, the ``add_message()`` method is called.
  - If the key already exists in the batch, the message is appended to the list of messages for that key.
  - If the key does not exist, a new entry is created in the batch.
  - **Example**:
    - ``message_1`` arrives for ``key_1`` and is added to ``batch["key_1"]``.

2. **Retrieving Message Counts**:

  - Use ``get_number_of_messages(key)`` to get the count of messages in the current batch for a specific key.
  - Use ``get_number_of_buffered_messages(key)`` to get the count of messages in the buffer for a specific key.

3. **Completing a Batch**:

  - The ``complete_batch()`` method is called to finalize and retrieve the batch data for a specific key.
  - **Scenarios**:

    - **Variant 1**: If only the current batch contains messages (buffer is empty), the batch is returned sorted by and with its timestamps. ``begin_timestamp`` reflects the timestamp of the first message in the batch, and ``end_timestamp`` the timestamp of the chronologically last message in the batch.
    - **Variant 2**: If both the batch and buffer contain messages, the buffered messages are included in the returned data. The ``begin_timestamp`` now reflects the first message's timestamp in the buffer instead of the batch.
    - **Variant 3**: If only the buffer contains messages (no new messages arrived), the buffer data is discarded.
    - **Variant 4**: If neither the batch nor the buffer contains messages, a ``ValueError`` is raised.

4. **Managing Stored Keys**:

  - The ``get_stored_keys()`` method returns a set of all keys currently stored in either the batch or the buffer, allowing the retrieval of all keys with associated messages or buffered data.

Example Workflow
................

1. **Initial Message**:

  - ``message_1`` arrives for ``key_1``, added to ``batch["key_1"]``.

2. **Subsequent Message**:

  - ``message_2`` arrives for ``key_1``, added to ``batch["key_1"]``.

3. **Completing the Batch**:

  - ``complete_batch("key_1")`` is called, and if ``buffer["key_1"]`` exists, it includes both buffered and batch
    messages, otherwise just the batch.
  - The current batch is moved to the buffer.

4. **Buffer Management**:

  - If no new messages arrive, ``buffer["key_1"]`` data is discarded upon the next call to ``complete_batch("key_1")``.

This class design effectively manages the batching and buffering of messages, allowing for precise timestamp tracking
and efficient data processing across different message streams.

Stage 4: Log Filtering
======================

Overview
--------

The `Log Filtering` stage is responsible for processing and refining log data by filtering out entries based on
specified error types. This step ensures that only relevant logs are passed on for further analysis, optimizing the
performance and accuracy of subsequent pipeline stages.

Main Class
----------

.. py:currentmodule:: src.prefilter.prefilter
.. autoclass:: Prefilter

The :class:`Prefilter` class serves as the primary component in this stage, handling the extraction and filtering of
log data.

Usage
-----

One :class:`Prefilter` per prefilter configuration in ``pipeline.log_filtering`` is started. Each instance loads from a Kafka topic name that depends on the logcollector the prefilter builds upon.
The prefix for each topic is defined in ``environment.kafka_topics_prefix.batch_sender_to_prefilter.`` and the suffix is the configured log collector name.
The prefilters extract the log entries and apply a filter function (or relevance function) to retain only those entries that match the specified requirements. 


Once the filtering process is complete, the refined data is sent back to the Kafka Brokers under the topic ``Inspect``
for further processing in subsequent stages.

Configuration
-------------

To customize the filtering behavior, the relevance function can be extended and adjusted in ``"src/base/logline_handler"`` and can be referenced in the ``"configuration.yaml"`` by the function name. 
Checks can be skipped by referencing the ``no_relevance_check`` function.
We currently support the following relevance methods:

    +---------------------------+-------------------------------------------------------------+
    | **Name**                  | **Description**                                             |
    +===========================+=============================================================+
    | ``no_relevance_check ``   | Skip the relevance check of the prefilters entirely.        |
    +---------------------------+-------------------------------------------------------------+
    | ``check_dga_relevance``   | Function to filter requests based on LisItems in the        |
    |                           | logcollector configuration. Using the fourth item in the    |
    |                           | list as a list of relevant status codes, only the request   |
    |                           | and responses are forwarded that include a  **NXDOMAIN**    |
    |                           | status code.                                                |
    +---------------------------+-------------------------------------------------------------+


Stage 5: Inspection
========================
.. _inspection_stage:

Overview
--------

The `Inspector` stage is responsible to run time-series based anomaly detection on prefiltered batches. This stage is essential to reduce
the load on the `Detection` stage.
Otherwise, resource complexity increases disproportionately.


Main Classes
------------

.. py:currentmodule:: src.inspector.inspector
.. autoclass:: InspectorAbstractBase

.. py:currentmodule:: src.inspector.inspector
.. autoclass:: InspectorBase

The :class:`InspectorBase` is the primary class for inspecotrs. It holds common functionalities and is responsible for data ingesting, sending, etc.. Any inspector build on top of this
class and needs to implement the methods specified by :class:`InspectorAbstractBase`. The class implementations need to go into ``"/src/inspector/plugins"``



Usage and Configuration
-----------------------

We currently support the following inspectors:

.. list-table::
   :header-rows: 1
   :widths: 15 30 55

   * - **Name**
     - **Description**
     - **Configuration**
   * - ``no_inspector``
     - Skip the anomaly inspection of data entirely.
     - No additional configuration
   * - ``stream_ad_inspector``
     - Uses StreamAD models for anomaly detection. All StreamAD models are supported (univariate, multivariate, ensembles).
     - - ``mode``: univariate (options: multivariate, ensemble)
       - ``ensemble.model``: WeightEnsemble (options: VoteEnsemble)
       - ``ensemble.module``: streamad.process
       - ``ensemble.model_args``: Additional Arguments for the ensemble model
       - ``models.model``: ZScoreDetector
       - ``models.module``: streamad.model
       - ``models.model_args``: Additional arguments for the model
       - ``anomaly_threshold``: 0.01
       - ``score_threshold``: 0.5
       - ``time_type``: streamad.process
       - ``time_range``: 20




Further inspectors can be added and referenced in the config by adjusting the ``pipeline.data_inspection.[inspector].inspector_module_name`` and ``pipeline.data_inspection.[inspector].inspector_class_name``.
Each inspector might need special configurations. For the possible configuration values, please reference the table above. 

StreamAD Inspector
...................

The inspector consumes batches on the topic ``inspect``, usually produced by the ``Prefilter``.
For a new batch, it derives the timestamps ``begin_timestamp`` and ``end_timestamp``.
Based on time type (e.g. ``s``, ``ms``) and time range (e.g. ``5``) the sliding non-overlapping window is created.
For univariate time-series, it counts the number of occurances, whereas for multivariate, it considers the number of occurances and packet size. :cite:`schuppen_fanci_2018`

An anomaly is noted when it is greater than a ``score_threshold``.
In addition, we support a relative anomaly threshold.
So, if the anomaly threshold is ``0.01``, it sends anomalies for further detection, if the amount of anomalies divided by the total amount of requests in the batch is greater than ``0.01``.

All StreamAD models are supported. This includes univariate, multivariate, and ensemble methods.
In case special arguments are desired for your environment, the ``model_args`` as a dictionary ``dict`` can be passed for each model.

Univariate models in `streamad.model`:

- :class:`ZScoreDetector`
- :class:`KNNDetector`
- :class:`SpotDetector`
- :class:`SRDetector`
- :class:`OCSVMDetector`

Multivariate models in `streamad.model`:
Currently, we rely on the packet size and number occurances for multivariate processing.

- :class:`xStreamDetector`
- :class:`RShashDetector`
- :class:`HSTreeDetector`
- :class:`LodaDetector`
- :class:`OCSVMDetector`
- :class:`RrcfDetector`

Ensemble prediction in ``streamad.process:

- :class:`WeightEnsemble`
- :class:`VoteEnsemble`

It takes a list of ``streamad.model`` to perform the ensemble prediction.

Stage 6: Detection
==================
.. _detection_stage:

Overview
--------

The `Detector` resembles the heart of heiDGAF. It runs pre-trained machine learning models to get a probability outcome for the DNS requests.
The pre-trained models are under the EUPL-1.2 license online available.
In total, we rely on the following data sets for the pre-trained models we offer:

- `CIC-Bell-DNS-2021 <https://www.unb.ca/cic/datasets/dns-2021.html>`_
- `DGTA-BENCH - Domain Generation and Tunneling Algorithms for Benchmark <https://data.mendeley.com/datasets/2wzf9bz7xr/1>`_
- `DGArchive <https://dgarchive.caad.fkie.fraunhofer.de/>`_

Main Classes
------------

.. py:currentmodule:: src.detector.detector
.. autoclass:: DetectorAbstractBase

.. py:currentmodule:: src.detector.detector
.. autoclass:: DetectorBase

.. py:currentmodule:: src.detector.plugins.dga_detector
.. autoclass:: DGADetector


The :class:`DetectorBase` is the primary class for Detectors. It holds common functionalities and is responsible for data ingesting, triggering alerts, logging, etc.. Any Detector is build on top of this
class and needs to implement the methods specified by :class:`DetectorAbstractBase`. The class implementations need to go into ``"/src/detector/plugins"``


Configuration and Usage
-----------------------

We currently support the following inspectors:


  +---------------------------+-------------------------------------------------------------+--------------------------------------------------------------------------------------------+
  | **Name**                  | **Description**                                             | **Configuration**                                                                          |                                               
  +===========================+=============================================================+============================================================================================+
  | ``DGADetector``           | Uses StreamAD models for anomaly detection. All StreamAD    | ``mode``: univariate (options: multivariate, ensemble)                                     |
  |                           | models are supported. This includes univariate, multivariate| ``ensemble.model``: WeightEnsemble (options: VoteEnsemble)                                 |
  |                           | and ensembles.                                              | ``ensemble.module``: streamad.process (Python module for the ensemble model)               |
  |                           |                                                             | ``ensemble.model_args``: Additional Arguments for the ensemble model.                      |
  |                           |                                                             | ``models.model``: ZScoreDetector (Model to use for data inspection)                        |
  |                           |                                                             | ``models.module``: streamad.model (Base python module for inspection models)               |
  |                           |                                                             | ``models.model_args``: Additional arguments for the model                                  |
  |                           |                                                             | ``anomaly_threshold``: 0.01 (Threshold for classifying an observation as an anomaly.)      |
  |                           |                                                             | ``score_threshold``: 0.5 (Threshold for the anomaly score.)                                |
  |                           |                                                             | ``time_type``: streamad.process (Unit of time used in time range calculations.)            |
  |                           |                                                             | ``time_range``: 20 (Time window for data inspection)                                       |
  +---------------------------+-------------------------------------------------------------+--------------------------------------------------------------------------------------------+

In case you want to load self-trained models, the configuration acn be adapted to load the model from a different location. Since download link is assembled the following way:
``<model_base_url>/files/?p=%2F<model_name>/<model_checksum>/<model_name>.pickle&dl=1"`` You can adapt the base url. If you need to adhere to another URL composition create 
A new detector class by either implementing the necessary base functions from :class:`DetectorBase` or by deriving the new class from :class:`DGADetector` and just overwrite the ``"get_model_download_url"`` method.

DGA Detector
...................
The :class:`DGADetector` consumes anomalous batches of requests.
It calculates a probability score for each request, and at last, an overall score of the batch.