Pipeline
~~~~~~~~

.. note::

   This page is under active development.

Overview
========

.. image:: media/pipeline_overview.pdf


Stage 1: Log Storage
====================

The primary goal of this stage is to temporarily store incoming log lines until they can be processed by subsequent
stages of the pipeline. This buffering approach ensures that incoming log data is not lost and can be processed
efficiently when the next stages are ready.

Once the data is stored, it can be retrieved by the next stage of the pipeline, referred to as
:ref:`Stage 2: Log Collection`, which connects to the server. This design allows multiple
:ref:`Log Collection<Stage 2: Log Collection>` instances to connect simultaneously, enabling load balancing and
efficient distribution of processing tasks.

Overview
--------

The :class:`LogServer` class is the core component of this stage. It opens two types of network ports:

1. **Incoming Port** (``port_in``): Used to receive log lines from various sources.
2. **Outgoing Port** (``port_out``): Used to send stored log lines to connected components.

This setup facilitates the asynchronous handling of log data, allowing various input sources to transmit log lines and
multiple processing components to retrieve them as needed.

Main Class
----------

.. py:currentmodule:: src.logserver.server
.. autoclass:: LogServer

Usage
-----

The :class:`LogServer` operates with two types of connections:

- **Incoming Connections** (``port_in``):

  - Components can connect to the incoming port to send log lines to the server. These components can include:

    - Direct APIs from systems generating log data.
    - Mock log line generators for testing.
    - File readers that parse log files and send individual log entries to the server.

  - The server is designed to handle multiple concurrent sources, allowing diverse and simultaneous input streams.

- **Outgoing Connections** (``port_out``):

  - Components can connect to the outgoing port to retrieve the next available log line from the server.
  - If no log lines are available at the time of the connection, the server will return ``None``.

This dual-port architecture allows for flexibility and scalability in log data handling.

Configuration
-------------

Configuration settings for the :class:`LogServer` are managed in the `config.yaml` file (key: ``heidgaf.logserver``).
The default settings include:

- **Port Configuration**:

  - ``port_in``: Default is ``9998``.
  - ``port_out``: Default is ``9999``.

- **Connection Limits**:

  - ``max_number_of_connections``: The maximum number of simultaneous connections allowed. Default is set to ``1000``.

These settings can be adjusted to meet specific deployment requirements, allowing customization of network ports and
connection handling.


Stage 2: Log Collection
=======================

The `Log Collection` stage is responsible for retrieving log lines from the :ref:`Log Storage<Stage 1: Log Storage>`,
parsing their information fields, and validating the data. Each field is checked to ensure it is of the correct type
and format. This stage ensures that all data is accurate, reducing the need for further verification in subsequent
stages. Any log lines that do not meet the required format are immediately discarded to maintain data integrity. Valid
log lines are then buffered and transmitted in batches after a pre-defined timeout or when the buffer reaches its
capacity. This minimizes the number of messages sent to the next stage and optimizes performance. The functionality of
the buffer is detailed in the subsection, :ref:`Buffer Functionality`.

Overview
--------

The `Log Collection` stage comprises two main classes:

1. :class:`LogCollector`: Connects to the :class:`LogServer` to retrieve and parse log lines, validating their format
   and content.
2. :class:`KafkaBatchSender`: Buffers validated log lines and sends them in batches, maintaining timestamps for
   accurate processing and analysis.

Main Classes
------------

.. py:currentmodule:: src.logcollector.collector
.. autoclass:: LogCollector

.. py:currentmodule:: src.base.batch_handler
.. autoclass:: CollectorKafkaBatchSender
   :show-inheritance:

Usage
-----

LogCollector
............

The :class:`LogCollector` connects to the :class:`LogServer` to retrieve one log line, which it then processes and
validates. The log line is parsed into its respective fields, each checked for correct type and format:

- **Field Validation**:

  - Checks include data type verification and value range checks (e.g., verifying that an IP address is valid).
  - Only log lines meeting the criteria are forwarded to the :class:`CollectorKafkaBatchSender`.

- **Connection to LogServer**:

  - The :class:`LogCollector` establishes a connection to the :class:`LogServer` and retrieves log lines when they
    become available.

- **Log Line Format**:

  - Log lines have the format:

    .. code-block::

        TIMESTAMP STATUS CLIENT_IP DNS_IP HOST_DOMAIN_NAME RECORD_TYPE RESPONSE_IP SIZE

    +----------------------+------------------------------------------------+
    | **Field**            | **Description**                                |
    +======================+================================================+
    | ``TIMESTAMP``        | The date and time when the log entry was       |
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
    | ``CLIENT_IP``        | The IP address of the client that made the     |
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

CollectorKafkaBatchSender
.........................

The :class:`CollectorKafkaBatchSender` manages the buffering and batch sending of validated log lines:

- **Batching Logic**:

  - Starts a timer upon receiving the first log entry.
  - Collects log entries into a `latest_messages` list.
  - Upon timer expiration or when a batch reaches the configured size (e.g., 1000 entries), the current and previous
    batches are concatenated and sent.

- **Timestamp Management**:

  - Maintains `begin_timestamp`, `center_timestamp`, and `end_timestamp` to track the timing of messages.
  - The `begin_timestamp` marks the start of the current batch, while the `end_timestamp` is updated with each batch's
    final message.
  - If no messages are present when the timer expires, nothing is sent.

- **Buffering Strategy**:

  - By default, uses a buffer to concatenate and send both the current and previous batches together.
  - This approach helps detect errors or attacks that may occur at the boundary between two batches when analyzed in
    :ref:`Stage 4: Data Inspection` and :ref:`Stage 5: Data Analysis`.

Configuration
-------------

Configuration settings for the :class:`LogCollector` and :class:`CollectorKafkaBatchSender` are managed in the
`config.yaml` file (key: ``heidgaf.collector``):

- **LogCollector Analyzation Criteria**:

    - ``valid_status_codes``: The accepted status codes for log line validation. Default list contains ``NOERROR``
      and ``NXDOMAIN``.
    - ``valid_record_types``: The accepted DNS record types for log line validation. Default list contains ``A`` and
      ``AAAA``.

- **Batch Configuration**:

    - ``batch_size``: The maximum number of log lines per batch. Default is ``1000``.
    - ``timeout_seconds``: The time interval (in seconds) after which the batch is sent, regardless of size. Default
      is ``60``.

Buffer Functionality
--------------------

The :class:`KafkaBatchSender` uses a dual-buffer strategy to ensure that messages are sent efficiently and with the
necessary context for later analysis. Here is a detailed example of how the batch processing works with timestamps:

Timestamps for `KafkaBatchSender`
.................................

The :class:`KafkaBatchSender` stores two lists of messages: `earlier_messages` and
`latest_messages`. The process for four example messages with a batch size of 2 is as follows:

1. **Initial Setup**:

   - On the first call of ``add_message()``, ``_reset_timer()`` is called for the first time.
   - In ``add_message()``, the `begin_timestamp` is set to the current time.

2. **Message Arrival**:

   - When ``message_1`` and ``message_2`` arrive, they are added to `latest_messages`.
   - Once the batch is full, it is sent to the :class:`KafkaProduceHandler` to be processed.
   - The `end_timestamp` is updated to the current time before sending.

3. **Buffering with Messages**:

   - Old messages ``message_1`` and ``message_2`` are moved to `earlier_messages`.
   - `begin_timestamp` remains the same, while `end_timestamp` becomes the `center_timestamp`.
   - New messages like ``message_3`` and ``message_4`` are added to `latest_messages`.

4. **Batch Completion**:

   - When the next batch is full, the `latest_messages` are sent with the `begin_timestamp` and the `end_timestamp`.
   - After sending, `begin_timestamp` is updated to `center_timestamp`, and `center_timestamp` becomes `end_timestamp`.

5. **Buffering Without Messages**:

   - If no messages arrive during the timer interval, the behavior is similar, ensuring that timestamps are updated
     appropriately without sending any data.

This approach allows for efficient data processing while ensuring that potential boundary issues between batches can be
detected and analyzed.


Stage 3: Log Filtering
======================

Overview
--------

Main Class
----------

.. py:currentmodule:: src.prefilter.prefilter

.. autoclass:: Prefilter

Usage
-----

Configuration
-------------


Stage 4: Data Inspection
========================

Overview
--------

Main Class
----------

.. py:currentmodule:: src.inspector.inspector

.. autoclass:: Inspector

Usage
-----

Configuration
-------------


Stage 5: Data Analysis
======================

Overview
--------

Main Class
----------

.. py:currentmodule:: src.detector.detector

.. autoclass:: Detector

Usage
-----

Configuration
-------------
