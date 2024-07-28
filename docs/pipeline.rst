Pipeline
========

.. note::

   This page is under active development.

Overview
--------


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
-----------------------

The `Log Collection` stage is used to retrieve the log lines from the :ref:`Log Storage<Stage 1: Log Storage>` and
parse their information
fields. Each field is then reviewed to ensure that it is of the correct type and format. Upon completion of this
stage, all information is considered accurate and therefore does not require further verification in subsequent
stages. Any log lines that do not meet the required format are immediately discarded to reduce
the volume of incorrect data. The validated log lines are then stored in a buffer and only transmitted after
a pre-defined timeout or when the buffer is full, further minimising the number of messages sent to the
subsequent stage. The functionality of the buffer is detailed in the dedicated subsection, `Buffer
functionality`.

Main Classes
~~~~~~~~~~~~

.. py:currentmodule:: src.logcollector.collector
.. autoclass:: LogCollector


.. py:currentmodule:: src.base.batch_handler
.. autoclass:: KafkaBatchSender

Usage
~~~~~

Configuration
~~~~~~~~~~~~~

Buffer functionality
~~~~~~~~~~~~~~~~~~~~


Stage 3: Log Filtering
----------------------

Overview
~~~~~~~~

Main Class
~~~~~~~~~~

.. py:currentmodule:: src.prefilter.prefilter

.. autoclass:: Prefilter

Usage
~~~~~

Configuration
~~~~~~~~~~~~~


Stage 4: Data Inspection
------------------------

Overview
~~~~~~~~

Main Class
~~~~~~~~~~

.. py:currentmodule:: src.inspector.inspector

.. autoclass:: Inspector

Usage
~~~~~

Configuration
~~~~~~~~~~~~~


Stage 5: Data Analysis
----------------------

Overview
~~~~~~~~

Main Class
~~~~~~~~~~

.. py:currentmodule:: src.detector.detector

.. autoclass:: Detector

Usage
~~~~~

Configuration
~~~~~~~~~~~~~

