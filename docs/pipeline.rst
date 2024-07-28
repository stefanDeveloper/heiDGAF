Pipeline
========

.. note::

   This page is under active development.

Overview
--------

Stage 1: Log Storage
--------------------

The purpose of this stage is to temporarily store incoming log lines until the subsequent stages
of the pipeline are available for further processing.

Once the data is ready, it can be collected by the :ref:`next stage<Stage 2: Log Collection>` via the appropriate
component connecting to the server. This approach allows for multiple :ref:`Log Collection<Stage 2: Log Collection>`
instances to balance the workload and split it.

Main Class
~~~~~~~~~~

LogServer
.........

.. py:currentmodule:: src.logserver.server

.. autoclass:: LogServer

Usage
~~~~~

The :class:`LogServer` accepts two types of connections: Another component can either connect to its ingoing port
(``port_in``) and push data onto the server, which is then stored, or it can connect to its outgoing port (``port_out``)
for receiving the next available log line on the server. If no log line is available at the moment of the connection,
``None`` is returned as a message.

A component connecting to the ingoing port can be a direct API from a system that returns log lines, a mock log line
generator for testing purposes, or a file containing the log lines. If the aforementioned option is to be
utilized, it will be necessary to employ an additional component to read the log lines from the file and transmit them
individually to the :class:`LogServer`.

Configuration
~~~~~~~~~~~~~

Both ports can be changed in ``config.yaml`` at ``heidgaf.logserver``, with default values ``port_in=9998`` and
``port_out=9999``. The maximum number of simultaneous connections can also be specified, by default it is set to
``max_number_of_connections=1000``.


Stage 2: Log Collection
-----------------------

The `Log Collection` stage is used to retrieve the log lines from the :ref:`Log Storage<Stage 1: Log Storage>` and
parse their information
fields. Each field is then reviewed to ensure that it is of the correct type and format. Upon completion of this
stage, all information is considered accurate and therefore does not require further verification in subsequent
stages. Any log lines that do not meet the required format are immediately discarded to reduce
the volume of incorrect data. The validated log lines are then stored in a buffer and only transmitted after
a pre-defined timeout or when the buffer is full, further minimising the number of messages sent to the
subsequent stage. The functionality of the buffer is detailed in the dedicated subsection, :ref:`Buffer
functionality`.

Main Classes
~~~~~~~~~~~~

.. py:currentmodule:: src.logcollector.collector

.. autoclass:: LogCollector


.. py:currentmodule:: src.base.batch_handler

.. autoclass:: CollectorKafkaBatchSender
   :show-inheritance:

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

