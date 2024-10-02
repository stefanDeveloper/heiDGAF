Usage
=====

.. note::

   This page is under active development.

.. _installation:
.. _configuration:

Getting Started
---------------

If you want to use heiDGAF, just use the provided ``docker-compose.yml`` to quickly bootstrap your environment:

.. code-block:: console

   $ docker compose -f docker/docker-compose.yml up

Installation
------------

Install all Python requirements.

.. code-block:: console

   $ python -m venv .venv

.. code-block:: console

   $ source .venv/bin/activate

.. code-block:: console

   (.venv) $ pip install -r requirements/requirements-dev.txt -r requirements/requirements.detector.txt -r requirements/requirements.logcollector.txt -r requirements/requirements.prefilter.txt -r requirements/requirements.inspector.txt

Now, you can start each stage, e.g. the inspector:

.. code-block:: console

   (.venv) $ python src/inspector/main.py

Configuration
-------------

This section will show a table of all configuration values, and their default values.
