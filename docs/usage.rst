Usage
=====

.. note::

   This page is under active development.

.. _installation:
.. _configuration:


Getting Started
---------------

To use heiDGAF, just use the provided ``docker-compose.yml`` to quickly bootstrap your environment:

.. code-block:: console

   $ docker compose -f docker/docker-compose.yml up

If you want to run containers individually, use:

.. code-block:: console

   $ docker compose -f docker/docker-compose.kafka.yml up
   $ docker run ...


Installation
------------

Install all Python requirements.

.. code-block:: console

   $ python -m venv .venv

.. code-block:: console

   $ source .venv/bin/activate

.. code-block:: console

   (.venv) $ sh install_requirements.sh

Now, you can start each module, e.g. the `Inspector`:

.. code-block:: console

   (.venv) $ python src/inspector/main.py


Commit Hook
------------

Contributing to the project you might be noting failed pipeline runs.
This can be due to the pre.commit hook finding errors in the formatting. Therefore, we suggest you run

.. code-block:: console

   (.venv) pre-commit run --show-diff-on-failure --color=always --all-files

before committing your changes to GitHub.
This reformates the code accordingly, preventing errors in the pipeline.


Configuration
-------------

.. include:: configuration.rst
