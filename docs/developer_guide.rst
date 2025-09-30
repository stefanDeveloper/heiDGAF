Developer Guide
===============

This section describes useful information for contributors of the project.

Commit Hook
------------

Contributing to the project you might be noting failed pipeline runs.
This can be due to the pre.commit hook finding errors in the formatting. Therefore, we suggest you run

.. code-block:: console

   (.venv) pre-commit run --show-diff-on-failure --color=always --all-files

before committing your changes to GitHub.
This reformates the code accordingly, preventing errors in the pipeline.
