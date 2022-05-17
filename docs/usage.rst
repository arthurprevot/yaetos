Usage
========

.. autosummary::
   :toctree: generated

To create a job
-------------------

Create a new job file, python or sql file. Set the inputs and output paths and any other parameters in the job file or in the job registry file.

..
   comment:: add a snapshot of an example or a code block!

To execute a job locally
------------------------

.. code-block:: console

   $ python jobs/examples/ex1_frameworked_job.py

To execute a pipeline locally
-----------------------------

.. code-block:: console

  $ python jobs/examples/ex1_frameworked_job.py --dependencies

To execute a job or pipeline in the cloud
-----------------------------------------

.. code-block:: console

  $ python jobs/examples/ex1_frameworked_job.py --deploy=EMR

To execute a job or pipeline in the cloud, on a scheduled
---------------------------------------------------------

.. code-block:: console

  $ python jobs/examples/ex1_frameworked_job.py --deploy=EMR_Scheduled
