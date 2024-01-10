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

  $ python jobs/examples/ex1_frameworked_job.py --deploy=airflow

To create and deploy dashboards
---------------------------------------------------------

To create dashboards, Yaetos integrates "Panel" python library (https://panel.holoviz.org/), and some functionalities to help the integration. Dashboards can be defined in jupyter notebooks, as per standard Panel process, and published using "Panel" from within the notebook. See the example https://github.com/arthurprevot/yaetos/blob/master/dashboards/wikipedia_demo_dashboard.ipynb .

The dashboards can be defined in the job manifest (jobs_metadata.yml) to provide full lineage of the data all the way to the dashboards.
