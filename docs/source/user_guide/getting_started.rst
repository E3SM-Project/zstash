.. _getting-started:

***************
Getting started
***************

Activate e3sm_unified environment
=================================

If you have an account on one of the E3SM supported machines, you can access ``zstash`` by activating ``e3sm_unified``, which is
a conda environment that pulls together Python and other E3SM tools such as
``e3sm_diags`` and ``zppy``.

The paths to ``e3sm_unified`` activation scripts are machine dependent. As of E3SM Unified 1.13.0, the supported machines and their corresponding activation scripts are:

**Andes**
    ::

     source /ccs/proj/cli115/software/e3sm-unified/load_latest_e3sm_unified_andes.sh

**Aurora**
    ::

     source /lus/flare/projects/E3SMinput/soft/e3sm-unified/load_latest_e3sm_unified_aurora.sh

**Chrysalis**
    ::

     source /lcrc/soft/climate/e3sm-unified/load_latest_e3sm_unified_chrysalis.sh

**Compy**
    ::

     source /share/apps/E3SM/conda_envs/load_latest_e3sm_unified_compy.sh

**Dane**
    ::

     source /usr/workspace/e3sm/apps/e3sm-unified/load_latest_e3sm_unified_dane.sh

**Frontier**
    ::

     source /ccs/proj/cli115/software/e3sm-unified/load_latest_e3sm_unified_frontier.sh

**Perlmutter (login or CPU nodes)**
    ::

     source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

**ALCF Polaris**
    ::

     source /lus/grand/projects/E3SMinput/soft/e3sm-unified/load_latest_e3sm_unified_polaris.sh


Change ``.sh`` to ``.csh`` for ``csh`` shells.

E3SM Unified and zstash versions
==============================

``zstash`` development is largely synced with ``e3sm_unified``. The last several releases have been:

* E3SM Unified 1.13.0: ``zstash 1.6.0``
* E3SM Unified 1.12.0: ``zstash 1.5.0``

To use ``zstash`` features/fixes not yet in a production release, you can use a development environment.

Nevertheless, it is possible that the version of ``zstash`` included in ``e3sm_unified`` may not be the latest. To install the latest stable release, refer to the following:

Setting up a development environment
====================================

.. code-block:: bash

        # Get the code ########################################################################

        # Set up your fork:
        # Go to https://github.com/E3SM-Project/zstash
        # Click the green "Code" button
        # Choose the SSH option and paste it here:
        git clone git@github.com:E3SM-Project/zstash.git
        cd zstash
        git remote -v # You should see the main repo listed as `origin`

        # A couple optional steps:
        git remote add upstream git@github.com:E3SM-Project/zstash.git # Use the name "upstream" instead of origin
        git remote add your-fork-name git@github.com:your-fork-name/zstash.git # Use your fork, if you have one

        # To use the latest code:
        git fetch upstream
        git checkout -b main upstream/main
        # To use code from a specific branch:
        git checkout -b that-branch-name upstream/that-branch-name

        # Set up the environment ##############################################################
        # First, make sure you have conda activated. Then:
        rm -rf build # Sometimes an existing `build` directory can cause problems.
        conda clean --all --y # This makes sure conda will pick up the latest information.
        conda env create -f conda/dev.yml -n env-name
        conda activate env-name
        pre-commit run --all-files # This is only necessary if you've made changes
        python -m pip install . # Install the code into your development environment (env-name)

Note: if you'd like to contribute to ``zstash`` rather than just using the latest code, please refer to the Developer Guide instead.



Running
=======

To run ``zstash``, refer to :doc:`user_guide/usage`.
