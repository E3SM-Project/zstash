***************************************
Testing directions for making a release
***************************************

1. Have three shells open: one on Chrysalis, one on Compy, and one on Perlmutter. Do the following steps on each machine.

    * If running on Perlmutter, it is preferable to run from ``$CSCRATCH`` rather than ``/global/homes``. Running from the latter may result in a 'Resource temporarily unavailable' error.
    * If running on Compy, it is necessary to run ``test_globus.py`` from a sub-directory of ``/compyfs`` rather than ``/qfs``. 

2. ``cd`` to the ``zstash`` directory.

3. Check out a branch to test on.

    a. test dev (run before making a new zstash RC) ::

        git fetch upstream main
	git checkout -b test_pre_zstash_rc<#> upstream/main
	git log # check the commits match https://github.com/E3SM-Project/zstash/commits/main

    b. test new Unified RC ::

        git fetch upstream main
	git checkout -b test_unified_rc<#>_<machine_name> upstream/main
	git log # check the commits match https://github.com/E3SM-Project/zstash/commits/main

    c. test final Unified ::

	git fetch upstream main
	git checkout -b test_unified_<#>_<machine_name> upstream/main
	git log # check the commits match https://github.com/E3SM-Project/zstash/commits/main

4. Set up your environment.

    a. test dev (run before making a new zstash RC): Set up a new development environment. This ensures that testing will use the latest conda changes. Note that you will need to run ``conda remove -n zstash_dev_pre_rc<#> --all`` first if you have previously done this step. ::

        mamba clean --all
        mamba env create -f conda/dev.yml -n zstash_dev_pre_rc<#>
        conda activate zstash_dev_pre_rc<#>
        pip install .

    b. test new Unified RC: Launch the E3SM Unified environment for the machine you're on. Change out the version numbers below. ::

        * Chrysalis: ``source /lcrc/soft/climate/e3sm-unified/test_e3sm_unified_1.9.0rc16_chrysalis.sh``
        * Compy: ``source /share/apps/E3SM/conda_envs/test_e3sm_unified_1.9.0rc16_compy.sh``
        * Perlmutter: ``source /global/common/software/e3sm/anaconda_envs/test_e3sm_unified_1.9.0rc16_pm-cpu.sh``

    c. test final Unified ::

        * Chrysalis: ``source /lcrc/soft/climate/e3sm-unified/load_latest_e3sm_unified_chrysalis.sh``
	* Compy: ``source /share/apps/E3SM/conda_envs/load_latest_e3sm_unified_compy.sh``
	* Perlmutter: ``source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh``

5. Activate Globus. Go to https://www.globus.org/. Log in with your NERSC credentials. Activate the following Globus endpoints using the corresponding credentials:

   * `Chrysalis <https://app.globus.org/file-manager/collections/61f9954c-a4fa-11ea-8f07-0a21f750d19b/overview>`_
   * `Compy <https://app.globus.org/file-manager/collections/68fbd2fa-83d7-11e9-8e63-029d279f7e24>`_
   * `Perlmutter/NERSC <https://app.globus.org/file-manager/collections/9d6d99eb-6d04-11e5-ba46-22000b92c6ec>`_

6. Run the unit tests with ``python -m unittest tests/test_*.py``.

    a. test dev (run before making a new zstash RC):

        * If there are any failures, fix the code (or tests). If you make any conda changes, go back to step 4a. If you otherwise change zppy source code, run ``pip install .`` and then redo step 6. If you only make changes to tests, you can immediately redo step 6.

    b. test new Unified RC:

        * If there are any failures, fix the code and go back to step 1, following the (a: test dev (run before making a new zstash RC)) directions.
	  
    c. test final Unified:

        * There should be no failures. If there are, a patch release of E3SM Unified may be required.
   
    For a, b, and c:

        * If there are no failures, proceed to the next step.

7. Make a pull request and merge any changes. This keeps the repo updated with the latest testing configurations. Mark yourself as the assignee, and mark "Testing" as the label. If you made bug fixes, add the "semver: bug" label. (If you've made no changes, skip this step).

8. Wrap up release testing:

    a. test dev (run before making a new zstash RC): Create the next zstash RC by following the "release candidates" directions at https://e3sm-project.github.io/zstash/_build/html/main/dev_guide/release.html.

    b. test new Unified RC: Create the next zstash RC by following the "production releases" directions at https://e3sm-project.github.io/zstash/_build/html/main/dev_guide/release.html.

    c. test final Unified: You can now safely remove old branches and environments. At https://github.com/E3SM-Project/zstash/branches, delete any branches that are no longer needed. Also, run: ::

        # Branches
        $ cd <zstash directory>
        $ git branch # Look at all branch names
        $ git branch -D <list branches you want to delete>

        # Environments
        $ conda env list
        # For each environment you want to delete, run:
        $ conda remove -n <environment_name> --all
