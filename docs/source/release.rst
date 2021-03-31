How to Prepare a Release
========================

In this guide, we'll cover:

* Preparing The Code For Release
* Creating A Release On GitHub
* Updating The sha256
* Releasing The Software On Anaconda
* Creating a New Version of the Documentation




Preparing The Code For Release
------------------------------

1. Pull the lastest code from whatever branch you want to release from.
It's usually ``master``. We will assume this to be the case
for all instructions on this page.

    ::

        git checkout master
        git pull <upstream-origin> master

Or checkout a branch:

    ::

        git fetch <upstream-origin> master
        git checkout -b <branch-name> <upstream-origin>/master

2. Edit ``version`` in ``setup.py`` to the new version. Don't prefix this with a "v".

3. Edit ``__version__`` in ``zstash/__init__.py``. Prefix this with a "v".

4. Change the ``version``  in ``conda/meta.yaml``.
``version`` is what the version of the software will be on Anaconda.
Don't prefix this with a "v". Reset the build number to 0 if necessary
(i.e., if higher builds of the previous version have been made).

5. Commit and push your changes.

    ::

        git commit -am 'Update to v0.4.1'
        git push <upstream-origin> master

Or:

    ::

        git commit -am 'Update to v0.4.1'
        git push <fork-origin> <branch-name>
        # Create pull request for the master branch

.. _github-release:

Creating A Release On GitHub
----------------------------

1. Go to the Releases on the GitHub repo of the project
`here <https://github.com/E3SM-Project/zstash/releases>`_.
and draft a new release.

2. ``Tag version`` and ``Release title`` should both be the version, including the "v".
``Target`` should be ``master``. Use ``Describe this release`` to write what features
the release adds. You can scroll through
`Zstash commits <https://github.com/E3SM-Project/zstash/commits/master>`_ to see
what features have been added recently.

3. Click "Publish release".

Updating The sha256
--------------------

1. Download "Source code (.tar.gz)" from the `GitHub releases page <https://github.com/E3SM-Project/zstash/releases>`_.

2. Run ``shasum -a 256`` on this file. For example:

    ::

        shasum -a 256 zstash-0.4.1.tar.gz

3. On your machine, pull the latest version of the code.
This will have the ``conda/meta.yaml`` we edited in the first section.

    ::

        git checkout master
        git pull <upstream-origin> master

Or:
    ::

        git fetch <upstream-origin> master
        git checkout -b <branch-name> <upstream-origin>/master

4. Change ``sha256`` in ``conda/meta.yaml`` to the result of step 2.

5. Commit and push your changes.

    ::

        git commit -am 'Edit sha256 for v0.4.1'
        git push <upstream-origin> master

Or:

    ::

        git commit -am 'Edit sha256 for v0.4.1'
        git push <fork-origin> <branch-name>
        # Create pull request for the master branch

Releasing The Software On Anaconda
----------------------------------

Since we're building with ``noarch``, you can run the below steps on
either a Linux or macOS machine. You do **not** need to run these steps on both.

It is recommended to use Miniconda3 rather than Anacdona3 to build packages.
The packages from the ``base`` Anaconda3 environment will come from the ``defaults`` channel,
not the ``conda-forge`` channel, and package updates fail when we try to update to the ``conda-forge`` version of
``conda-build``. If you don't have Miniconda3 yet, you can install it from
`Miniconda3 <https://docs.conda.io/en/latest/miniconda.html>`_.


1. Activate the ``base`` conda environment if you have not already done so:

    ::

        conda activate base

2. Make sure you have the latest versions of ``conda``, ``conda-build``, and ``anaconda-client``
by running ``conda update -n base conda conda-build anaconda-client``.

3. On your machine, pull the latest version of the code.
This will have the ``conda/meta.yaml`` we edited in the first and third sections.

    ::

        git checkout master
        git pull <upstream-origin> master

Or:
    ::

        git fetch <upstream-origin> master
        git checkout -b <branch-name> <upstream-origin>/master

4. Run ``conda env list``. Determine the path for the ``miniconda3`` installation you are using to build the package.
Typically, this will be ``~/miniconda3``. Run ``rm -rf <path>/conda-bld`` to ensure that previously built packages are
not included in the current build.

5. Run the following commands to make sure the ``conda-forge`` channel is included by default and that packages
come from that channel whenever possible:

    ::

        conda config --add channels conda-forge
        conda config --set channel_priority strict

6. Run ``conda build conda/``. The ``conda/`` folder is where ``meta.yaml`` is located. Keep the output of this command.
We'll use it in step 8.

7. Run ``conda search --info --use-local zstash``. The only dependency should be ``python >=3.6``. In particular,
``python_abi`` should not be listed as a dependency.

8. In the output of step 6, you should see something like the below.
We only have one package of type ``noarch``, meaning it works on both Linux and OSX and is compatible with multiple
versions of Python (3.6, 3.7, 3.8, etc.).
Since we have constrained python versions to >= 3.6 in the dependencies, it will not work with Python 2 or any other
version of Python <= 3.5.

    ::

        # Automatic uploading is disabled
        # If you want to upload package(s) to anaconda.org later, type:

        anaconda upload /usr/local/anaconda3/conda-bld/noarch/zstash-0.4.1-py_0.tar.bz2

        # To have conda build upload to anaconda.org automatically, use
        # $ conda config --set anaconda_upload yes

Copy the ``anaconda upload`` command and append ``-u e3sm`` to upload
the package to the ``e3sm`` Anaconda channel. Below is an example:

    ::

        anaconda upload /usr/local/anaconda3/conda-bld/noarch/zstash-0.4.1-py_0.tar.bz2 -u e3sm
        # If you don't appear to have anaconda installed, try the following:
        which conda
        # Append the top-level directory for anaconda (e.g., `/usr/local/anaconda3`) to the command.
        # For example:
        /usr/local/anaconda3/bin/anaconda upload /usr/local/anaconda3/conda-bld/noarch/zstash-0.4.1-py_0.tar.bz2  -u e3sm

If you're having permission issues uploading a package to the e3sm channel,
contact either Jill Zhang (zhang40@llnl.gov) or Rob Jacob (jacob@anl.gov) for permission.
You will need to have a `Conda account <https://anaconda.org/>`_.
Then, you can be given permission to upload a package.


9. Check the https://anaconda.org/e3sm/zstash page to view the newly updated package.


10. Notify the maintainers of the E3SM Unified environment about the new ``zstash`` release on the
`E3SM Confluence site <https://acme-climate.atlassian.net/wiki/spaces/WORKFLOW/pages/129732419/E3SM+Unified+Anaconda+Environment>`_.
Be sure to only update the ``zstash`` version number in the correct version(s) of the E3SM Unified environment.
This is almost certainly one of the versions listed under "Next versions".
If you are uncertain of which to update, leave a comment on the page asking.


Creating a New Version of the Documentation
-------------------------------------------

1. Be sure to have already completed :ref:`Creating A Release On GitHub <github-release>`.
This triggers the CI/CD workflow that handles publishing documentation versions.

2. Wait until the CI/CD build is successful. You can view all workflows at
`All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.

3. Changes will be available on the
`zstash documentation page <https://e3sm-project.github.io/zstash/>`_.
