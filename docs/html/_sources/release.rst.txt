How to Prepare a Release
========================

In this guide, we'll cover:

* Preparing The Code For Release
* Creating A Release On GitHub
* Updating The sha256
* Releasing The Software On Anaconda




Preparing The Code For Release
------------------------------

1. Pull the lastest code from whatever branch you want to release from.
It's usually ``master``. We will assume this to be the case
for all instructions on this page.

    ::

        git checkout master
        git pull origin master

Or checkout a branch:

    ::

        git fetch origin master
        git checkout -b <branch-name> origin/master

2. Edit ``version`` in ``setup.py`` to the new version. Don't prefix this with a "v".

3. Edit ``__version__`` in ``zstash/__init__.py``. Prefix this with a "v".

4. Change the ``version``  in ``conda/meta.yaml``.
``version`` is what the version of the software will be on Anaconda.
Don't prefix this with a "v". Reset the build number to 0 if necessary
(i.e., if higher builds of the previous version have been made).

5. Commit and push your changes.

    ::

        git commit -am 'Update to v0.4.1'
        git push origin master

Or:

    ::

        git commit -am 'Update to v0.4.1'
        git push <fork-name> <branch-name>
        # Create pull request for the master branch

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
        git pull origin master

Or:
    ::

        git fetch origin master
        git checkout -b <branch-name> origin/master

4. Change ``sha256`` in ``conda/meta.yaml`` to the result of step 2.

5. Commit and push your changes.

    ::

        git commit -am 'Edit sha256 for v0.4.1'
        git push origin master

Or:

    ::

        git commit -am 'Edit sha256 for v0.4.1'
        git push <fork-name> <branch-name>
        # Create pull request for the master branch

Releasing The Software On Anaconda
----------------------------------

Since we're building with ``noarch``, you can run the below steps on
either a Linux or macOS machine. You do **not** need to run these steps on both.


1. Make sure you have the latest versions of ``anaconda``, ``conda``, and ``conda-build``.
You cannot be in an existing Anaconda environment when you run ``conda update``,
so run ``conda deactivate`` first. If the ``conda deactivate`` command doesn't work,
use ``source deactivate``. This means you have an older version of Anaconda,
which should be remedied after the following ``update`` command.

    ::

        conda deactivate
        conda update anaconda conda conda-build

2. On your machine, pull the latest version of the code.
This will have the ``conda/meta.yaml`` we edited in the first and third sections.

    ::

        git checkout master
        git pull origin master

Or:
    ::

        git fetch origin master
        git checkout -b <branch-name> origin/master

3. Run the command below. The ``conda/`` folder is where ``meta.yaml`` is located and the ``-c``
tells conda the channels where the dependencies defined in ``meta.yaml`` can be found.

    ::

        conda build conda/ -c conda-forge

4. When ``conda build`` is completed, you should see something like the example below.
We only have one package of type ``noarch``, so it's compatible with both Python 2 and 3.
But since we only officially support Python 3, it might not work with Python 2.


    ::

        # Automatic uploading is disabled
        # If you want to upload package(s) to anaconda.org later, type:

        anaconda upload /usr/local/anaconda3/conda-bld/noarch/zstash-0.4.1-py_0.tar.bz2

        # To have conda build upload to anaconda.org automatically, use
        # $ conda config --set anaconda_upload yes

Copy the ``anaconda upload`` command and append ``-u e3sm`` to upload
the package to the ``e3sm`` Anaconda channel. Below is an example.

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


5. Check the https://anaconda.org/e3sm/zstash page to view the newly updated package.


6. Notify the maintainers of the unified E3SM environment about the new release on the
`E3SM Confluence site <https://acme-climate.atlassian.net/wiki/spaces/WORKFLOW/pages/129732419/E3SM+Unified+Anaconda+Environment>`_.
