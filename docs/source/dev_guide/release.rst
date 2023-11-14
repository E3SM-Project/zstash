How to Prepare a Release
========================

In this guide, we'll cover:

* Bumping the Version
* Releasing On GitHub
* Releasing The Software On Anaconda
* Creating a New Version of the Documentation

Bumping the Version
-------------------

1. Checkout a branch with the name of the version.

    ::

        git fetch upstream main
        # Prepend "v" to <version>
        # For release candidates, append "rc" to <version>
        git checkout -b v<version> upstream/main

2. Bump version using tbump.

    ::

        # Exclude "v" and <version> should match the above step.
        # --no-tag is required since tagging is handled in "Releasing on GitHub"
        $ tbump <version> --no-tag

        :: Bumping from 1.1.0 to 1.2.0
        => Would patch these files
        - setup.py:26 version="1.1.0",
        + setup.py:26 version="1.2.0",
        - zstash/__init__.py:1 __version__ = "v1.1.0"
        + zstash/__init__.py:1 __version__ = "v1.2.0"
        - conda/meta.yaml:2 {% set version = "1.1.0" %}
        + conda/meta.yaml:2 {% set version = "1.2.0" %}
        - tbump.toml:5 current = "1.1.0"
        + tbump.toml:5 current = "1.2.0"
        => Would run these git commands
        $ git add --update
        $ git commit --message Bump to 1.2.0
        $ git push origin v1.2.0
        :: Looking good? (y/N)
        >

3. If you encounter ``Error: Command `git push upstream main` failed``, as in `zppy issue 470 <https://github.com/E3SM-Project/zppy/issues/470>`_, you can run ``git push upstream <branch>`` yourself.
	
4. Create a pull request to the main repo and merge it. Mark yourself as the assignee, and mark "Update version" as the label.

.. _github-release:

Releasing on GitHub: release candidates
---------------------------------------

1. Create a tag for the release candidate at https://github.com/E3SM-Project/zstash/tags. Example, bumping to v1.2.0rc1:

     ::

	$ git checkout main
	$ git fetch upstream
	$ git reset --hard upstream/main
	$ git tag -a v1.2.0rc1 -m "v1.2.0rc1"
	# Delete the branch from the tbump step. Otherwise, the push command won't work.
	$ git branch -D v1.2.0rc1
	$ git push upstream v1.2.0rc1
   
Releasing on GitHub: production releases
----------------------------------------

1. Draft a new release `here <https://github.com/E3SM-Project/zstash/releases>`_. You can save this and come back to it later, if need be.
2. Set `Tag version` to ``v<version>``, **including the "v"**. `@Target` should be ``main``.
3. Set `Release title` to ``v<version>``, **including the "v"**.
4. Use `Describe this release` to summarize the changelog.

   * You can scroll through `zstash commits <https://github.com/E3SM-Project/zstash/commits/main>`_ for a list of changes.
   * You can look at the last release to get an idea of how to format the description.

5. Click `Publish release`.
6. CI/CD release workflow is automatically triggered.


Releasing on conda-forge: release candidates
--------------------------------------------

1. If you don't have a local version of the conda-forge repo, run: ::

     git clone git@github.com:conda-forge/zstash-feedstock.git
     git remote add upstream git@github.com:conda-forge/zstash-feedstock.git

2. If you don't have a fork of the conda-forge repo, on `conda-forge <https://github.com/conda-forge/zstash-feedstock/>`_, click the "Fork" button in the upper right hand corner. Then, on your fork, click the green "Code" button, and copy the SSH path. Run: ::

     git remote add <your fork name> <SSH path for your fork>

3. Get the sha256 of the tag you made in "Releasing on GitHub: release candidates": ::

     curl -sL https://github.com/E3SM-Project/zstash/archive/v1.2.0rc1.tar.gz | openssl sha256

4. Make changes on a local branch. Example, bumping to v1.2.0rc1: ::

     $ git fetch upstream dev
     $ git checkout -b v1.2.0rc1 upstream/dev # You can name the branch anything you want
     # In `recipe/meta.yaml`, update the version and sha256 (and the build number if needed):
     {% set version = "1.2.0rc1" %} # Set to your version
     sha256: ... # The sha256 from the previous step
     number: 0 # build > number should always be 0
     $ git add -A
     $ git commit -m "v1.2.0rc1"
     $ git push <your fork name> v1.2.0rc1

5. Note that the conda-forge bot does not work for release candidates. So, make a PR manually from your fork of the feedstock to the ``dev`` branch of `conda-forge <https://github.com/conda-forge/zstash-feedstock/>`_. Then, the package build on conda-forge will end up with the ``zstash_dev`` label. You can add the "automerge" label to have the PR automatically merge once CI checks pass.

6. 6. After merging, CI runs again (in a slightly different way). Then, check the https://anaconda.org/conda-forge/zstash page to view the newly updated package. Release candidates are assigned the ``zstash_dev`` label. Note that it takes about 15 minutes for the files to propagate across conda-forge's mirroring services, which must happen before you can use the files.
   
Releasing on conda-forge: production releases
------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles Anaconda releases.
2. Wait for a bot PR to come up automatically on conda-forge after the GitHub release. This can happen anywhere from 1 hour to 1 day later.
3. Re-render the PR (see `docs <https://conda-forge.org/docs/maintainer/updating_pkgs.html#rerendering-feedstocks>`_).
4. Merge the PR on conda-forge.
5. Check the https://anaconda.org/conda-forge/zstash page to view the newly updated package. Production releases are assigned the ``main`` label.
6. Notify the maintainers of the unified E3SM environment about the new release on the `E3SM Confluence site <https://acme-climate.atlassian.net/wiki/spaces/WORKFLOW/pages/129732419/E3SM+Unified+Anaconda+Environment>`_.

   * Be sure to only update the ``zstash`` version number in the correct version(s) of the E3SM Unified environment.
   * This is almost certainly one of the E3SM Unified versions listed under “Next versions”. If you are uncertain of which to update, leave a comment on the page asking.

Creating a New Version of the Documentation
-------------------------------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles publishing documentation versions.
2. Wait until the CI/CD build is successful. You can view all workflows at `All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.
3. Changes will be available on the `zstash documentation page <https://e3sm-project.github.io/zstash/>`_.

Extra Resources
---------------

Conda-forge:

* https://conda-forge.org/docs/user/introduction.html#why-conda-forge
* https://conda-forge.org/docs/maintainer/infrastructure.html#admin-web-services
* https://acme-climate.atlassian.net/wiki/spaces/IPD/pages/3616735236/Releasing+E3SM+Software+on+Anaconda+conda-forge+channel
