How to Prepare a Release
========================

In this guide, we'll cover:

* Bumping the Version
* Releasing On GitHub
* Releasing The Software On Anaconda
* Creating a New Version of the Documentation

Bumping the Version
-------------------

1. Checkout the latest ``main``.
2. Checkout a branch with the name of the version.

    ::

        # Prepend "v" to <version>
        # For release candidates, append "rc" to <version>
        git checkout -b v<version> -t origin/<version>

3. Bump version using tbump.

    ::

        # Exclude "v" and <version> should match step 2
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
4. Create a pull request to the main repo and merge it.

.. _github-release:

Releasing on GitHub: release candidates
---------------------------------------

1. Create a tag for the release candidate at https://github.com/E3SM-Project/zstash/tags.

     ::

	$ git checkout main
	$ git fetch upstream
	$ git rebase upstream/main
	$ git tag -a v1.2.0rc1 -m "v1.2.0rc1"
	$ git push upstream v1.2.0rc1
   
Releasing on GitHub: production releases
----------------------------------------

1. Draft a new release `here <https://github.com/E3SM-Project/zstash/releases>`_.
2. Set `Tag version` to ``v<version>``, **including the "v"**. `@Target` should be ``main``.
3. Set `Release title` to ``v<version>``, **including the "v"**.
4. Use `Describe this release` to summarize the changelog.

   * You can scroll through `zstash commits <https://github.com/E3SM-Project/zstash/commits/main>`_ for a list of changes.

5. Click `Publish release`.
6. CI/CD release workflow is automatically triggered.


Releasing on conda-forge: release candidates
--------------------------------------------

1. Make a PR to `conda-forge <https://github.com/conda-forge/zstash-feedstock/>`_ from your fork of the feedstock. Note that the conda-forge bot does not work for release candidates.

   * Start from the current dev branch and update the version number and the sha256 sum manually.
   * Set the build number back to 0 if needed.
   * Make the dev branch the target of the PR. Then, the package build on conda-forge will end up with the ``zstash_dev`` label.

2. Check the https://anaconda.org/conda-forge/zstash page to view the newly updated package. Release candidates are assigned the ``zstash_dev`` label.

Releasing on conda-forge: production releases
------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles Anaconda releases.
2. Wait for a bot PR to come up automatically on conda-forge after the GitHub release. This can happen anywhere from 1 hour to 1 day later.
3. Re-render the PR (see `docs <https://conda-forge.org/docs/maintainer/updating_pkgs.html#rerendering-feedstocks>`_).
4. Merge the PR on conda-forge.
5. Check the https://anaconda.org/conda-forge/zstash page to view the newly updated package. Production releases are assigned the ``main`` label.
6. Notify the maintainers of the unified E3SM environment about the new release on the `E3SM Confluence site <https://acme-climate.atlassian.net/wiki/spaces/WORKFLOW/pages/129732419/E3SM+Unified+Anaconda+Environment>`_.

Creating a New Version of the Documentation
-------------------------------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles publishing documentation versions.
2. Wait until the CI/CD build is successful. You can view all workflows at `All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.
3. Changes will be available on the `zstash documentation page <https://e3sm-project.github.io/zstash/>`_.
