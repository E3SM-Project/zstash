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

Releasing on GitHub
-------------------

1. Draft a new release `here <https://github.com/E3SM-Project/zstash/releases>`_.
2. Set `Tag version` to ``v<version>``, **including the "v"**. `@Target` should be ``main``.
3. Set `Release title` to ``v<version>``, **including the "v"**.
4. Use `Describe this release` to summarize the changelog.

   * You can scroll through `zstash commits <https://github.com/E3SM-Project/zstash/commits/main>`_ for a list of changes.

5. If this version is a release candidate (``<version>`` appended with ``rc``), checkmark `This is a pre-release`.
6. Click `Publish release`.
7. CI/CD release workflow is automatically triggered.

Releasing on Anaconda
------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles Anaconda releases.
2. Wait until the CI/CD build is successful. You can view all workflows at `All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.
3. Check the https://anaconda.org/e3sm/zstash page to view the newly updated package.

   * Release candidates are assigned the ``e3sm_dev`` label
   * Production releases are assigned the ``main`` label

4. Notify the maintainers of the unified E3SM environment about the new release on the `E3SM Confluence site <https://acme-climate.atlassian.net/wiki/spaces/WORKFLOW/pages/129732419/E3SM+Unified+Anaconda+Environment>`_.

   * Be sure to only update the ``zstash`` version number in the correct version(s) of the E3SM Unified environment.
   * This is almost certainly one of the versions listed under “Next versions”. If you are uncertain of which to update, leave a comment on the page asking.

Creating a New Version of the Documentation
-------------------------------------------

1. Be sure to have already completed :ref:`Releasing On GitHub <github-release>`. This triggers the CI/CD workflow that handles publishing documentation versions.
2. Wait until the CI/CD build is successful. You can view all workflows at `All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.
3. Changes will be available on the `zstash documentation page <https://e3sm-project.github.io/zstash/>`_.
