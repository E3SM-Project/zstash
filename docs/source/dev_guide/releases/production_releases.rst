.. _production_releases:

Preparing a production release
==============================

Step 1: Testing
---------------

Be sure to run the entire integration test suite before making a production release.

Step 2: Confluence
------------------

This step should already have been completed during the release-candidate phase, however it is good practice to double check that the `E3SM Unified version tracking page <https://e3sm.atlassian.net/wiki/spaces/DOC/pages/129732419/Packages+in+the+E3SM+Unified+conda+environment>`_ has had the next E3SM Unified version updated with the new ``zstash`` version number.

Step 3: tbump
-------------

Similar to the release-candidate directions, we'll use ``v1.2.3`` as an example version number here.

   .. code-block:: bash

        cd zstash
        git status # Confirm there's no uncommitted changes
        git fetch upstream main # This assumes you've named your remote for the main repo as "upstream"
        git checkout -b v1.2.3 upstream/main
        git log --oneline | head -n 5
        # Check that the latest commits match what's on https://github.com/E3SM-Project/zstash/commits/main/
        conda activate env-name # Activate any zstash dev environment you have; we just need `tbump`
        tbump 1.2.3 --no-tag
        # This creates a commit, but won't push it (because the branch isn't named `main`)
        git push upstream v1.2.3
        # Create, and "Update version" label" to, and merge the PR; delete the branch on GitHub

Step 4: Make the release on the zstash repo
--------------------------------------------

.. figure:: /_static/figures/github_release.png
   :alt: GitHub Release Diagram

1. Draft a new release `here <https://github.com/E3SM-Project/zstash/releases>`_. Click "Draft a new release".
2. Set Tag version to ``v1.2.3``, including the "v". ``@Target`` should be ``main``. Click "Tag", then "Create new tag" and enter "v1.2.3".
3. Set Release title to ``v1.2.3``, including the "v".
4. Use "Describe this release" to summarize the changelog. Write two sections: "Summary of changes" (the high-level summary) & "Full list of changes" (the categorized list of commits, from reviewing the `zstash commits <https://github.com/E3SM-Project/zstash/commits/main>`_).
5. Make sure "Set as the latest release" is checked.
6. Click "Publish release". Unlike the RCs, ``v1.2.3`` should now appear on _both_ `Tags <https://github.com/E3SM-Project/zstash/tags>`_ and `Releases <https://github.com/E3SM-Project/zstash/releases>`_.
7. CI/CD release workflow will be automatically triggered. The docs workflow is just for the docs. Clicking "Publish release" is responsible for triggering the bot PR on conda-forge.

Step 5: zstash-feedstock repo
------------------------------

1. Wait for a bot PR to come up automatically on conda-forge after the GitHub release. This can happen anywhere from 1 hour to 1 day later. Check https://github.com/conda-forge/zstash-feedstock/pulls. (Alternative: open an issue with the bot command: ``@conda-forge-admin, please update version`` and the PR will be opened.)
2. Complete any requirements to merge the PR.
3. Check the https://anaconda.org/conda-forge/zstash/files/manage page to view the newly updated package. Check it has the ``main`` label.

Step 6: Check the docs
----------------------

1. Wait for the docs workflow to complete successfully.
2. Wait until the CI/CD build is successful. You can view all workflows at `All Workflows <https://github.com/E3SM-Project/zstash/actions>`_.
3. Changes will be available on the `zstash documentation page <https://docs.e3sm.org/zstash/_build/html/main/index.html>`_.
