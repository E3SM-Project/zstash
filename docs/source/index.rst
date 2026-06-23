.. zstash documentation master file, created by
   sphinx-quickstart on Fri Jul 28 17:31:00 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _index-label:

********************
Zstash documentation
********************

What is zstash?
===============

Zstash is an **HPSS long-term archiving** solution for E3SM.

Zstash is written entirely in Python using standard libraries.
Its design is intentionally minimalistic to provide an effective
long-term HPSS archiving solution without creating an overly complicated
(and hard to maintain) tool.

**Key features:**

* Files are archived into standard **tar files** with a user **specified maximum size**.
* Tar files are first created locally, then transferred to HPSS.
* **Checksums (md5)** of input files are computed *on-the-fly* during
  archiving. For large files, this saves a considerable amount of
  time compared to separate checksumming and archiving steps.
  Checksums are also computed *on-the-fly* for tars.
* Checksums and additional metadata (size, modification time, tar file and offset)
  are stored in a sqlite3 **index database**.
* **Database enables faster retrieval** of individual files by locating in which tar
  file a specific file is stored, as well as its location (offset) within the
  tar file.
* **File integrity** is verified by computing checksums on-the-fly while **extracting**
  files.

Source code is available on Github: `<https://github.com/E3SM-Project/zstash>`_.

To change the documentation version, use the version selector in the bottom left-hand corner.

For documentation not included in the version selector (<= ``v1.0.1``):

* `v1.0.1 <https://e3sm-project.github.io/zstash/_build_old/html-v1-0-1/index.html>`_
* `v1.0.0 <https://e3sm-project.github.io/zstash/_build_old/html-v1-0-0/index.html>`_

The documentation is organized into two major sections:

* :doc:`User Guide <user_guide/index>` for installation, day-to-day usage,
  Globus setup, and archive management
* :doc:`Developer Guide <dev_guide/index>` for contributing, testing, release
  work, and internal implementation details

User Guide pages
================

The user-facing documentation is organized under :doc:`user_guide/index` and
includes:

* :doc:`user_guide/design` for the high-level architecture and implementation overview
* :doc:`user_guide/getting_started` for installation and first-time setup
* :doc:`user_guide/usage` for command-line usage details
* :doc:`user_guide/globus` for Globus account setup and transfer workflows and ``.zstash.ini`` configuration details
* :doc:`user_guide/best_practices` for archive management recommendations
* :doc:`user_guide/database` for the archive index database layout
* :doc:`user_guide/support` for where to ask questions or report issues
* :doc:`user_guide/archived_documentation` for a full archive creation and extraction walkthrough


Developer Guide pages
=====================

The contributor and maintainer documentation is organized under
:doc:`dev_guide/index` and includes:

* :doc:`dev_guide/project_standards` for coding standards and conventions
* :doc:`dev_guide/ci` for continuous integration details
* :doc:`dev_guide/tar_tracking_modes` for tar tracking behavior in each storage
  mode
* :doc:`dev_guide/testing` for the test layout and execution guidance
* :doc:`dev_guide/releases` for the release process
* :doc:`dev_guide/contributing_to_docs` for development environment setup and contribution
  workflow

.. toctree::
   :maxdepth: 2
   :caption: Guides:

   self
   user_guide/index
   dev_guide/index
