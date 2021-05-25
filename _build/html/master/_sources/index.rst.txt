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

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   self
   getting_started
   tutorial
   usage
   best_practices
   design
   database
   support
   dev_guide/index
   contributing

