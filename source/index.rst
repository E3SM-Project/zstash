.. zstash documentation master file, created by
   sphinx-quickstart on Fri Jul 28 17:31:00 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

********************
Zstash documentation
********************

What is zstash?
===============

Zstash is an **HPSS long-term archiving** solution for E3SM.

Zstash is written entirely in Python (2.7) using standard libraries.
Its design is intentionally minimalistic to provide an effective
long-term HPSS archiving solution without creating an overly complicated
(and hard to maintain) tool.

**Key features:**

* Files are archived into standard **tar files** with a user **specified maximum size**.
* Tar files are first created locally, then transferred to HPSS.
* **Checksums (md5)** of input files are computed *on-the-fly* during
  archiving. For large files, this saves a considerable amount of
  time compared to separate checksumming and archiving steps.
* Checksums and additional metadata (size, modification time, tar file and offset) 
  are stored in a sqlite3 **index database**.
* **Database enables faster retrieval** of individual files by locating in which tar
  file a specific file is stored, as well as its location (offset) within the 
  tar file.
* **File integrity** is verified by computing checksums on-the-fly while **extracting** 
  files.

Source code is available on Github: `<https://github.com/E3SM-Project/zstash>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   self
   getting_started
   usage
   best_practices
   design
   database
   support
   todo
   contributing

