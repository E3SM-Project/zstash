.. zstash documentation master file, created by
   sphinx-quickstart on Fri Jul 28 17:31:00 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

********************
Zstash documentation
********************

What is zstash?
===============

Zstash is a prototype **HPSS long-term archiving** solution for ACME.

Zstash is written entirely in Python (2.7) using standard libraries.
Its design is intentionally minimalistic to provide an effective
long-term HPSS archiving solution without creating an overly complicated
(and hard to maintain) tool.

**Key features:**

* Files are archived into standard **tar files** with a user **specified maximum size**.
* Tar archives are first created locally, then transferred to HPSS.
* **Checksums (md5)** of individual input files are computed *on-the-fly* while the
  file is being archived. For large files, this saves a considerable amount of
  time compared to separate checksumming and archiving steps.
* Checksums and additional metadata (size, modification time, tar archive and offset) 
  are stored in a sqlite3 **database**.
* The database enables faster retrieval of individual files by locating in which tar
  archive a specific file is stored, as well as its location (offset) within the 
  tar archive.
* **File integrity** is verified by computing checksums on-the-fly while **extracting** 
  files.

To date, zstash has only been tested on NERSC, but should easily be portable
to other computing centers. ACME users are welcome to try it 
out, while keeping in mind that zstash is currently under development. As such,
its design and functionality may change, or it may be replaced with a better 
tool, should one emerge.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   self
   quickstart
   design
   contributing

