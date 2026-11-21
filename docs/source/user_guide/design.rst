*********************
Design considerations
*********************

Why zstash?
===========

NERSC HPSS and other similar tape storage systems are optimized
for file sizes of 100 to 500 GB. The retrieval of a large number
of small files that are spread across dozens or hundreds of tapes
is very inefficient because it requires multiple loads into tape 
drives and positioning the tape.

The recommended practice is thus to bundle smaller files into larger
ones. zstash was created specifically for this purpose. While
the existing 'htar' utility offers a similar functionality, it also
suffers from limitations that make it not suitable for archiving
E3SM model output (see below).

**Features of zstash:**

* Files are bundled **into standard tar files** of a specified maximum size.
  The default maximum size is 256 GB, but it can be adjusted at
  creation using the ``--maxsize`` command line argument.
* **Individual files are never split** between two separate tar
  files. As a result, the size of tar files varies, but will
  generally be smaller than the specified maximum size. The only 
  exception would be if individual files are larger than the target size.
* **Tar files are named sequentially** using a six
  digit hexadecimal number (`000000.tar`,  `000001.tar`, ...).
* **Tar files are first created locally** on disk (under the 
  `zstash/` subdirectory) and **then uploaded to HPSS**. Similarly, when
  extracting files, tar files will be first downloaded from HPSS into
  the local cache (if they are not already present).
* An sqlite3 **index database** is maintained (`zstash/index.db`) that contains
  information about every archived files, including:

  * full pathname relative to top-level archival directory.
  * size
  * modification time
  * md5 checksum
  * tar file in which the file is included
  * offset within this tar file (for faster extraction).

* **Database enables faster retrieval** of individual files by locating in which tar
  file a specific file is stored, as well as its location (offset) within the 
  tar file.
* **Checksums (md5)** of input files are computed *on-the-fly* during
  archiving. For large files, this saves a considerable amount of
  time compared to separate checksumming and archiving steps.
* **File integrity** is verified by computing checksums on-the-fly while **extracting** 
  files.


Why not use 'htar'?
===================

Htar is an HPSS utility with many nice features. From the NERSC `htar documentation page 
<http://docs.nersc.gov/filesystems/archive_access/#htar>`_:

  *HTAR is a command line utility that creates and manipulates HPSS-resident 
  tar-format archive files. It is ideal for storing groups of files in HPSS. 
  Since the tar file is created directly in HPSS, it is generally faster and 
  uses less local space than creating a local tar file then storing that into 
  HPSS. Furthermore, HTAR creates an index file that (by default) is stored 
  along with the archive in HPSS. This allows you to list the contents of 
  an archive without retrieving it to local storage first. The index file is 
  only created if the HTAR bundle is successfully stored in the archive.*

This sounds great. In fact, some of these htar functionalities served as inspiration for zstash.
However, htar also suffers from a number of limitations that were deemed to be **show stoppers**:

  **Member File Path Length**

  File path names within an HTAR aggregate of the form prefix/name are limited to 
  **154 characters** for the prefix and 99 characters for the file name. Link names 
  cannot exceed 99 characters.

  **Member File Size**

  The maximum file size the NERSC archive will support is approximately 20 TB. 
  However, we recommend you aim for HTAR aggregate sizes of several hundred GBs. 
  Member files within an HTAR aggregate are limited to approximately **68GB**.


Both the member file path length and file size were simply too restrictive
for archiving output from E3SM simulations. Furthermore, htar lacks the ability
to compute checksums of member files.


Why not just use 'cput -R'?
===========================

This is the brute force approach: recursive conditional put. It will recursively
transfer all your files to tape and only do so if the file isn't already on tape.

While this approach *may* work, it is not recommended when dealing with very
large number of (relatively small) files. Instead, NERSC recommends to
`group small files together <https://docs.nersc.gov/filesystems/archive/#group-small-files-together>`_ :

   *HPSS is a tape system and responds differently than a typical file system. 
   If you upload large numbers of small files they will be spread across 
   dozens or hundreds of tapes, requiring multiple loads into tape drives
   and positioning the tape. Storing many small files in HPSS without 
   bundling them together will result in extremely long retrieval times 
   for these files and will slow down the HPSS system for all users.*

