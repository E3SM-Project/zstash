*********************
Design considerations
*********************

How does zstash work?
=====================

...to do...

Why not use 'htar'?
===================

Htar is an HPSS utility with many nice features. From the NERSC `htar documentation page 
<http://www.nersc.gov/users/storage-and-file-systems/hpss/storing-and-retrieving-data/clients/htar-usage/>`_:

  HTAR is a command line utility that creates and manipulates HPSS-resident 
  tar-format archive files. It is ideal for storing groups of files in HPSS. 
  Since the tar file is created directly in HPSS, it is generally faster and 
  uses less local space than creating a local tar file then storing that into 
  HPSS. Furthermore, HTAR creates an index file that (by default) is stored 
  along with the archive in HPSS. This allows you to list the contents of 
  an archive without retrieving it to local storage first. The index file is 
  only created if the HTAR bundle is successfully stored in the archive.

This sounds great. In fact, some of these htar functionalities served as inspiration for zstash.
However, htar also suffers from a number of limitations that were deemed to be show stoppers:

  **Member File Path Length** *

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
large number of (relatively small) files as mentioned on the 
`NERSC Mistakes to avoid <http://www.nersc.gov/users/storage-and-file-systems/hpss/storing-and-retrieving-data/mistakes-to-avoid>`_ page:

  *Large tape storage systems do not work well with small files. Retrieval 
  of large numbers of small files from HPSS will incur significant delays 
  due to the characteristics of tape media:*

  * *File retrieval from tape media involves loading tapes into tape drives 
    and positioning the media. This operation can be quite time consuming.*
  * *Storing large numbers of small files may spread them across dozens or 
    hundreds of tapes.*
  * *Mounting dozens of tapes and then seeking to particular locations on 
    tape can take a long time and impair usability for others.*

