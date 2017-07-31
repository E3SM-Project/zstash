***********************************
To do: possible future enhancements
***********************************

Possible future enhancements depending on project priorities.

Improvements
============

* Test and modify for **other computing centers** available to ACME.

* Implement option to **exclude** certain files from archiving, either by
  specifying a list of files (patterns) directly on the command line or in
  an external file:

  * ``--exclude "build/*/*.o","build/*/*.mod"``
  * or ``--exclude-file <file>``


* Implement **verbosity** level option.

* ...

New functionality
=================

* **zstash list**

  * Support wildcards.
  * List from all or selected tar files.

* **zstash update**

  * Similar to 'zstash create', but add new or modified
    files to an existing archive. 
  * Modify 'zstash extract' functionality to deal with
    multiple versions of the same file, by either:

    * only extracting the most recent one,
    * extracting all of them (naming older older ones file.txt, file.txt.1, file.txt.2, ...).

* **zsstash verify** 

  * Similar to 'zstash extract'. Read and compute
    checksums of all input files, without actually extracting them.

* ...

