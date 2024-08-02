# Test symlinks
# Adjusted from https://github.com/E3SM-Project/zstash/issues/341

mkdir workdir workdir2 workdir3
cd workdir
mkdir -p src/d1 src/d2
touch src/d1/large_file.txt

# This creates a symlink in d2 that links to a file in d1
# Notice absolute path is used for source
ln -s /home/ac.forsyth2/ez/zstash/tests/scripts/workdir/src/d1/large_file.txt src/d2/large_file.txt
ls -l  src/d2
# lrwxrwxrwx 1 ac.forsyth2 cels 71 Aug  2 16:50 large_file.txt -> /home/ac.forsyth2/ez/zstash/tests/scripts/workdir/src/d1/large_file.txt

zstash create --hpss=none --follow-symlinks --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2 src/d2
# For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
# INFO: Gathering list of files to archive
# INFO: Creating new tar archive 000000.tar
# INFO: Archiving large_file.txt
# INFO: tar name=000000.tar, tar size=10240, tar md5=b51fab8640dc26029eb01b46c2bcf04f
# INFO: put: HPSS is unavailable
# INFO: put: Keeping tar files locally and removing write permissions
# INFO: '/home/ac.forsyth2/ez/zstash/tests/scripts/workdir2/000000.tar' original mode=b"'644'"
# INFO: '/home/ac.forsyth2/ez/zstash/tests/scripts/workdir2/000000.tar' new mode=b"'444'"
# INFO: put: HPSS is unavailable
# total 1

ls -l  src/d2
# lrwxrwxrwx 1 ac.forsyth2 cels 71 Aug  2 16:50 large_file.txt -> /home/ac.forsyth2/ez/zstash/tests/scripts/workdir/src/d1/large_file.txt
# Notice src is unaffected, still has a symlink

cd ../workdir3
zstash extract --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2
# For help, please see https://e3sm-project.github.io/zstash. Ask questions at https://github.com/E3SM-Project/zstash/discussions/categories/q-a.
# INFO: /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2/000000.tar exists. Checking expected size matches actual size.
# INFO: Opening tar archive /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2/000000.tar
# INFO: Extracting large_file.txt
# INFO: No failures detected when extracting the files. If you have a log file, run "grep -i Exception <log-file>" to double check.

ls workdir3
# large_file.txt
