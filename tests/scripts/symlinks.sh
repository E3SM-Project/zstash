# Test symlinks
# Adjusted from https://github.com/E3SM-Project/zstash/issues/341

follow_symlinks=true

rm -rf workdir workdir2 workdir3
mkdir workdir workdir2 workdir3
cd workdir
mkdir -p src/d1 src/d2
touch src/d1/large_file.txt

# This creates a symlink in d2 that links to a file in d1
# Notice absolute path is used for source
ln -s /home/ac.forsyth2/ez/zstash/tests/scripts/workdir/src/d1/large_file.txt src/d2/large_file.txt

echo ""
echo "ls -l  src/d2"
ls -l  src/d2
# symlink

echo ""
if [[ "${follow_symlinks,,}" == "true" ]]; then
  echo "zstash create --hpss=none --follow-symlinks --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2 src/d2"
  zstash create --hpss=none --follow-symlinks --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2 src/d2
else
  echo "zstash create --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2 src/d2"
  zstash create --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2 src/d2
fi

echo ""
echo "ls -l  src/d2"
ls -l  src/d2
# symlink (src is unaffected)

cd ../workdir3
echo ""
echo "zstash extract --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2"
zstash extract --hpss=none --cache /home/ac.forsyth2/ez/zstash/tests/scripts/workdir2

cd ..
echo ""
echo "ls workdir3"
ls workdir3
# large_file.txt
