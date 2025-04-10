from zstash.utils import exclude_files, include_files

# NOTE: CommadInfo methods are tested implicitly via other tests in this directory.


# Test filter_files via exclude_files & include_files
def test_filter_files():
    file_str = "file1.txt,file2.txt,file3.txt"
    file_list = [f"file{i}.txt" for i in range(5)]
    assert exclude_files(file_str, file_list) == ["file0.txt", "file4.txt"]
    assert include_files(file_str, file_list) == ["file1.txt", "file2.txt", "file3.txt"]

    file_str = "path1/*"
    file_list = ["path1/f1.txt", "path1/f2.txt", "path2/f1.txt", "path2/f2.txt"]
    assert exclude_files(file_str, file_list) == ["path2/f1.txt", "path2/f2.txt"]
    assert include_files(file_str, file_list) == ["path1/f1.txt", "path1/f2.txt"]
