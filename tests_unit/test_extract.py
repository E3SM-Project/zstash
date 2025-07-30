from datetime import datetime

from zstash.extract import parse_tars_option, process_matches, setup_extract
from zstash.utils import CommandInfo, HPSSType


def test_setup_extract():
    # Test required parameters
    args_str = "zstash extract".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.files == ["*"]
    assert args.hpss is None
    assert args.workers == 1
    assert args.keep is False
    assert args.cache is None
    assert args.retries == 1
    assert args.verbose is False
    assert command_info.cache_dir == "zstash"
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.maxsize is None
    assert command_info.config.hpss is None
    assert command_info.hpss_type == HPSSType.UNDEFINED

    args_str = "zstash extract file*.txt".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.files == ["file*.txt"]

    args_str = "zstash extract file1.txt fileA.txt".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.files == ["file1.txt", "fileA.txt"]

    # Test optional parameters
    args_str = "zstash extract --hpss=none".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.hpss == "none"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "none"
    assert command_info.hpss_type == HPSSType.NO_HPSS

    args_str = "zstash extract --hpss=my_path".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.hpss == "my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "my_path"
    assert command_info.hpss_type == HPSSType.SAME_MACHINE_HPSS

    args_str = "zstash extract --hpss=globus://my_path".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.hpss == "globus://my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "globus://my_path"
    assert command_info.hpss_type == HPSSType.GLOBUS

    args_str = "zstash extract --workers=5".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.workers == 5

    args_str = "zstash extract --keep".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.keep is True

    args_str = "zstash extract --cache=my_cache".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.cache == "my_cache"
    assert command_info.cache_dir == "my_cache"

    args_str = "zstash extract --retries=3".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.retries == 3

    args_str = "zstash extract --verbose".split(" ")
    command_info = CommandInfo("extract")
    args = setup_extract(command_info, args_str)
    assert args.verbose is True


def test_process_matches():
    # TupleFilesRow = Tuple[int, str, int, datetime.datetime, Optional[str], str, int]
    matches_ = [
        (2, "file2.txt", 10, datetime.fromordinal(3), "md5_2", "tar0", 20),
        (0, "file0.txt", 10, datetime.fromordinal(1), "md5_0", "tar0", 0),
        (0, "file0.txt", 10, datetime.fromordinal(5), "md5_4", "tar1", 10),
        (1, "file1.txt", 10, datetime.fromordinal(2), "md5_1", "tar0", 10),
        (3, "file3.txt", 10, datetime.fromordinal(4), "md5_3", "tar1", 0),
    ]
    # Sorts on name [1], then on tar [5], then on offset [6]
    # Removes duplicates (removes the earlier entry)
    # Sorts on tar [5], then on offset [6]
    #
    # So, that would order this as:
    # (0, "file0.txt", 10, datetime.fromordinal(1), "md5_0", "tar0", 0),
    # (0, "file0.txt", 10, datetime.fromordinal(5), "md5_4", "tar1", 10),
    # (1, "file1.txt", 10, datetime.fromordinal(2), "md5_1", "tar0", 10),
    # (2, "file2.txt", 10, datetime.fromordinal(3), "md5_2", "tar0", 20),
    # (3, "file3.txt", 10, datetime.fromordinal(4), "md5_3", "tar1", 0),
    # Then, as:
    # (1, "file1.txt", 10, datetime.fromordinal(2), "md5_1", "tar0", 10),
    # (2, "file2.txt", 10, datetime.fromordinal(3), "md5_2", "tar0", 20),
    # (3, "file3.txt", 10, datetime.fromordinal(4), "md5_3", "tar1", 0),
    # (0, "file0.txt", 10, datetime.fromordinal(5), "md5_4", "tar1", 10),
    matches = process_matches(matches_)
    assert len(matches) == 4

    n = 0
    assert matches[n].identifier == 1
    assert matches[n].name == "file1.txt"
    assert matches[n].size == 10
    assert matches[n].mtime == datetime.fromordinal(2)
    assert matches[n].md5 == "md5_1"
    assert matches[n].tar == "tar0"
    assert matches[n].offset == 10

    n = 1
    assert matches[n].identifier == 2
    assert matches[n].name == "file2.txt"
    assert matches[n].size == 10
    assert matches[n].mtime == datetime.fromordinal(3)
    assert matches[n].md5 == "md5_2"
    assert matches[n].tar == "tar0"
    assert matches[n].offset == 20

    n = 2
    assert matches[n].identifier == 3
    assert matches[n].name == "file3.txt"
    assert matches[n].size == 10
    assert matches[n].mtime == datetime.fromordinal(4)
    assert matches[n].md5 == "md5_3"
    assert matches[n].tar == "tar1"
    assert matches[n].offset == 0

    n = 3
    assert matches[n].identifier == 0
    assert matches[n].name == "file0.txt"
    assert matches[n].size == 10
    assert matches[n].mtime == datetime.fromordinal(5)
    assert matches[n].md5 == "md5_4"
    assert matches[n].tar == "tar1"
    assert matches[n].offset == 10


def test_prepare_multiprocess():
    # TODO eventually -- this is a complicated function to test.
    pass


def test_parse_tars_option():
    # Starting at 00005a until the end
    tar_list = parse_tars_option("00005a-", "000000", "00005d")
    assert tar_list == ["00005a", "00005b", "00005c", "00005d"]

    # Starting from the beginning to 00005a (included)
    tar_list = parse_tars_option("-00005a", "000058", "00005d")
    assert tar_list == ["000058", "000059", "00005a"]

    # Specific range
    tar_list = parse_tars_option("00005a-00005c", "00000", "000005d")
    assert tar_list == ["00005a", "00005b", "00005c"]

    # Selected tar files
    tar_list = parse_tars_option("00003e,00004e,000059", "000000", "00005d")
    assert tar_list == ["00003e", "00004e", "000059"]

    # Mix and match
    tar_list = parse_tars_option("000030-00003e,00004e,00005a-", "000000", "00005d")
    assert tar_list == [
        "000030",
        "000031",
        "000032",
        "000033",
        "000034",
        "000035",
        "000036",
        "000037",
        "000038",
        "000039",
        "00003a",
        "00003b",
        "00003c",
        "00003d",
        "00003e",
        "00004e",
        "00005a",
        "00005b",
        "00005c",
        "00005d",
    ]

    # Check removal of duplicates and sorting
    tar_list = parse_tars_option("000009,000003,-000005", "000000", "00005d")
    assert tar_list == [
        "000000",
        "000001",
        "000002",
        "000003",
        "000004",
        "000005",
        "000009",
    ]

    # Remove .tar suffix
    tar_list = parse_tars_option(
        "000009.tar-00000a,000003.tar,-000002.tar", "000000", "00005d"
    )
    assert tar_list == ["000000", "000001", "000002", "000003", "000009", "00000a"]
