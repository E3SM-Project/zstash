from zstash.ls import setup_ls
from zstash.utils import CommandInfo, HPSSType


def test_setup_ls():
    # Test required parameters
    args_str = "zstash ls".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.files == ["*"]
    assert args.hpss is None
    assert args.long is None
    assert args.cache is None
    assert args.tars is False
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
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.files == ["file*.txt"]

    args_str = "zstash extract file1.txt fileA.txt".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.files == ["file1.txt", "fileA.txt"]

    # Test optional parameters
    args_str = "zstash ls --hpss=none".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.hpss == "none"
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "none"
    assert command_info.hpss_type == HPSSType.NO_HPSS

    args_str = "zstash ls --hpss=my_path".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.hpss == "my_path"
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "my_path"
    assert command_info.hpss_type == HPSSType.SAME_MACHINE_HPSS

    args_str = "zstash ls --hpss=globus://my_path".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.hpss == "globus://my_path"
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "globus://my_path"
    assert command_info.hpss_type == HPSSType.GLOBUS

    args_str = "zstash ls -l".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.long is True

    args_str = "zstash ls --cache=my_cache".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.cache == "my_cache"
    assert command_info.cache_dir == "my_cache"

    args_str = "zstash ls --tars".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.tars is True

    args_str = "zstash ls --verbose".split(" ")
    command_info = CommandInfo("ls")
    args = setup_ls(command_info, args_str)
    assert args.verbose is True


def test_process_matches_files():
    # TODO eventually
    pass


def test_process_matches_tars():
    # TODO eventually
    pass
