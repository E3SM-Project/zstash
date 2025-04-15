from zstash.update import setup_update
from zstash.utils import CommandInfo, HPSSType


def test_setup_update():
    # Test required parameters
    args_str = "zstash update".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.hpss == "none"
    assert args.include is None
    assert args.exclude is None
    assert args.dry_run is False
    assert args.maxsize == 256
    assert args.keep is True
    assert args.cache is None
    assert args.non_blocking is False
    assert args.verbose is False
    assert args.follow_symlinks is False
    assert command_info.cache_dir == "zstash"
    assert command_info.keep is True
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.maxsize == 274877906944
    assert command_info.config.hpss == "none"
    assert command_info.hpss_type == HPSSType.NO_HPSS

    # Test --hpss, without --keep
    args_str = "zstash update --hpss=none".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.hpss == "none"
    assert args.keep is True
    assert command_info.keep is True
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "none"
    assert command_info.hpss_type == HPSSType.NO_HPSS

    args_str = "zstash update --hpss=my_path".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.hpss == "my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "my_path"
    assert command_info.hpss_type == HPSSType.SAME_MACHINE_HPSS

    args_str = "zstash update --hpss=globus://my_path".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.hpss == "globus://my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.path.endswith(
        "zstash"
    )  # If running from top level of git repo
    assert command_info.dir_to_archive_relative == command_info.config.path
    assert command_info.config.hpss == "globus://my_path"
    assert command_info.hpss_type == HPSSType.GLOBUS

    # Test --hpss, with --keep
    args_str = "zstash update --hpss=none --keep".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    args_str = "zstash update --hpss=my_path --keep".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    args_str = "zstash update --hpss=globus://my_path".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.keep is False
    assert command_info.keep is False

    # Test other optional parameters
    args_str = "zstash update --include=file1.txt,file2.txt,file3.txt".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.include == "file1.txt,file2.txt,file3.txt"
    assert args.exclude is None

    args_str = "zstash update --exclude=file1.txt,file2.txt,file3.txt".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.include is None
    assert args.exclude == "file1.txt,file2.txt,file3.txt"

    args_str = "zstash update --include=file1.txt,file2.txt --exclude=file3.txt,file4.txt".split(
        " "
    )
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.include == "file1.txt,file2.txt"
    assert args.exclude == "file3.txt,file4.txt"

    args_str = "zstash update --dry-run".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.dry_run is True

    args_str = "zstash update --maxsize=1024".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.maxsize == 1024
    assert command_info.config.maxsize == 1024**4

    args_str = "zstash update --keep".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    args_str = "zstash update --cache=my_cache".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.cache == "my_cache"
    assert command_info.cache_dir == "my_cache"

    args_str = "zstash update --non-blocking".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.non_blocking is True

    args_str = "zstash update --verbose".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.verbose is True

    args_str = "zstash update --follow-symlinks".split(" ")
    command_info = CommandInfo("update")
    args = setup_update(command_info, args_str)
    assert args.follow_symlinks is True
