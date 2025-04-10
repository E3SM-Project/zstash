from zstash.create import setup_create
from zstash.utils import CommandInfo, HPSSType


def test_setup_create():
    # Test required parameters
    args_str = "zstash create dir_to_archive --hpss=none".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.path == "dir_to_archive"
    assert args.hpss == "none"
    assert args.include is None
    assert args.exclude is None
    assert args.maxsize == 256
    assert args.keep is True
    assert args.cache is None
    assert args.non_blocking is False
    assert args.verbose is False
    assert args.no_tars_md5 is False
    assert args.follow_symlinks is False
    assert command_info.cache_dir == "zstash"
    assert command_info.keep is True
    assert command_info.config.path.endswith("dir_to_archive")
    assert command_info.dir_to_archive_relative == "dir_to_archive"
    assert command_info.config.maxsize == 274877906944
    assert command_info.config.hpss == "none"
    assert command_info.hpss_type == HPSSType.NO_HPSS

    args_str = "zstash create dir_to_archive --hpss=my_path".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.hpss == "my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.hpss == "my_path"
    assert command_info.hpss_type == HPSSType.SAME_MACHINE_HPSS

    args_str = "zstash create dir_to_archive --hpss=globus://my_path".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.hpss == "globus://my_path"
    assert args.keep is False
    assert command_info.keep is False
    assert command_info.config.hpss == "globus://my_path"
    assert command_info.hpss_type == HPSSType.GLOBUS
    assert command_info.globus_info.hpss_path == "globus://my_path"

    # Test required parameters, with --keep
    args_str = "zstash create dir_to_archive --hpss=none --keep".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    args_str = "zstash create dir_to_archive --hpss=my_path --keep".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    args_str = "zstash create dir_to_archive --hpss=globus://my_path --keep".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.keep is True
    assert command_info.keep is True

    # Test optional parameters
    args_str = "zstash create dir_to_archive --hpss=none --include=file1.txt,file2.txt,file3.txt".split(
        " "
    )
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.include == "file1.txt,file2.txt,file3.txt"
    assert args.exclude is None

    args_str = "zstash create dir_to_archive --hpss=none --exclude=file1.txt,file2.txt,file3.txt".split(
        " "
    )
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.include is None
    assert args.exclude == "file1.txt,file2.txt,file3.txt"

    args_str = "zstash create dir_to_archive --hpss=none --include=file1.txt,file2.txt --exclude=file3.txt,file4.txt".split(
        " "
    )
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.include == "file1.txt,file2.txt"
    assert args.exclude == "file3.txt,file4.txt"

    args_str = "zstash create dir_to_archive --hpss=none --maxsize=1024".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.maxsize == 1024
    assert command_info.config.maxsize == 1024**4

    args_str = "zstash create dir_to_archive --hpss=none --cache=my_cache".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.cache == "my_cache"
    assert command_info.cache_dir == "my_cache"

    args_str = "zstash create dir_to_archive --hpss=none --non-blocking".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.non_blocking is True

    args_str = "zstash create dir_to_archive --hpss=none --verbose".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.verbose is True

    args_str = "zstash create dir_to_archive --hpss=none --no_tars_md5".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.no_tars_md5 is True

    args_str = "zstash create dir_to_archive --hpss=none --follow-symlinks".split(" ")
    command_info = CommandInfo("create")
    args = setup_create(command_info, args_str)
    assert args.follow_symlinks is True
