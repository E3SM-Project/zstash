from zstash.chgrp import setup_chgrp


def test_setup_chgrp():
    args_str = "zstash chgrp 775 my_path".split(" ")
    args = setup_chgrp(args_str)
    assert args.group == "775"
    assert args.hpss == "my_path"
    assert args.R is None
    assert args.verbose is False

    args_str = "zstash chgrp 775 my_path -R".split(" ")
    args = setup_chgrp(args_str)
    assert args.group == "775"
    assert args.hpss == "my_path"
    assert args.R is True
    assert args.verbose is False

    args_str = "zstash chgrp 775 my_path -v".split(" ")
    args = setup_chgrp(args_str)
    assert args.group == "775"
    assert args.hpss == "my_path"
    assert args.R is None
    assert args.verbose is True

    args_str = "zstash chgrp 775 my_path -R -v".split(" ")
    args = setup_chgrp(args_str)
    assert args.group == "775"
    assert args.hpss == "my_path"
    assert args.R is True
    assert args.verbose is True
