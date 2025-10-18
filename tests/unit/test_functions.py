from zstash.extract import parse_tars_option


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
