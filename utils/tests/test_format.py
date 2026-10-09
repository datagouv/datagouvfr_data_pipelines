from datagouvfr_data_pipelines.utils.format import human_size


def test_human_size_bytes():
    assert human_size(0) == "0.0 B"
    assert human_size(512) == "512.0 B"
    assert human_size(1023) == "1023.0 B"


def test_human_size_scales_units():
    assert human_size(1024) == "1.0 KiB"
    assert human_size(1024 * 1024) == "1.0 MiB"
    assert human_size(1024**3) == "1.0 GiB"
    assert human_size(1024**4) == "1.0 TiB"


def test_human_size_rounds_to_one_decimal():
    assert human_size(1536) == "1.5 KiB"
    assert human_size(1024 * 1024 + 524288) == "1.5 MiB"


def test_human_size_does_not_overflow_past_tib():
    # Beyond TiB the result stays in TiB rather than crashing.
    assert human_size(1024**5) == "1024.0 TiB"
