from yaetos.env_dispatchers import FSOpsDispatcher
import pytest
from typing import Tuple, List


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("s3://my-bucket", True),
        ("s3a://my-bucket", True),
    ],
)
def test_is_s3_path_valid_path(test_input: str, expected: bool) -> None:
    assert FSOpsDispatcher().is_s3_path(test_input) == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("s_3://", False),
        ("s3//", False),
    ],
)
def test_is_s3_path_invalid_path(test_input: str, expected: bool) -> None:
    result = FSOpsDispatcher().is_s3_path(test_input)
    assert result == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "s3://my-bucket/a/b/e/f/v.txt",
            ("my-bucket", "/a/b/e/f/v.txt", ["a", "b", "e", "f", "v.txt"]),
        ),
        (
            "s3a://my-cool_bucket-32/without_file/",
            ("my-cool_bucket-32", "/without_file/", ["without_file"]),
        ),
    ],
)
def test_split_s3_path_working_paths(
    test_input: str, expected: Tuple[str, str, List[str]]
) -> None:

    result = FSOpsDispatcher().split_s3_path(test_input)
    assert result == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "s3:/my-bucket/a/b/e/f/v.txt",
            (None, None, None),
        ),
        (
            "s://my-cool_bucket-32/without_file/",
            (None, None, None),
        ),
    ],
)
def test_split_s3_path_invalid_paths(
    test_input: str, expected: Tuple[str, str, List[str]]
) -> None:
    with pytest.raises(RuntimeError) as e:
        result = FSOpsDispatcher().split_s3_path(test_input)
