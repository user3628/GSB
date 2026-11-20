import pytest

from logindex.errors import LineInvalidError
from logindex.parser import parse_line, parse_lines


def test_parse_valid_line():
    record = parse_line("1700000000123|auth-svc|INFO|hello|pipe|world", 1)
    assert record.timestamp_ms == 1700000000123
    assert record.service == "auth-svc"
    assert record.level == "INFO"
    assert record.message == "hello|pipe|world"
    assert record.line_no == 1


@pytest.mark.parametrize(
    "line",
    [
        "",
        "   ",
        "170000000012|auth|INFO|short ts",
        "1700000000123|auth|INFO",
        "1700000000123|bad service|INFO|space in service",
        "1700000000123|auth|TRACE|bad level",
        "1700000000123|auth|INFO| ",
        "abcdefghijklm|auth|INFO|non numeric ts",
    ],
)
def test_parse_invalid_lines(line):
    with pytest.raises(LineInvalidError) as exc:
        parse_line(line, 7)
    assert exc.value.code == "E_LINE_INVALID"
    assert exc.value.detail["line_no"] == 7


def test_parse_lines_is_all_or_nothing():
    lines = ["1700000000000|svc|INFO|ok", "garbage", "1700000001000|svc|ERROR|x"]
    with pytest.raises(LineInvalidError):
        parse_lines(lines)


def test_all_sample_files_parse_except_invalid(sample_lines):
    parsed = parse_lines(sample_lines("batch1_full.txt"))
    assert len(parsed) == 12
    assert {r.service for r in parsed} == {
        "auth-svc", "billing-svc", "gateway", "inventory-svc", "notify-svc"
    }
    assert {r.level for r in parsed} == {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
    with pytest.raises(LineInvalidError):
        parse_lines(sample_lines("batch_invalid.txt"))
