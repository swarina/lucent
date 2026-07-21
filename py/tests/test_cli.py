from click.testing import CliRunner

from lucent.cli import main


def test_help_lists_core_commands() -> None:
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    for cmd in ("dev", "ingest", "bench", "record", "corpus", "node"):
        assert cmd in result.output


def test_version() -> None:
    result = CliRunner().invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "lucent" in result.output


def test_record_is_implemented() -> None:
    # Every command is now real (record was the last stub, M6-T2). Its help
    # exposes the bundle options rather than a "not implemented" stub.
    result = CliRunner().invoke(main, ["record", "--help"])
    assert result.exit_code == 0
    assert "--out" in result.output and "--config" in result.output
