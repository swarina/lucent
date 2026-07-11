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


def test_unimplemented_command_exits_nonzero() -> None:
    result = CliRunner().invoke(main, ["dev"])
    assert result.exit_code == 2
