"""CLI tailer entry point."""

from __future__ import annotations

from logiq.cli import main


def test_tail_command_ingests_file(tmp_path, capsys):
    log = tmp_path / "app.log"
    log.write_text("2026-01-01T00:00:00+00:00 INFO hello world\n")
    rc = main([
        "tail",
        str(log),
        "--source", "app",
        "--db", str(tmp_path / "logiq.db"),
        "--wal", str(tmp_path / "logiq.wal"),
    ])
    assert rc == 0
    assert "ingested 1 records" in capsys.readouterr().out
