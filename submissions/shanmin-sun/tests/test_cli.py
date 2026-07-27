from pipeline.main import build_parser


def test_cli_exposes_all_pipeline_commands_without_python39_actions():
    parser = build_parser()

    for command in ("smoke-test", "collect-prices", "collect-news", "run-all"):
        args = parser.parse_args(["--config", "config.yaml", command])
        assert args.command == command
        assert args.config == "config.yaml"


def test_setup_failure_reports_the_failed_json_contract(capsys):
    """A missing config file must produce the machine-readable stderr line
    and exit code 1, not a traceback."""
    from pipeline.main import main

    code = main(["--config", "does-not-exist.yaml", "smoke-test"])
    assert code == 1
    err = capsys.readouterr().err
    import json as _json

    payload = _json.loads(err.strip().splitlines()[-1])
    assert payload["status"] == "failed"
    assert "does-not-exist" in payload["error"]
