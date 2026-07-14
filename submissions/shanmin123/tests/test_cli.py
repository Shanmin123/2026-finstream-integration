from pipeline.main import build_parser


def test_cli_exposes_all_pipeline_commands_without_python39_actions():
    parser = build_parser()

    for command in ("smoke-test", "collect-prices", "collect-news", "run-all"):
        args = parser.parse_args(["--config", "config.yaml", command])
        assert args.command == command
        assert args.config == "config.yaml"
