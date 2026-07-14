from scripts.verify_environment import EXPECTED_PACKAGES, validate_environment


def test_environment_validator_accepts_exact_finstream_contract():
    errors = validate_environment(
        version_fn=lambda name: EXPECTED_PACKAGES[name],
        python_version=(3, 8, 20),
        java_version_output='openjdk version "17.0.18"',
    )

    assert errors == []


def test_environment_validator_reports_every_version_mismatch():
    errors = validate_environment(
        version_fn=lambda _name: "99.0.0",
        python_version=(3, 11, 0),
        java_version_output='java version "25.0.1"',
    )

    assert any("Python 3.8 required" in error for error in errors)
    assert any("Java 17 required" in error for error in errors)
    assert len(errors) == len(EXPECTED_PACKAGES) + 2
