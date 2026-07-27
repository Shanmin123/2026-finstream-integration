import json
import re
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version


EXPECTED_PACKAGES = {
    "apache-airflow": "2.8.1",
    "pyspark": "3.5.1",
    "pandas": "1.3.5",
    "numpy": "1.22.4",
    "scikit-learn": "0.24.2",
    "SQLAlchemy": "1.4.46",
    "pydantic": "2.4.2",
    "tensorflow": "2.10.1",
    "pyarrow": "12.0.1",
    "ibapi": "9.81.1.post1",
    "PyYAML": "6.0.1",
}


def validate_environment(version_fn, python_version, java_version_output):
    errors = []
    if tuple(python_version[:2]) != (3, 8):
        errors.append(
            "Python 3.8 required; found {0}.{1}".format(
                python_version[0], python_version[1]
            )
        )

    java_match = re.search(r'version\s+"(\d+)', java_version_output)
    if java_match is None or java_match.group(1) != "17":
        found = java_match.group(1) if java_match is not None else "unknown"
        errors.append("Java 17 required; found {0}".format(found))

    for package_name, expected in EXPECTED_PACKAGES.items():
        try:
            actual = version_fn(package_name)
        except PackageNotFoundError:
            errors.append(
                "{0}=={1} required; package is missing".format(package_name, expected)
            )
            continue
        if actual != expected:
            errors.append(
                "{0}=={1} required; found {2}".format(package_name, expected, actual)
            )
    return errors


def read_java_version():
    try:
        completed = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        # No java on PATH is a finding for the report, not a crash before it.
        return ""
    return completed.stdout


def main():
    java_output = read_java_version()
    errors = validate_environment(version, sys.version_info, java_output)
    payload = {
        "python": sys.version.split()[0],
        "java": java_output.splitlines()[0] if java_output else "unavailable",
        "packages": {},
        "errors": errors,
    }
    for package_name in EXPECTED_PACKAGES:
        try:
            payload["packages"][package_name] = version(package_name)
        except PackageNotFoundError:
            payload["packages"][package_name] = None
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
