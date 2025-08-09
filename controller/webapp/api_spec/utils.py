from pathlib import Path

from ruamel.yaml import YAML


def load_api_spec_json():
    yaml = YAML()
    return yaml.load(Path(__file__).parents[1] / "api_spec" / "openapi.yaml")


api_spec_json = load_api_spec_json()
