import pytest
from pathlib import Path
from src.pipeline import load_pipeline, load_all_pipelines


FIXTURES = Path(__file__).parent / "fixtures"


def test_load_valid_pipeline(tmp_path):
    yaml_content = """
name: test_pipe
description: "A test pipeline"
enabled: true
source:
  type: postgres
  query: "SELECT 1"
destination:
  type: slack
  params:
    channel: "#test"
schedule:
  type: cron
  cron: "0 9 * * *"
"""
    p = tmp_path / "test_pipe.yaml"
    p.write_text(yaml_content)
    config = load_pipeline(p)
    assert config.name == "test_pipe"
    assert config.source.type == "postgres"
    assert config.destination.type == "slack"
    assert config.schedule.cron == "0 9 * * *"


def test_load_all_pipelines_skips_invalid(tmp_path):
    good = tmp_path / "good.yaml"
    good.write_text("""
name: good
source:
  type: postgres
  query: "SELECT 1"
destination:
  type: slack
  params:
    channel: "#x"
""")
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: yaml: pipeline: content: at all: :")

    pipelines = load_all_pipelines(tmp_path)
    # bad.yaml raises ValidationError, load_all_pipelines catches and continues
    names = [p.name for p in pipelines]
    assert "good" in names


def test_pipeline_disabled_flag(tmp_path):
    p = tmp_path / "disabled.yaml"
    p.write_text("""
name: disabled_pipe
enabled: false
source:
  type: postgres
  query: "SELECT 1"
destination:
  type: email
  params:
    to: admin@test.com
""")
    config = load_pipeline(p)
    assert config.enabled is False
