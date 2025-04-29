import pytest

from jobrunner.cli import add_backend_to_flags
from jobrunner.lib import database
from jobrunner.models import Flag


def test_add_backend_to_flag(db, monkeypatch):
    monkeypatch.setattr("jobrunner.config.agent.BACKEND", "dummy_backend")
    monkeypatch.setattr("builtins.input", lambda _: "y")

    flag1 = Flag(id="foo", value="bar")
    database.insert(flag1)
    flag2 = Flag(id="foo1", value="bar1", backend="the_test_backend")
    database.insert(flag2)

    add_backend_to_flags.main()
    assert database.find_one(Flag, id=flag1.id).backend == "dummy_backend"
    assert database.find_one(Flag, id=flag2.id).backend == "the_test_backend"


@pytest.mark.parametrize(
    "response,expected_backend",
    [
        ("Y", "dummy_backend"),
        ("y", "dummy_backend"),
        ("N", None),
        ("foo", None),
    ],
)
def test_add_backend_to_flag_confirmation(db, monkeypatch, response, expected_backend):
    monkeypatch.setattr("jobrunner.config.agent.BACKEND", "dummy_backend")
    flag1 = Flag(id="foo", value="bar", backend="the_test_backend")
    database.insert(flag1)
    flag2 = Flag(id="foo1", value="bar1")
    database.insert(flag2)

    monkeypatch.setattr("builtins.input", lambda _: response)
    add_backend_to_flags.main()
    assert database.find_one(Flag, id=flag1.id).backend == "the_test_backend"
    assert database.find_one(Flag, id=flag2.id).backend == expected_backend


def test_add_backend_to_flag_nothing_to_do(db, monkeypatch, capsys):
    monkeypatch.setattr("jobrunner.config.agent.BACKEND", "dummy_backend")
    # flag with a backend already set
    flag1 = Flag(id="foo", value="bar", backend="the_test_backend")
    database.insert(flag1)

    add_backend_to_flags.main()
    assert database.find_one(Flag, id=flag1.id).backend == "the_test_backend"
    captured = capsys.readouterr()
    assert "nothing to do" in captured.out
