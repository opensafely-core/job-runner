import pytest
from django.conf import settings

from controller_app import settings as settings_funcs


def test_secret_key():
    assert len(settings.SECRET_KEY) > 0


def test_get_env_var():
    with pytest.raises(
        RuntimeError, match="Missing environment variable: AINT_NO_SUCH_VAR"
    ):
        settings_funcs.get_env_var("AINT_NO_SUCH_VAR")
