from textwrap import dedent

from jobrunner import service

def test_parse_env():
    env = service.parse_env(dedent("""\
        key=value
        spaces_value=val ue
        spaces key = value
           whitespace\t  =  value  
        single='val ue'
        double="val ue"
    """))
    assert env == {
        "key": "value",
        "spaces_value": "val ue",
        "spaces key": "value",
        "whitespace": "value",
        "single": "val ue",
        "double": "val ue",
    }


