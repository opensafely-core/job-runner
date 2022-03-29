import inspect


def print_config(config):
    # ghetto print config module contents
    vars_ish = {
        k: v
        for k, v in config.__dict__.items()
        if not k.startswith("_") and not inspect.ismodule(v)
    }
    for k, v in sorted(vars_ish.items()):
        print(f"{k}: {v}")
