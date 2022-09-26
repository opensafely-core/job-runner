def requires_db_access(args):
    """
    By default actions do not have database access, but certain trusted actions require it
    """
    valid_commands = {
        "cohortextractor": ("generate_cohort", "generate_codelist_report"),
        "databuilder": ("generate-dataset",),
        "sqlrunner": None,  # all commands are valid
    }
    if len(args) <= 1:
        return False

    image, command = args[0], args[1]
    image = image.split(":")[0]
    if image in valid_commands:
        if valid_commands[image] is None or command in valid_commands[image]:
            return True
    return False
