def requires_db_access(args):
    """
    By default actions do not have database access, but certain trusted actions require it
    """
    valid_commands = {
        "cohortextractor": ("generate_cohort", "generate_codelist_report"),
        "databuilder": ("generate_dataset",),
    }
    if len(args) <= 1:
        return False

    image, command = args[0], args[1]
    image = image.split(":")[0]
    return command in valid_commands.get(image, [])
