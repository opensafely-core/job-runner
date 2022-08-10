def is_extraction_command(args, require_version=None):
    """
    The `cohortextractor generate_cohort` command gets special treatment in
    various places (e.g. it's the only command which gets access to the
    database) so it's helpful to have a single function for identifying it
    """
    version_found = None
    if len(args) > 1 and args[1] in ("generate_cohort", "generate_dataset"):
        if args[0].startswith("cohortextractor:"):
            version_found = 1
        # databuilder is a rebranded cohortextractor-v2.
        elif args[0].startswith("databuilder:"):
            version_found = 2

    # If we're not looking for a specific version then return True if any
    # version found
    if require_version is None:
        return version_found is not None
    # Otherwise return True only if specified version found
    else:
        return version_found == require_version
