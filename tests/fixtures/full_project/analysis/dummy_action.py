import sys


def main(output_file):
    """
    Write a dummy file to disk

    Databuilder doesn't currently provide many features for studies, so the
    options we have for actions in this full_project fixture are limited.  This
    "action" does the bare minimum just so we can have another action in the
    project.yaml.  Once databuilder supports more column types in the dummy
    data then we can remove this in favour of more meaningful actions such as
    those used in the cohortextractor version of the integration test.
    """
    with open(output_file, "w") as f:
        f.write("test file\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
