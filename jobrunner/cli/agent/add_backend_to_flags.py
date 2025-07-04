from controller.models import Flag
from jobrunner.config import agent as config
from jobrunner.lib.database import find_where, update


def main():
    """
    Command to add missing backend attribute to flags. We expect this to only
    run once per backend, prior to moving the controller out of the backend
    """
    flags_missing_backend = find_where(Flag, backend=None)
    if flags_missing_backend:
        print(
            "This command will add a backend attribute to all flags and should only be run from inside a backend. Please confirm you want to continue:"
        )
        confirm = input("\nY to continue, N to quit\n")
        if confirm.lower() != "y":
            return
    else:
        print("All flags have a backend assigned; nothing to do")

    for flag in flags_missing_backend:
        flag.backend = config.BACKEND
        update(flag)

    print(f"{len(flags_missing_backend)} flags updated")


if __name__ == "__main__":
    main()
