from argparse import ArgumentParser
from runner import run_job
from runner import watch


if __name__ == "__main__":
    parser = ArgumentParser(description="Extract cohort at specific tag")
    subparsers = parser.add_subparsers(help="sub-command help", dest="subparser_name")
    parser_run = subparsers.add_parser("run", help="Run a once-off cohort build")
    parser_watch = subparsers.add_parser(
        "watch", help="Poll a remote server for cohort builds"
    )
    parser_watch.add_argument(
        "queue_endpoint", help="URL of job queue endpoint", type=str,
    )
    parser_run.add_argument(
        "repo", help="Full URL to an opensafely git repo", type=str,
    )
    parser_run.add_argument(
        "tag", help="Tag or branch name to run against", type=str,
    )
    options = parser.parse_args()
    if options.subparser_name == "run":
        run_job({"repo": options.repo, "tag": options.tag})
    elif options.subparser_name == "watch":
        watch(options.queue_endpoint)
    else:
        parser.print_help()
