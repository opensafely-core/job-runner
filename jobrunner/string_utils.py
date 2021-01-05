import re
from urllib.parse import urlparse


def slugify(s):
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    # Remove anything that's not alphanumeric, underscore, dash or whitespace
    s = re.sub(r"[^\w\s\-]", "", s)
    # Replace repeated runs of anything non-alphanumeric with a single dash
    return re.sub(r"[_\s\-]+", "-", s).strip("-")


def project_name_from_url(url):
    """
    Return the name of a repository from its URL (there's nothing particularly
    significant about a repository's name but it can make debugging easier to
    include it in various places)
    """
    # "URL" here can in fact be a local path and it may be a Windows path, in
    # which case we need to convert the slash type so that it gets handled
    # correctly
    url = url.replace("\\", "/")
    name = urlparse(url).path.strip("/").split("/")[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name


def tabulate(rows, separator=" ", indent=0, empty=""):
    """
    Formats two columns of data
    """
    if not rows:
        return f"{' ' * indent}{empty}"
    max_col_0 = max(len(row[0]) for row in rows)
    max_col_1 = max(len(row[1]) for row in rows)
    format_str = f"{' ' * indent}{{0:<{max_col_0}}}{separator}{{1:<{max_col_1}}}"
    return "\n".join(format_str.format(*row) for row in rows)
