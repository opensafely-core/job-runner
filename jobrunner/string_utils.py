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
    name = urlparse(url).path.strip("/").split("/")[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name
