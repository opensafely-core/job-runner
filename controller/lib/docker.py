from http.cookiejar import split_header_words
from urllib.parse import urlparse

import requests

from common import config as common_config


session = requests.Session()

# cache of token to avoid having to re-auth everytime
# will be refreshed automatically as needed
token = None

# In-memory cache of docker shas. If we need to preserve the cache across
# worker restarts, we'll need to move this to a db table lookup
docker_sha_cache = {}


# Tell ghcr.io exactly what kind of manifest we want, since multi-arch
# manfiests requires us to be explicit.
MANIFEST_ACCEPT = (
    "application/vnd.oci.image.index.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json,"
    "application/vnd.docker.distribution.manifest.v2+json,"
    "application/vnd.oci.image.manifest.v1+json"
)


def get_current_image_sha(image_with_tag):
    """Get the current sha for a tag from a docker registry.

    Falls back to stale result on network error.
    """
    name, _, tag = image_with_tag.partition(":")
    # this adds hostname and more importantly, org name, which we need
    full_image = f"{common_config.DOCKER_REGISTRY}/{name}"

    parsed = urlparse("https://" + full_image)
    try:
        response = dockerhub_api(
            f"/v2/{parsed.path.lstrip('/')}/manifests/{tag}", accept=MANIFEST_ACCEPT
        )
    except requests.exceptions.RequestException as exc:
        # do not block on failure, use stale sha, if available
        if image_with_tag in docker_sha_cache:
            return docker_sha_cache[image_with_tag]
        raise exc

    # Confusingly, there are two shas for a docker image. The
    # Config sha, and Content sha. For our purposes, we want
    # the Content sha, as that can be used with docker run.
    sha = response.headers["Docker-Content-Digest"]
    docker_sha_cache[image_with_tag] = sha
    return sha


def dockerhub_api(path, accept):
    """Generic wrapper for calling the dockerhub api.

    Handles 401 and authentication, which is needed even for public images.
    """
    global token

    url = f"https://ghcr.io/{path.lstrip('/')}"

    # Docker API requires auth token, even for public resources.
    # However, we can reuse a public token.
    if token is None:
        response = session.get(url)
        token = get_auth_token(response.headers["www-authenticate"])

    response = session.get(
        url, headers={"Authorization": f"Bearer {token}", "Accept": accept}
    )

    # refresh token if needed
    if response.status_code == 401:
        token = get_auth_token(response.headers["www-authenticate"])
        response = session.get(
            url, headers={"Authorization": f"Bearer {token}", "Accept": accept}
        )

    response.raise_for_status()
    return response


def get_auth_token(header):
    """Parse a docker v2 www-authentication header and fetch a token.

    The header looks like this

    Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:opensafely-core/busybox:pull"

    And then needs converting to a url like

    https://ghcr.io/token?service=ghcr.io&scope=repository:opensafely-core/busybox:pull

    """
    header = header.lstrip("Bearer")
    # split_header_words is weird, but better than doing it ourselves
    words = split_header_words([header])
    values = dict(next(zip(*words)))
    url = values.pop("realm")
    auth_response = session.get(url, params=values)
    return auth_response.json()["token"]
