def ensure_unix_path(path):
    "Ensure path is unix path string"
    # if we're string, handle it as a string
    if isinstance(path, str):
        return path.replace("\\", "/")
    return path.as_posix()
