"""Small, fail-closed reader for environment and mounted deployment secrets."""
import os
from pathlib import Path

MAX_SECRET_FILE_BYTES = 65536

def secret_value(name, default=None, required=False, environ=None):
    """Read NAME or NAME_FILE without logging secret values or file paths."""
    env = os.environ if environ is None else environ
    direct = env.get(name)
    filename = env.get(name + "_FILE")
    if direct not in (None, "") and filename:
        raise RuntimeError(f"Set either {name} or {name}_FILE, not both")
    value = direct
    if filename:
        path = Path(filename)
        try:
            if not path.is_file():
                raise RuntimeError(f"{name}_FILE must reference a readable regular file")
            if path.stat().st_size > MAX_SECRET_FILE_BYTES:
                raise RuntimeError(f"{name}_FILE exceeds the allowed size")
            raw = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(f"{name}_FILE could not be read") from exc
        try:
            value = raw.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"{name}_FILE must contain UTF-8 text") from exc
    if value is None or value == "":
        value = default
    if isinstance(value, str) and "\x00" in value:
        raise RuntimeError(f"{name} contains a NUL byte")
    if required and (value is None or not str(value).strip()):
        raise RuntimeError(f"{name} or {name}_FILE must be configured")
    return value
