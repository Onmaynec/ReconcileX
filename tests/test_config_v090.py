import pytest

from reconcilex_config import secret_value
from app.db import resolve_database_url
from app import storage
from ops import offsite
from ops.init_storage import ensure_bucket
from botocore.exceptions import ClientError


def test_secret_file_reads_utf8_and_removes_only_terminal_newlines(tmp_path):
    path = tmp_path / "jwt"
    path.write_bytes(b"  keep-surrounding-space  \r\n")
    assert secret_value("JWT_SECRET", environ={"JWT_SECRET_FILE": str(path)}) == "  keep-surrounding-space  "


def test_secret_reader_rejects_ambiguous_sources(tmp_path):
    path = tmp_path / "secret"
    path.write_text("file-value", encoding="utf-8")
    with pytest.raises(RuntimeError, match="either DB_PASSWORD"):
        secret_value("DB_PASSWORD", environ={
            "DB_PASSWORD": "environment-value",
            "DB_PASSWORD_FILE": str(path),
        })


def test_secret_reader_rejects_missing_large_and_invalid_utf8_files(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(RuntimeError, match="could not be read"):
        secret_value("JWT_SECRET", required=True, environ={"JWT_SECRET_FILE": str(missing)})

    large = tmp_path / "large"
    large.write_bytes(b"x" * 65537)
    with pytest.raises(RuntimeError, match="allowed size"):
        secret_value("JWT_SECRET", environ={"JWT_SECRET_FILE": str(large)})

    invalid = tmp_path / "invalid"
    invalid.write_bytes(b"\xff")
    with pytest.raises(RuntimeError, match="UTF-8"):
        secret_value("JWT_SECRET", environ={"JWT_SECRET_FILE": str(invalid)})


def test_database_url_is_built_from_file_secret_and_escapes_password(tmp_path):
    password = tmp_path / "db_password"
    password.write_text("p@ss:/word", encoding="utf-8")
    result = resolve_database_url({
        "DB_PASSWORD_FILE": str(password),
        "DB_HOST": "database",
        "DB_PORT": "5432",
        "DB_USER": "reconcilex",
        "DB_NAME": "ledger",
    })
    assert result == "postgresql+psycopg://reconcilex:p%40ss%3A%2Fword@database:5432/ledger"


def test_database_url_can_be_loaded_as_a_complete_secret(tmp_path):
    path = tmp_path / "database_url"
    path.write_text("postgresql+psycopg://user:secret@db:5432/app", encoding="utf-8")
    assert resolve_database_url({"DATABASE_URL_FILE": str(path)}) == path.read_text()


def test_s3_client_uses_file_backed_access_and_secret_keys(tmp_path, monkeypatch):
    access = tmp_path / "access"
    secret = tmp_path / "secret"
    access.write_text("file-access", encoding="utf-8")
    secret.write_text("file-secret", encoding="utf-8")
    captured = {}
    monkeypatch.setenv("S3_ENDPOINT", "https://objects.example.invalid")
    monkeypatch.setenv("S3_ACCESS_KEY", "")
    monkeypatch.setenv("S3_SECRET_KEY", "")
    monkeypatch.setenv("S3_ACCESS_KEY_FILE", str(access))
    monkeypatch.setenv("S3_SECRET_KEY_FILE", str(secret))
    monkeypatch.setattr(storage.boto3, "client", lambda *args, **kwargs: captured.update(kwargs) or object())

    storage._client()
    assert captured["aws_access_key_id"] == "file-access"
    assert captured["aws_secret_access_key"] == "file-secret"


def test_offsite_settings_accepts_file_backed_credentials(tmp_path, monkeypatch):
    def write(name, value):
        path = tmp_path / name
        path.write_text(value, encoding="utf-8")
        return str(path)

    monkeypatch.setenv("OFFSITE_ENABLED", "1")
    monkeypatch.setenv("OFFSITE_S3_ENDPOINT", "https://remote.example.invalid")
    monkeypatch.setenv("OFFSITE_S3_BUCKET", "remote-backups")
    monkeypatch.setenv("OFFSITE_S3_ACCESS_KEY", "")
    monkeypatch.setenv("OFFSITE_S3_SECRET_KEY", "")
    monkeypatch.setenv("OFFSITE_MANIFEST_HMAC_KEY", "")
    monkeypatch.setenv("OFFSITE_S3_ACCESS_KEY_FILE", write("offsite-access", "remote-access"))
    monkeypatch.setenv("OFFSITE_S3_SECRET_KEY_FILE", write("offsite-secret", "remote-secret"))
    monkeypatch.setenv("OFFSITE_MANIFEST_HMAC_KEY_FILE", write("hmac", "h" * 48))
    monkeypatch.setenv("S3_ENDPOINT", "http://silo:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "local-access")
    monkeypatch.setenv("S3_SECRET_KEY", "local-secret")

    config = offsite.settings()
    assert config.access_key == "remote-access"
    assert config.secret_key == "remote-secret"
    assert config.manifest_hmac_key == "h" * 48


def test_storage_bucket_initializer_is_idempotent():
    class ExistingBucket:
        def __init__(self):
            self.created = 0

        def head_bucket(self, Bucket):
            return {}

        def create_bucket(self, Bucket):
            self.created += 1

    class MissingBucket(ExistingBucket):
        def head_bucket(self, Bucket):
            raise ClientError(
                {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "HeadBucket",
            )

    existing = ExistingBucket()
    assert ensure_bucket(existing, "reconcilex") is False
    assert existing.created == 0

    missing = MissingBucket()
    assert ensure_bucket(missing, "reconcilex") is True
    assert missing.created == 1
