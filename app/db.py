import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker
from reconcilex_config import secret_value

def resolve_database_url(environ=None):
    env = os.environ if environ is None else environ
    configured = secret_value("DATABASE_URL", environ=env)
    if configured:
        return configured
    password = secret_value("DB_PASSWORD", environ=env)
    if password:
        return URL.create(
            "postgresql+psycopg", username=env.get("DB_USER", "reconcilex"),
            password=password, host=env.get("DB_HOST", "localhost"),
            port=int(env.get("DB_PORT", "5432")),
            database=env.get("DB_NAME", "reconcilex"),
        ).render_as_string(hide_password=False)
    return "sqlite+pysqlite:///./reconcilex.db"

DATABASE_URL = resolve_database_url()
engine = create_engine(
    DATABASE_URL, pool_pre_ping=True,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
