from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from alembic import command
from alembic.config import Config


class Database:
    def __init__(self) -> None:
        database_path = self.get_database_file_path()
        engine_url = self._get_engine_url(database_path)

        self.engine = create_engine(engine_url)

        self.session_maker = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_maker)

        self._run_migrations(engine_url)

    def _get_engine_url(self, database_path: Path) -> str:
        return f"sqlite+pysqlite:///{str(database_path)}"

    def _run_migrations(self, database_url: str) -> None:
        path = Path().parent.parent.resolve() / "alembic.ini"
        alembic_cfg = Config(path)
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        command.upgrade(alembic_cfg, "head")

    def get_session(self) -> Session:
        return self.session()
    
    def get_database_directory(self) -> Path:
        return Path.home() / Path("file_ops")

    def get_database_file_path(self) -> Path:
        db_dir = self.get_database_directory()
        db_dir.mkdir(parents=True, exist_ok=True)

        db_path = db_dir / Path("file_ops.db")

        return db_path.resolve()