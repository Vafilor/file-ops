from multiprocessing import Process
from pathlib import Path

from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from alembic import command
from file_ops.config.config import get_app_config_directory


class Database:
    @staticmethod
    def _run_migrations(database_url: str) -> None:
        path = Path().parent.parent.resolve() / "alembic.ini"
        alembic_cfg = Config(path)
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)

        command.upgrade(alembic_cfg, "heads")

    def __init__(self) -> None:
        database_path = self.get_database_file_path()
        engine_url = self._get_database_url(database_path)

        self.engine = create_engine(engine_url)

        self.session_maker = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_maker)

        p = Process(target=Database._run_migrations, args=(engine_url,))
        p.start()
        p.join()

    def _get_database_url(self, database_path: Path) -> str:
        return f"sqlite+pysqlite:///{str(database_path)}"

    def get_session(self) -> Session:
        return self.session()

    def get_database_directory(self) -> Path:
        return get_app_config_directory()

    def get_database_file_path(self) -> Path:
        db_dir = self.get_database_directory()
        db_dir.mkdir(parents=True, exist_ok=True)

        db_path = db_dir / Path("file_ops.db")

        return db_path.resolve()
