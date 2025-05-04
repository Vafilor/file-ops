from typing import Sequence

from sqlalchemy import delete
from sqlalchemy.orm import Session

from file_ops.database import models


class FileStatsService:
    @staticmethod
    def delete_for_content_hashes(session: Session, hashes: Sequence[str]) -> None:
        if not len(hashes):
            return None

        statement = delete(models.FileStats).where(models.FileStats.content_hash.in_(hashes))
        
        session.execute(statement)