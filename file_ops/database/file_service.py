import logging
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from file_ops.database import models

logger = logging.getLogger(__name__)

class FileService:
    @staticmethod
    def get_for_hashes(session: Session, hashes: Sequence[str]) -> Sequence[models.File]:
        query = (
            select(models.File)
            .where(models.File.content_hash.in_(hashes))
        )

        return session.scalars(query).all()