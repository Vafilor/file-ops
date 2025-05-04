import uuid
from datetime import datetime

from sqlalchemy import update
from sqlalchemy.orm import Session

from file_ops.database import models


def update_files_to_status(
    session: Session, ids: list[uuid.UUID], status: models.FileStatus
) -> None:
    now = datetime.now()
    session.execute(
        update(models.File), [{"id": file_id, "status": status, "db_updated_at": now} for file_id in ids]
    )

def change_files_status(
    session: Session, from_status: models.FileStatus, to_status: models.FileStatus
) -> None:
    now = datetime.now()
    session.execute(
        update(models.File).where(models.File.status == from_status).values(status=to_status, db_updated_at=now)
    )