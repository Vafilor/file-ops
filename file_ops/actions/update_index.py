from file_ops.database.database import Database


def update_index(database: Database, batch_size: int = 500) -> None:
    # Mark each file as checking validity 
    # go through and index everything, this should update the status if the file is found
    # after you finish indexing
    # go through each file that is still in checking validity and check it, make sure to delete or update as appropriate
    pass