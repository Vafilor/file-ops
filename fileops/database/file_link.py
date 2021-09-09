from fileops.database.file import File


class FileLink:
    """
    A FileLink represents two files that have an identical hash, identified by their ids as stored in the files
    table
    """
    def __init__(self, file_1_id, file_2_id):
        self.file_1_id = file_1_id
        self.file_2_id = file_2_id


class FileLinkWithFiles(FileLink):
    """
    A FileLinkWithFiles stores a file link, along with the associated file information.
    """
    def __init__(self, file1: File, file2: File):
        super().__init__(file1.key, file2.key)
        self.file1 = file1
        self.file2 = file2
