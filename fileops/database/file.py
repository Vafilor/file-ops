from ..files.file import File as BaseFile


class File(BaseFile):
    """A database File is the same as a regular file, but it has an identifying key (id)"""
    @classmethod
    def from_file(cls, file, key, eager=False):
        return cls(key=key, path=file.path, size=file._size, content_hash=file._content_hash, modified_at=file._modified_at, is_directory=file._is_directory, deleted_at=file.deleted_at, eager=eager)

    def __init__(self, key=None, path=None, size=None, content_hash=None, modified_at=None, is_directory=None, deleted_at=None, eager=False):
        BaseFile.__init__(self, path, size, content_hash, modified_at, is_directory, deleted_at, eager)
        self.key = key
