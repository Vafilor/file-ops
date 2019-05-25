import hashlib
import os
import stat
from datetime import datetime
from typing import Optional, Union


class File:
    # Constant for how many bytes to read at a time while calculating content_hash for a file.
    BytesToRead = 1048576

    def __init__(self, path: Union[bytes, str], size: Optional[int] = None, content_hash: Optional[str] = None,
                 modified_at: Optional[datetime] = None, is_directory: Optional[bool] = None,
                 deleted_at: Optional[datetime] = None, eager: bool = False):
        """
        If eager is set to true, file data (apart from content hash) are loaded immediately.
        """
        self.path = path
        self._size = size
        self._content_hash = content_hash
        self._modified_at = modified_at
        self._is_directory = is_directory
        self.deleted_at = deleted_at

        if eager:
            self.load()

    @property
    def size(self) -> Union[int, None]:
        """
        For a File, returns the size of the file.
        For a directory, returns None.

        Raises a OSError if the file at the path can not be opened in rb mode.
        :return:
        """
        if self._size is None:
            if self.is_directory:
                return None

            self.load()

        return self._size

    @size.setter
    def size(self, value: int):
        """
        Sets the size of the file, in bytes.
        """
        self._size = value

    @property
    def content_hash(self) -> str:
        """
        Calculates the md5 hash of the file contents.
        If we have a directory, returns an empty string.

        Raises a OSError if the file at the path can not be opened in rb mode.
        """
        if self.is_directory:
            return ''

        if self._content_hash is None:
            hash_md5 = hashlib.md5()
            with open(self.path, "rb") as f:
                for chunk in iter(lambda: f.read(File.BytesToRead), b""):
                    hash_md5.update(chunk)

            self._content_hash = hash_md5.hexdigest()

        return self._content_hash

    @content_hash.setter
    def content_hash(self, value: str):
        """
        Sets the file's content hash, excepted to be a string.
        """
        self._content_hash = value

    @property
    def modified_at(self) -> datetime:
        """
        Returns the modified_at time as a datetime.

        Raises a OSError if the file at the path can not be opened in rb mode.
        """
        if self._modified_at is None:
            self.load()

        return self._modified_at

    @modified_at.setter
    def modified_at(self, value: datetime):
        self._modified_at = value

    @property
    def is_directory(self) -> bool:
        """
        Raises a FileNotFoundError if the file is not found.

        Returns true if file is a directory, false otherwise.
        """
        if self._is_directory is None:
            self.load()

        return self._is_directory

    @is_directory.setter
    def is_directory(self, value: bool):
        self._is_directory = value

    @property
    def exists(self) -> bool:
        """
        Returns true if the file exists, false otherwise.
        Always checks on the spot, no caching is done.
        """
        return os.path.exists(self.path)

    def load(self):
        """
        Loads the file statistics.
            Files load
                * is_directory
                * size
                * modified_at
            Directories load
                * is_directory
                * modified_at

        Raises a FileNotFoundError if the file is not found.
        """
        stats = os.stat(self.path)
        self.is_directory = stat.S_ISDIR(stats.st_mode)
        self.modified_at = datetime.utcfromtimestamp(stats.st_mtime)

        if not self.is_directory:
            self.size = stats.st_size

    def __hash__(self):
        return self.content_hash

    def __eq__(self, other):
        return self.size == other.size and self.content_hash == other.content_hash

    def __str__(self):
        return 'file_path:{}\nsize:{}\nhash:{}\nmodified_at:{}\ndeleted_at:{}'.\
            format(self.path, str(self._size), self._content_hash, self._modified_at,
                   self.deleted_at)
