from pathlib import Path

from pydantic import BaseModel

CONFIG_ENCODING = "utf-8"


class FileOpsConfig(BaseModel):
    skip: list[str]


def get_app_config_directory() -> Path:
    return Path.home() / Path("file_ops")


def get_app_config_file_path() -> Path:
    return get_app_config_directory() / Path("config.json")


def read_config() -> FileOpsConfig:
    path = get_app_config_file_path()
    if not path.exists():
        config = FileOpsConfig(skip=[])
        path.write_text(config.model_dump_json(), encoding=CONFIG_ENCODING)
        return config

    read_content = path.read_text(encoding=CONFIG_ENCODING)
    return FileOpsConfig.model_validate_json(read_content)


def write_config(data: FileOpsConfig) -> None:
    path = get_app_config_file_path()
    path.write_text(data.model_dump_json(), encoding=CONFIG_ENCODING)
