from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Sequence

import click

from file_ops.database import models


def get_user_int_input(
    prompt: str, min: int, max: int, quit: str, skip: str
) -> int | Literal["skip", "quit"]:
    result: None | str = None

    while not result:
        result = input(prompt)

        if result.lower() == quit:
            return "quit"

        if result.lower() == skip:
            return "skip"

        try:
            int_value = int(result)
            if int_value >= min and int_value <= max:
                return int_value
            else:
                click.echo(f"Please choose from {min} to {max}")
                result = None
        except ValueError:
            click.echo(f"Please choose from {min} to {max}")
            result = None

    return "quit"


def prompt_choice(prompt: str, choices: list[str]) -> str:
    while True:
        value = input(prompt).lower()
        if value in choices:
            return value

        click.echo("Not a valid choice")


def prompt_choice_with_int_range(
    prompt: str, choices: list[str], min: int, max: int
) -> str:
    while True:
        value = input(prompt).lower()
        if value in choices:
            return value

        try:
            int_value = int(value)

            if min <= int_value <= max:
                return value
        except ValueError:
            pass

        click.echo("Not a valid choice")


def parse_int(value: str) -> int | None:
    try:
        int_value = int(value)
        return int_value
    except ValueError:
        return None


@dataclass
class DedupePromptAction:
    action: Literal["quit", "skip"]


@dataclass
class FileAction:
    action: Literal["view", "open-directory"]
    path: Path  # the path to the file, regardless of view or open-directory


@dataclass
class FileKeepAction:
    action: Literal["keep"]
    index: int


def dedupe_prompt(
    files: Sequence[models.File],
) -> DedupePromptAction | FileAction | FileKeepAction:
    file_count = len(files)
    prompt = f"Which file do you want to keep 1-{file_count} (q to quit, s to skip, v to view, d to open directory):"

    while True:
        value = input(prompt).lower().strip()
        if value == "q":
            return DedupePromptAction("quit")
        if value == "s":
            return DedupePromptAction("skip")

        if not len(value):
            click.echo("Not a valid choice")
            continue

        if value[0] in ["v", "d"]:
            int_value = parse_int(value[1:])
            if not int_value:
                click.echo("Not a valid integer")
                continue
            if int_value < 1 or int_value > file_count:
                click.echo("Not a valid choice")
                continue

            path = Path(files[int_value - 1].path)

            if value[0] == "v":
                return FileAction(action="view", path=path)

            return FileAction(action="open-directory", path=path)

        int_choice = parse_int(value)
        if not int_choice:
            click.echo("Not a valid file")
            continue

        return FileKeepAction(action="keep", index=int_choice - 1)


def prompt_choice_with_actions(
    prompt: str,
    continue_choices: list[str],
    action_choices: dict[str, Callable[[], None]],
) -> str:
    while True:
        value = input(prompt).lower()
        if value in continue_choices:
            return value

        if value in action_choices:
            action = action_choices[value]
            action()
            continue

        click.echo("Not a valid choice")
