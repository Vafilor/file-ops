import logging
from pathlib import Path

import click

from file_ops.actions import update_index
from file_ops.actions.archive import remove_directory, zip_path, zip_path_like
from file_ops.actions.dedupe import (
    generate_file_stats_by_biggest_file_count,
    move_files_to_directory,
)
from file_ops.actions.hash import hash_files
from file_ops.actions.index import index_directory
from file_ops.actions.stats import calculate_stats
from file_ops.actions.unique_names import (
    execute_unqiue_name_changes,
    generate_unique_name_changes,
)
from file_ops.actions.watch import watch
from file_ops.config.config import read_config
from file_ops.database.database import Database
from file_ops.database.file_service import FileService
from file_ops.file_size.file_size import humanize_bytes_str, parse_file_size
from file_ops.logging_config import configure_logging, verbosity_extra
from file_ops.system.system import open_directory, open_file
from file_ops.user_io.user_io import (
    dedupe_prompt,
    prompt_choice,
    prompt_choice_with_actions,
)

logger = logging.getLogger(__name__)


@click.group()
@click.version_option("0.1.0", prog_name="file-ops")
@click.option("-v", "--verbose", count=True, default=1)
def cli(verbose: int) -> None:
    configure_logging("INFO", verbosity=verbose)


@cli.command("uqname")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True),
)
@click.option("--dry-run", type=bool, is_flag=True)
@click.option("--recursive", type=bool, is_flag=True)
def unique_name_files_in_path(path: str, dry_run: bool, recursive: bool) -> None:
    """Goes through all of the files in the path and gives them unique names that are uuids.
    Files that have the same name but different extension will be preserved in the new naming scheme.

    The database file will record the changes.
    """
    database = Database()

    for changes in generate_unique_name_changes(Path(path), recursive=recursive):
        for change in changes:
            print(f"{change.original_path} -> {change.new_path}")

        if not dry_run:
            execute_unqiue_name_changes(database, changes)


@cli.command("index")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True),
)
def index_path(path: str) -> None:
    config = read_config()
    database = Database()

    click.echo(f"Indexing {path}")
    index_directory(path=Path(path), database=database, skip=config.skip)


@cli.command("hash")
@click.option("--prefix", type=str)
@click.option("--max-workers", type=int, default=5)
def hash_database_files(prefix: str | None, max_workers: int) -> None:
    database = Database()
    hash_files(database=database, max_workers=max_workers, prefix=prefix)


@cli.command("stats")
def calculate_stats_for_database_files() -> None:
    database = Database()
    calculate_stats(database=database)


@cli.command("check")
@click.option("--prefix", type=str)
@click.option("--dry-run", type=bool, is_flag=True)
@click.option("--limit", type=int)
@click.option("--progress-counter", type=int, default=1000)
def check_database_files(
    prefix: str | None,
    dry_run: bool,
    limit: int | None,
    progress_counter: int | None,
) -> None:
    database = Database()

    total_processed = 0
    total_deletes = 0
    total_updates = 0
    counter = 0

    logger.info("Counting changes")
    total_changes = update_index.count_index_changes(database=database, prefix=prefix)
    logger.info(f"Total files to check {total_changes:,} files")

    limit_str = "" if limit is None else str(limit)
    logger.info(f"Checking {limit_str} database files")

    file_service = FileService(session_maker=lambda: database.get_session())

    for changes in update_index.generate_index_changes(
        database=database, prefix=prefix, batch_size=500
    ):
        if limit and total_processed >= limit:
            logger.info(f"Limit {limit} reached. {total_processed} files processed")
            return

        if len(changes.deletes):
            total_deletes += len(changes.deletes)
            logger.info(f"Deleting {len(changes.deletes)} files")

        for file in changes.deletes:
            logger.info(f"Deleting {file.path}", extra=verbosity_extra(3))

        if len(changes.updates):
            total_updates += len(changes.updates)
            logger.info(f"Updating {len(changes.updates)} files")

        for update_change in changes.updates:
            logger.info(
                f"Updating {update_change.path} because of: {update_change.reason}",
                extra=verbosity_extra(3),
            )

        if not dry_run:
            update_index.execute_file_changes(
                file_service=file_service, changes=changes
            )

        counter += changes.files_checked
        total_processed += changes.files_checked

        # > is important here because we process in batches. The batch size might not fit
        # progress_counter nicely
        if progress_counter and counter >= progress_counter:
            counter = 0
            percent_done = (total_processed / total_changes) * 100
            logger.info(
                f"{percent_done:.4}% Processed {total_processed:,} / {total_changes:,} files"
            )

    logger.info(f"{100.0:.4}% Processed {total_processed:,} / {total_changes:,} files")
    logger.info(f"Deletes: {total_deletes:,}")
    logger.info(f"Updates: {total_updates:,}")


@cli.command("dedupe")
@click.option(
    "--dupe-path",
    type=click.Path(exists=False, file_okay=False, readable=True, resolve_path=True),
)
@click.option(
    "--max-count",
    type=int,
    default=50,
)
@click.option("--min-size", type=str)
def dedupe(dupe_path: str, max_count: int, min_size: str | None) -> None:
    if min_size is None:
        min_file_size = 0
    else:
        try:
            min_file_size = parse_file_size(min_size)
        except ValueError:
            click.echo("Unable to parse file size")
            return

    dupe_files_path = Path(dupe_path)
    if not dupe_files_path.exists():
        click.echo(f"{dupe_path} does not exist. Creating.")
        dupe_files_path.mkdir(parents=True)

    database = Database()
    file_service = FileService(session_maker=lambda: database.get_session())

    count = 0
    file_size_saved: int = 0
    file_count_saved: int = 0

    for file_stats in generate_file_stats_by_biggest_file_count(
        session=database.get_session(), min_file_size=min_file_size
    ):
        for original_file_stat in file_stats:
            file_stat = file_service.check_and_update_file_stat(original_file_stat)
            if not file_stat or file_stat.file_count == 1:
                continue

            humanized_size = humanize_bytes_str(file_stat.file_size)

            files_count = file_stat.file_count
            duplicate_size = (files_count - 1) * file_stat.file_size
            duplicate_size_humanize = humanize_bytes_str(duplicate_size)

            if file_size_saved != 0:
                click.echo(
                    f"{count}: Saved {file_count_saved:,} files with total {humanize_bytes_str(file_size_saved)}"
                )

            click.echo(
                f"There are {files_count:,} files with hash {file_stat.content_hash}. The files are {humanized_size}. Extra space taken: {duplicate_size_humanize}"
            )

            if files_count > max_count:
                sample_files = file_service.get_files_for_hash(
                    hash=file_stat.content_hash, limit=1
                )
                sample_file = sample_files[0]

                click.echo(f"Sample file: {sample_file.path}")

                skip_choice = prompt_choice(
                    "Skip(s) View(v) Quit (q):", choices=["s", "v", "q"]
                )
                if skip_choice == "q":
                    click.echo("Quitting")
                    return

                if skip_choice == "s":
                    continue

            files = file_service.get_files_for_hash(hash=file_stat.content_hash)

            click.echo("Duplicate files")

            for index, file in enumerate(files, start=1):
                click.echo(f"{index}: {file.path}")

            while True:
                user_input = dedupe_prompt(files=files)
                if user_input.action == "quit":
                    click.echo("Quitting")
                    return

                if user_input.action == "skip":
                    click.echo("Skipping")
                    break

                if user_input.action == "view":
                    open_file(user_input.path)
                    continue

                if user_input.action == "open-directory":
                    open_directory(user_input.path)
                    continue

                file_index = user_input.index
                kept_file = files[file_index]
                moved_files = [file for file in files if file.id != kept_file.id]
                if not kept_file.content_hash:
                    raise ValueError(f"File {kept_file.id} has no content hash")

                destination_directory = dupe_files_path / Path(kept_file.content_hash)

                click.echo(f"Moving files to {destination_directory}")
                move_files_to_directory(
                    database=database, files=moved_files, path=destination_directory
                )

                file_size_saved += duplicate_size
                file_count_saved += files_count - 1
                count += 1

                break


@cli.command("watch")
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True),
)
def watch_files(path: str | None) -> None:
    if path is None:
        path_obj = Path.cwd()
    else:
        path_obj = Path(path)

    if not path_obj.exists():
        click.echo(f"{path} is not a valid path", err=True)
        return

    database = Database()
    click.echo(f"Watching path {str(path_obj)}")

    watch(path=path_obj, database=database)


@cli.command("archive-from-db")
def archive_from_db() -> None:
    database = Database()

    # Other entries to consider
    # .webpack ?
    # php composer modules - vendor
    # golang

    # "%onepanel\\web\\",

    # zip_path_like(
    #     database=database,
    #     like="%GoogleRestaurantScraper",
    #     dry_run=True,
    #     delete_original=False,
    # )

    # return

    like_paths = [
        "%coin-genius",
        "%citees-blist-portal",
        "%blastflyers-wordpress",
        "%mycommute",
        "%imbeekio-talar-io",
        "%goldcars-etc",
        "%zombie-rancher",
        "%time_tracker",
        "%onepanel\\onepanel_modeldb",
        "%utilities\\kubernetes",
        "%elastic_search",
        "%__MACOSX",
        "%venv",
        "%GoogleRestaurantScraper",
        "%SQLExport\\Saved_Queries",
        "%kubernetes\\kubernetes-master",
        "%\\app\\logs",
        "%\\app\\cache",
        "%onepanel\\web",
        "%onepanel\\python-sdk",
        "%onepanel\\api",
        "%onepanel\\cron",
        "%onepanel\\onepanel-cli",
        "%onepanel\\cli",
        "%onepanel\\onepanel-web",
        "%onepanel\\onepanel-web-admin",
        "%onepanel\\cvat",
        "%onepanel\\core-ui",
        "%VervetaCRM-master\\packages",
        "%zombie_rancher\\var",
        "%modeldb_cli",
        "%projects\\bluez",
        "%bluetooth\\bluez-5.55",
        "E:\\projects\\cpp%Release",
        "E:\\projects\\cpp%Debug",
        "%C++%Release",
        "%C++%Debug",
        "%DMN-Code\\DMN\\Pods",
        "%AppData\\Roaming\\Adobe",
        "%AppData\\Local\\Programs\\Python",
        "%AppData\\Local\\Packages",
        "%.onepanel",
        "%MyTTP\\packages",
        "%MyTTP.iOS",
        "%WebBundle\\Resources",
        "%.gradle",
        "%wp-content",
        "%var\\cache",
        "%obj\\Debug",
        "%iPhoneSimulator",
        "%.idea",
        "%Xcode\\UserData",
        "%.venv",
        "%.git",
        "%CoreSimulator",
        "%site-packages",
        "%vendor",
        "%node_modules",
    ]

    for like_path in like_paths:
        click.echo(f"Checking path pattern: {like_path}")
        zip_path_like(
            database=database,
            like=like_path,
            dry_run=False,
            delete_original=True,
        )


@cli.command("archive")
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True),
)
@click.option("--confirm", type=bool, is_flag=True, default=True)
def archive(path: str | None, confirm: bool) -> None:
    dry_run = False
    delete_original = True

    if not path:
        click.echo("No path provided")
        return

    path_obj = Path(path)
    if not path_obj.exists():
        click.echo(f"Path {path} does not exist")
        return

    def list_directory(path: Path) -> None:
        for file in path.iterdir():
            click.echo(f"  {file}")

    paths_to_zip = list[Path]()

    for file in path_obj.iterdir():
        if not file.is_dir():
            continue

        if file.with_suffix(".zip").exists():
            logger.info(f"Zip found, skipping: {file}")

            # if delete_original:
            #     remove_directory(file)

            continue

        if not confirm:
            logger.info(f"Zipping {file}")
        else:
            do_zip = prompt_choice_with_actions(
                f"{file} - zip? (y/n) d to list contents:",
                continue_choices=["y", "n"],
                action_choices={"d": lambda: list_directory(file)},
            )

            if do_zip.lower() != "y":
                continue

        if dry_run:
            continue

        paths_to_zip.append(file)

    for file in paths_to_zip:
        if confirm:
            logger.info(f"Zipping {file}")

        try:
            zip_path(file)

            if delete_original:
                remove_directory(file)

        except Exception as e:
            logger.error(f"Unable to zip path {file}", exc_info=e)

            zipped_file_path = file.with_suffix(".zip")
            try:
                zipped_file_path.unlink(missing_ok=True)
            except Exception:
                logger.error(f"Unable to delete failed zip: {zipped_file_path}")


if __name__ == "__main__":
    cli()
