# File Ops

File Ops is a set of components and basic CLI to get file statistics from a hard drive.
It stores this information in a SQLite database, which you can then query to your heart's content.

With it, you can

* Store all of the files and directories of a drive with
  * file/directory path
  * file/directory modified at
  * file/directory size 
  * file md5 hash

With that information you can (quickly)

* Find largest files, directories, etc across many external drives.
* Find duplicate files by comparing hashes
* Find the largest directories

## Installation

```bash
pip install file-ops
```

## Commands

All commands support a `--time` flag to output how long it took.

### Index File Stats (no hash is calculated for files)

Note: The database file is automatically created if it does not exist and defaults to `files.db`.

```bash
fileops index path/to/index database_file.db  
```

### Hash files in database

```bash
fileops hash database_file.db
```

### Cleanup

This removes files that have been deleted, etc. 
It should be followed by the index command to keep things up to date.

```bash
fileops cleanup database_file.db
```

### Calculate Folder Stats

This updates all folders with their sizes. This is a slow process.

```bash
fileops folder-stats database_file.db
```

## Find duplicate files by comparing hashes 

Use the following query to find the files with the most duplicates

```sql
SELECT hash, COUNT(*)
FROM files
WHERE is_directory = 0
GROUP BY hash
ORDER BY COUNT(*) DESC
LIMIT 10;
```

Then you can do a query per hash

```sql
SELECT *
FROM files
WHERE hash = '94bd41953ca5233c5efe121c73959af7';
```
  
## Tested on

* Mac 10.14.3
* Windows 10
* Ubuntu 18

## Project Goals

* Minimal dependencies

## Feature Ideas

### Ignore file settings

* hidden files
* certain directories

### Command to join database files

I might run multiple copies of the program on several drives, or computers, for speed.
When they are all done, I want to merge the output database files into one for easy querying.

### Calculate Directory size faster

Right now it works, but its rather slow.

### Hash Directories

Given two directories on different hard drives, I would like to be able to quickly know if they have the same contents.

If we hash all the files of a directory, can we hash the individual hashes to get a folder hash? Would that work?  

### Uniquely Identifier Hard Drives

Given a lot of external hard drives, thumb drives, etc, I want to be able to store them 
all in a single database file and be able to uniquely identify them. 

So if I index a thumb drive on my laptop, I want to be able to take it to my PC and update the index
there and still know it's for the same thumbdrive.  

If this is implemented, the file paths should not have the mount path.
The hard drive identifier should be a separate column. 

E.g. on windows

e:\projects\python\cli.py

should become

projects\python\cli.py

### Performance Improvements

Need to see if we can improve the performance. Yep.

## Grand Ideas

### Supplementary UI application

Doing this from the CLI is great, but it would be nice to have a UI application that could instantly show you

* Largest Files
* Largest Duplicate Files
* Find files by name (see Everything Search - https://www.voidtools.com/)
* Status Updates as the indexing happens (particularly for hashing)
* Re-index command
* Re-calculate hashes command

### One source for all files, with sym-links everywhere else

Once you have all of the files indexed for a drive, store them all in a directory and sym-link them everywhere else. 
With this, remove all duplicates via hash comparison.

This wouldn't work for auto-generated files of course and may only be useful for relatively static directories.
