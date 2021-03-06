# backup script
This project features a script using rsync to make backups with the possibility of creating incremental backups. Only use it to backup directories to a backup directory. If you put files directly in the specified backup directory, when removing them it is not possible to remove them in the backup in an incremental backup.

## Prerequisites
- Python3 
- rsync
- wsl for windows rsync

## Usage

First time you start you run something like this:

```
python3 backup.py -f="--stats" -f="--progress" -f='--delete' -f='-av' -f='--recursive' -f='--no-perms' -f="--chmod=u=rw,g=r,o-rwx,-X" -s /home/user/source1 -s /home/user/source2 -d /home/backup_destination
```
where the -f option specifies the switches for rsync.

This will create a folder called ```0``` in ```/home/backup_destination```. This is the location of your first backup series starting with a full backup. Within this folder you find a folder which has the name of the time when you initiated the backup. This is your first backup of the series. In this folder you will find copies of the two sources specified above.

After that you essentially have two options for the next run:

1) You do the same again
2) You decide you want to take an incremental backup.

### Option 1: Same old, same old... do it once again

run
```
python3 backup.py -f='--delete' -f='-av' -f='--recursive' -f='--no-perms' -f="--chmod=u=rw,g=r,o-rwx,-X" -s /home/user/source1 -s /home/user/source2 -d /home/backup_destination
```
or modify the sources. This will cause two things to happen:
1) the folder previously called ```0``` is now moved and renamed to the most recent backup run which is placed within it.
2) a new folder called ```0``` is created and the new series connected to the current full backup is placed within it.


### Option 2: Run an incremental backup.
For that use the command
```
python3 backup.py -f='--delete' -f='-av' -f='--recursive' -f='--no-perms' -f="--chmod=u=rw,g=r,o-rwx,-X" -s /home/user/source1 -s /home/user/source2 -d /home/backup_destination -i
```
This will cause two things to happen:
1) We first make a copy of the most recent folder within ```0``` using hard links and give it a name corresponding to the time where we triggered the backup.
2) We now synchronize the sources to that newly created folder.

This concludes our first incremental backup.

## Using the releases
When using the releases they ship as self-contained executables. Just run them directly as above but without calling python.
