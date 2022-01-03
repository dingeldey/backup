import os
import sys
import subprocess
from pathlib import Path
import PyInstaller.__main__
import shutil
import tarfile


def build():
    """
    Running PyInstaller
    """
    if os.name == 'nt':
        miniconda_path: os.PathLike = Path(sys.executable).parents[2]
        oppenssl_path = os.path.join(miniconda_path, 'pkgs/openssl-1.1.1l-h8ffe710_0/Library/bin')
        oppenssl_path = oppenssl_path.replace(os.sep, '/')
        print(f"OpenSSL path {oppenssl_path}.")
        PyInstaller.__main__.run([
            'backup.py',
            '--onefile',
            '-F',
            '-y',
            f'--paths',
            '{oppenssl_path}'
        ])
    else:
        PyInstaller.__main__.run([
            'backup.py',
            '--onefile',
            '-F',
            '-y',
        ])


def cleanup():
    """
    Cleanup directory from build.
    """
    if os.path.isdir('__pycache__'):
        shutil.rmtree('__pycache__')
    if os.path.isdir('dist'):
        shutil.rmtree('dist')
    if os.path.isdir('build'):
        shutil.rmtree('build')
    if os.path.isfile('backup.spec'):
        os.remove('backup.spec')


def make_tarfile(source_dir, output_filename):
    """
    Create a tar.gz archive
    @param output_filename: output path
    @param source_dir: input dir
    """
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def make_release():
    """
    Make a directory for the release and pack it with all dependencies.
    """
    if os.path.isdir('release'):
        shutil.rmtree('release')

    if os.name == 'nt':
        shutil.copytree('dist/', 'release/backup')
        shutil.copy('README.md', 'release/backup')
        out = subprocess.check_output(["C:/Program Files/7-Zip/7z.exe",
                                       'a', '-t7z', './release/backup.7z', 'release/backup'])
        print(out.decode("UTF-8"))
        out = subprocess.check_output(["C:/Program Files/7-Zip/7z.exe",
                                       'a', './release/backup.zip', 'release/backup'])
        print(out.decode("UTF-8"))
    else:
        shutil.copytree('dist/', 'release/backup')
        shutil.copy('README.md', 'release/backup')
        make_tarfile('release/backup', 'release/backup.tar.gz')


def main():
    build()
    make_release()
    cleanup()


if __name__ == "__main__":
    main()
