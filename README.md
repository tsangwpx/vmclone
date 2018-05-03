# vmclone with Python

This package provides:
1. a command `vmclone` clones virtual machines online.
2. a module [`vmclone.transaction`](vmclone/transaction.py) that manages hot cloning (or ultizes tempoary snapshots).

## Requirements
1. Python 3.5 (tested on Debian stretch)
2. [libvirt-python](https://github.com/libvirt/libvirt-python)

## Install / Uninstall
In the project root, run `pip3 install .` or `pip3 uninstall vmclone`

## Usage
See `vmclone --help` or the source code. :see_no_evil:


