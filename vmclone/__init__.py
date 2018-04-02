from contextlib import contextmanager

from .transaction import VMTransaction, BadStageError, SnapshotDisk, default_disk_filter

__version__ = '0.1.0'


@contextmanager
def clone_vm(domain, *args, **kwargs):
    vmt = VMTransaction(domain, *args, **kwargs)
    vmt.initialize()
    vmt.prepare()
    vmt.begin()

    yield vmt

    vmt.commit()
