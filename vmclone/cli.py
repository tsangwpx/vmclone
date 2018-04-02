import logging
import os
from argparse import ArgumentParser
import subprocess

import libvirt
from lxml import etree

from .transaction import VMTransaction

logger = logging.getLogger(__name__)


def say(*args, **kwargs):
    print(*args, flush=True, **kwargs)


def parse():
    p = ArgumentParser()
    p.add_argument('--connect', '-c', dest='conn_uri', default='qemu:///system',
                   help='URI to a hypervisor')
    p.add_argument('--disk-only', action='store_true',
                   help='Save the disk state only and no memory state is preserved')
    p.add_argument('--quiesce', action='store_true',
                   help='Quiesce')
    p.add_argument('--workdir', default=None,
                   help='Working directory for images')
    p.add_argument('--verbose', '-v', action='count', default=0,
                   help='Enable verbose')
    p.add_argument('--dry-run', '-n', action='store_true', dest='dryrun',
                   help='')
    p.add_argument('domain',
                   help='Name of domain')
    p.add_argument('destdir',
                   help='Target directory used to store the backup')

    return p.parse_args()


def copy_block(source, dest):
    args = [
        '/usr/bin/qemu-img',
        'convert',
        '-f', 'raw',
        '-O', 'qcow2',
        '-S', '4k',
        source,
        dest,
    ]

    logger.debug('Executing %s', args)

    subprocess.check_call(args)


def copy_file(source, dest, overwrite=False):
    args = [
        '/bin/cp',
        '--sparse=auto',
    ]

    if not overwrite:
        if os.path.exists(dest):
            raise FileExistsError(dest)

        args.append('--no-clobber')

    args.extend((source, dest))

    logger.debug('Executing %s', args)

    subprocess.check_call(args)


def main():
    ns = parse()

    if ns.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif ns.verbose >= 1:
        logging.basicConfig(level=logging.INFO)

    if ns.dryrun:
        logger.info('Open readonly connection to libvirt daemon')
        conn = libvirt.openReadOnly(ns.conn_uri)
    else:
        logger.info('Open connection to libvirt daemon')
        conn = libvirt.open(ns.conn_uri)

    logger.info('Lookup domain by name')
    domain = conn.lookupByName(ns.domain)

    if not ns.workdir:
        logger.info('No workdir is specified')

    logger.info('Domain: {}'.format(ns.domain))
    logger.info('Working Directory {}'.format(ns.workdir))
    logger.info('Backup Path: {}'.format(ns.destdir))

    vmc = VMTransaction(domain, ns.workdir, ns.disk_only, ns.quiesce)

    vmc.initialize()
    vmc.prepare()
    vmc.begin()

    try:
        disks = vmc.snapshot_disks

        for disk in disks:
            device = disk.device
            source = disk.source
            source_type = disk.source_type

            if source_type == 'block':
                filename = '{}.img'.format(device)
            elif source_type == 'file':
                dummy, ext = os.path.splitext(source)
                filename = device + ext

            dest = os.path.join(ns.destdir, filename)

            copy_file(source, dest)

    except BaseException:
        logger.info('Exception occurred during committing')
        raise
    finally:
        try:
            vmc.commit()
        except BaseException:
            logger.error('Failed in committing')
            raise
