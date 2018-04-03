import enum
import logging
import os
import time
import typing
from types import SimpleNamespace
from typing import Callable
from collections import namedtuple

import libvirt
from lxml import etree

logger = logging.getLogger(__name__)

SnapshotDisk = namedtuple('SnapshotDisk', ('device', 'source', 'source_type'))


def default_disk_filter(node):
    device_name = node.xpath('string(target/@dev)')

    # Skip snapshot, readonly, shareable and transient disks
    snapshot = node.xpath('string(@snapshot)')
    readonly = bool(len(node.xpath('readonly')))
    shareable = bool(len(node.xpath('shareable')))
    transient = bool(len(node.xpath('transient')))

    if snapshot == 'no' or readonly or shareable or transient:
        logger.debug('reject dev %s due to property', device_name)
        return False

    # Ignore non-qemu driver
    driver_name = node.xpath('string(driver/@name)')
    if driver_name != 'qemu':
        logger.debug('reject dev %s due to driver type', device_name)
        return False

    # Only allow raw and qcow2 formats
    driver_type = node.xpath('string(driver/@type)')
    if driver_type not in ('raw', 'qcow2'):
        logger.debug('reject dev %s due to driver subtype', device_name)
        return False

    # Only disk devices backed by file and block are currently supported
    device_type = node.xpath('string(@device)')
    source_type = node.xpath('string(@type)')

    if device_type == 'disk' and source_type in ('file', 'block'):
        logger.debug('accept dev %s', device_name)
        return True

    logger.debug('reject dev %s', device_name)
    return False


class LazyString(SimpleNamespace):
    """
    Lazily output a string

    """

    def __init__(self, payload):
        self.payload = payload

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        payload = self.payload
        if isinstance(payload, Callable):
            return payload()
        else:
            return str(payload)


class LazyDiskString(LazyString):
    """
    Lazily output a string from a disk node
    """

    def __str__(self):
        node = self.payload

        if node is None:
            return '<unknown disk>'

        device = node.xpath('string(target/@dev)')
        device_type = node.xpath('string(@device)')

        return '<{}, {}>'.format(device, device_type)

    def __repr__(self):
        return self.__str__()


class BadStageError(Exception):
    pass


class TransactionStage(enum.IntEnum):
    """
    Stages of a transaction from UNINITIALIZED to FINISHED
    """
    FAILED = -1

    # The transaction object is uninitialized
    UNINITIALIZED = 0

    # The transaction object is initialized. Domain name and xml are available
    INITIALIZED = 1

    # The transaction is prepared. snapshot xml is available
    PREPARED = 2

    # The transaction is begun. snapshot have been taken
    BEGUN = 3

    # The transaction is committing.
    COMMITTING = 4

    # The transaction is committed and finished
    FINISHED = 5


class VMTransaction:
    """
    Transaction logic when manipulating images of a virtual machine
    """

    def __init__(self, domain: libvirt.virDomain, workdir=None, disk_only=True, quiesce=False,
                 disk_filter: Callable = None):
        self._stage = TransactionStage.UNINITIALIZED
        self._domain = domain
        self._workdir = workdir
        self._disk_only = disk_only
        self._quiesce = quiesce

        self._disk_filter = disk_filter

        # Readonly members
        self._domain_name = None
        self._domain_xml = None
        self._snapshot_xml = None
        self._snapshot_flags = 0
        self._snapshot_disks_readonly = None

        # private members
        self._domain_xmltree = None
        self._snapshot_xmltree = None
        self._snapshot_disks = []

    @property
    def stage(self):
        return self._stage

    @property
    def domain(self):
        return self._domain

    @property
    def workdir(self):
        return self._workdir

    @property
    def disk_only(self):
        return self._disk_only

    @property
    def quiesce(self):
        return self._quiesce

    @property
    def domain_name(self):
        self._check_stage_between(TransactionStage.INITIALIZED)
        return self._domain_name

    @property
    def snapshot_xml(self):
        self._check_stage_between(TransactionStage.PREPARED)
        return self._snapshot_xml

    @property
    def snapshot_flags(self):
        self._check_stage_between(TransactionStage.PREPARED)
        return self._snapshot_flags

    @property
    def snapshot_disks(self) -> typing.Sequence[SnapshotDisk]:
        self._check_stage_between(TransactionStage.PREPARED)

        if self._snapshot_disks_readonly is None:
            def mapper(node):
                device = node.xpath('string(target/@dev)')
                source_type = node.xpath('string(@type)')
                if source_type == 'file':
                    source = node.xpath('string(source/@file)')
                elif source_type == 'block':
                    source = node.xpath('string(source/@block)')

                return SnapshotDisk(device, source, source_type)

            self._snapshot_disks_readonly = tuple(map(mapper, self._snapshot_disks))

        return self._snapshot_disks_readonly

    @property
    def disk_filter(self):
        return self._disk_filter

    @disk_filter.setter
    def disk_filter(self, func: Callable):
        self._check_stage_between(TransactionStage.UNINITIALIZED, TransactionStage.INITIALIZED)
        self._disk_filter = func

    def _check_stage(self, stage):
        if self.stage != stage:
            raise BadStageError('Stage {} is expected instead of {}'.format(stage, self.stage))

    def _check_stage_between(self, start: TransactionStage, end: TransactionStage = TransactionStage.FINISHED):
        stage = self._stage

        if self._stage < start or self._stage > end:
            raise BadStageError('Stage {} is not between {} and {}'.format(stage, start, end))

    def _prepare_snapshot(self, disk_nodes):
        xmltree = self._prepare_snapshot_xml(disk_nodes)

        flags = libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_NO_METADATA
        flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC

        if self.disk_only:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_DISK_ONLY

        if self.quiesce:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_QUIESCE

        return xmltree, flags

    def _prepare_snapshot_xml(self, disk_nodes: typing.Sequence):
        from lxml.builder import E

        if self._disk_only:
            memory_element = E.memory({
                'snapshot': 'no',
            })
        else:
            if not self._workdir:
                raise TypeError('workdir is not set')

            memory_file = os.path.join(self._workdir, 'memory.state')
            memory_element = E.memory({
                'snapshot': 'external',
                'file': memory_file,
            })

        disk_elements = []
        for node in disk_nodes:
            device_name = node.xpath('string(target/@dev)')

            source_type = node.xpath('string(@type)')

            if self._workdir:
                disk_workdir = self._workdir
                delta_basename = '{}-{}-unmerged.qcow2'.format(self._domain_name, device_name)
            else:
                if source_type != 'file':
                    raise TypeError('No workdir is available to back up disk %s', LazyDiskString(node))

                source_file = node.xpath('string(source/@file)')
                disk_workdir = os.path.dirname(source_file)

                basename = os.path.basename(source_file)
                filename, ext = os.path.splitext(basename)
                delta_basename = '{}-unmerged.qcow2'.format(filename)

            delta_file = os.path.join(disk_workdir, delta_basename)

            disk_element = E.disk(
                {
                    'name': device_name,
                    'snapshot': 'external',
                },
                E.source({
                    'file': delta_file
                }),
                E.driver({
                    'type': 'qcow2'
                })
            )
            disk_elements.append(disk_element)

        root = E.domainsnapshot(
            E.name('vmclone'),
            E.description('vmclone'),
            memory_element,
            E.disks(*disk_elements),
        )

        return root

    def initialize(self):
        self._check_stage(TransactionStage.UNINITIALIZED)

        self._domain_xml = self._domain.XMLDesc()
        self._domain_xmltree = etree.fromstring(self._domain_xml)
        self._domain_name = self._domain_xmltree.xpath('/domain/name')[0].text

        self._stage = TransactionStage.INITIALIZED
        logger.debug('stage changed to INITIALIZED')

    def prepare(self):
        self._check_stage(TransactionStage.INITIALIZED)

        disk_filter = self._disk_filter if self._disk_filter else default_disk_filter
        domain_disks = self._domain_xmltree.xpath('/domain/devices/disk')

        self._snapshot_disks = []
        for disk in domain_disks:
            if disk_filter(disk):
                self._snapshot_disks.append(disk)
                logger.info('Accept disk %s', LazyDiskString(disk))

        self._snapshot_xmltree, self._snapshot_flags = self._prepare_snapshot(self._snapshot_disks)
        self._snapshot_xml = etree.tostring(self._snapshot_xmltree, encoding=str)

        self._stage = TransactionStage.PREPARED
        logger.debug('stage changed to PREPARED')

    def begin(self):
        self._check_stage(TransactionStage.PREPARED)

        try:
            logger.debug(
                'Snapshot XML: %s',
                LazyString(lambda: etree.tostring(self._snapshot_xmltree, encoding=str, pretty_print=True))
            )

            self._domain.snapshotCreateXML(self._snapshot_xml, self._snapshot_flags)
            self._stage = TransactionStage.BEGUN
            logger.debug('stage changed to BEGUN')
        except BaseException as ex:
            logger.info(ex)
            self._stage = TransactionStage.FAILED
            logger.debug('stage changed to FAILED')
            raise

    def commit(self):
        self._check_stage(TransactionStage.BEGUN)

        try:
            self._stage = TransactionStage.COMMITTING
            logger.debug('stage changed to COMMITTING')

            active = self._domain.isActive()
            deleting_files = []

            snap_disks = self._snapshot_xmltree.xpath('/domainsnapshot/disks/disk')

            for disk in snap_disks:
                device_name = disk.xpath('string(@name)')
                live_file = disk.xpath('string(source/@file)')
                bandwidth = 0

                # Block job flags
                flags = libvirt.VIR_DOMAIN_BLOCK_COMMIT_SHALLOW

                if active:
                    # No idea why need this flag for an active domain?
                    flags |= libvirt.VIR_DOMAIN_BLOCK_COMMIT_ACTIVE

                logger.info('blockCommit: device %s with bandwith %d and flags %d', device_name, bandwidth, flags)

                self._domain.blockCommit(device_name, None, live_file, bandwidth, flags)

                while True:
                    info = self._domain.blockJobInfo(device_name, 0)

                    if not len(info):
                        # Finish this disk
                        break

                    if info['cur'] == info['end']:
                        self._domain.blockJobAbort(device_name, libvirt.VIR_DOMAIN_BLOCK_JOB_ABORT_PIVOT)
                        logger.info('blockJobAbort: device %s with pivot', device_name)
                        break

                    logger.debug('blockJobInfo: device %s progress %d/%d (%.2f)', device_name, info['cur'], info['end'],
                                 info['cur'] / info['end'])

                    time.sleep(10)
                deleting_files.append(live_file)

            self._stage = TransactionStage.FINISHED
            logger.debug('stage changed to FINISHED')
        except BaseException:
            self._stage = TransactionStage.FAILED
            logger.debug('stage changed to FAILED')
            raise

        exceptions = []

        for file in deleting_files:
            try:
                os.remove(file)
            except (OSError, RuntimeError) as ex:
                exceptions.append(ex)

        if exceptions:
            raise RuntimeError('Failed in deleting {} files'.format(len(exceptions))) from exceptions[0]


def flags_to_names(prefix, flags):
    all_names = filter(lambda s: s.startswith(prefix), dir(libvirt))

    used_names = []

    for name in all_names:
        flag = getattr(libvirt, name)
        if flag & flags == flag:
            used_names.append(name)

    return used_names


__all__ = ['VMTransaction', 'BadStageError', 'SnapshotDisk', 'default_disk_filter']
