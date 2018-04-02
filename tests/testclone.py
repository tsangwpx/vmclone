from subprocess import check_call
import vmclone
import libvirt
import time

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    conn = libvirt.open('qemu:///system')
    domain = conn.lookupByName('debianVM')
    vmt = vmclone.VMTransaction(domain)

    try:
        logger.info('State: %s', vmt.stage)
        vmt.initialize()
        vmt.prepare()
        vmt.begin()

        logger.info('State: %s', vmt.stage)
        for x in range(5):
            print(x)
            time.sleep(1)

        vmt.commit()
        logger.info('State: %s', vmt.stage)
    except BaseException as ex:
        logger.info(ex)
        raise


if __name__ == '__main__':
    main()
