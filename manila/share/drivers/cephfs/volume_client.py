# Copyright (c) 2015 Red Hat, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


import json
import logging
import os

import rados
import cephfs

# Generate missing lib errors at load time, rather than the
# first time someone tries to use the FS
try:
    cephfs.load_libcephfs()
except EnvironmentError as e:
    raise ImportError(e.__str__())

from ceph_argparse import json_command
import time


class RadosError(Exception):
    """
    Something went wrong talking to Ceph with librados
    """
    pass


RADOS_TIMEOUT = 10

log = logging.getLogger(__name__)


# TODO support multiple ceph clusters
# TODO get ceph auth info from config database

class CephFSVolumeClient(object):
    """
    Combine libcephfs and librados interfaces to implement a
    'Volume' concept implemented as a cephfs directory and
    client capabilities which restrict mount access to this
    directory.
    """

    # Where shall we create our volumes?
    VOLUME_PREFIX = "/volumes"
    RADOS_NAME = "client.manila"
    CLUSTER_NAME = 'ceph'
    POOL_PREFIX = "manila_"

    def __init__(self):
        self.fs = None
        self.rados = None
        self.connected = False

    def connect(self):
        # TODO: optionally evict any other CephFS client with my client ID
        # so that a hard restart of manila doesn't have to wait for its
        # previous client instance's session to time out.
        log.debug("Connecting to RADOS...")
        self.rados = rados.Rados(
            name=self.RADOS_NAME,
            clustername=self.CLUSTER_NAME,
            conffile='',
            conf={
                'keyring': "./keyring.manila"
            }
        )
        self.rados.connect()

        log.debug("Connection to RADOS complete")
        log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.rados)
        self.fs.mount()
        log.debug("Connection to cephfs complete")

    def get_mon_addrs(self):
        log.info("get_mon_addrs")
        result = []
        mon_map = self._rados_command("mon dump")
        for mon in mon_map['mons']:
            ip_port = mon['addr'].split("/")[0]
            result.append(ip_port)

        return result

    def disconnect(self):
        log.info("disconnect")
        if self.fs:
            time.sleep(5)
            log.debug("Disconnecting cephfs...")
            self.fs.shutdown()
            self.fs = None
            log.debug("Disconnecting cephfs complete")

        if self.rados:
            time.sleep(5)
            log.debug("Disconnecting rados...")
            self.rados.shutdown()
            self.rados = None
            log.debug("Disconnecting rados complete")

        time.sleep(5)

    def __del__(self):
        self.disconnect()

    def _get_pool_id(self, osd_map, pool_name):
        # Maybe borrow the OSDMap wrapper class from calamari if more helpers like this aren needed.
        for pool in osd_map['pools']:
            if pool['pool_name'] == pool_name:
                return pool['pool']

        return None

    def _create_volume_pool(self, pool_name):
        """
        Idempotently create a pool for use as a CephFS data pool, with the given name

        :return The ID of the created pool
        """
        osd_map = self._rados_command('osd dump', {})

        existing_id = self._get_pool_id(osd_map, pool_name)
        if existing_id is not None:
            log.info("Pool {0} already exists".format(pool_name))
            return existing_id

        osd_count = len(osd_map['osds'])

        # We can't query the actual cluster config remotely, but since this is
        # just a heuristic we'll assume that the ceph.conf we have locally reflects
        # that in use in the rest of the cluster.
        pg_warn_max_per_osd = int(self.rados.conf_get('mon_pg_warn_max_per_osd'))

        other_pgs = 0
        for pool in osd_map['pools']:
            if not pool['pool_name'].startswith(self.POOL_PREFIX):
                other_pgs += pool['pg_num']

        # A basic heuristic for picking pg_num: work out the max number of
        # PGs we can have without tripping a warning, then subtract the number
        # of PGs already created by non-manila pools, then divide by ten.  That'll
        # give you a reasonable result on a system where you have "a few" manila
        # shares.
        pg_num = ((pg_warn_max_per_osd * osd_count) - other_pgs) / 10
        # TODO Alternatively, respect an override set by the user.

        self._rados_command(
            'osd pool create',
            {
                'pool': pool_name,
                'pg_num': pg_num
            }
        )

        osd_map = self._rados_command('osd dump', {})
        pool_id = self._get_pool_id(osd_map, pool_name)

        if pool_id is None:
            # If the pool isn't there, that's either a ceph bug, or it's some outside influence
            # removing it right after we created it.
            log.error("OSD map doesn't contain expected pool '{0}':\n{1}".format(
                pool_name, json.dumps(osd_map, indent=2)
            ))
            raise RuntimeError("Pool '{0}' not present in map after creation".format(pool_name))
        else:
            return pool_id

    def create_volume(self, volume_name, size=None, data_isolated=False):
        """
        Set up metadata, pools and auth for a volume.

        This function is idempotent.  It is safe to call this again
        for an already-created volume, even if it is in use.

        :param volume_name: Any string.  Must be unique per volume.
        :param size: In bytes, or None for no size limit
        :param data_isolated: If true, create a separate OSD pool for this volume
        :return:
        """
        log.info("create_volume: {0}".format(volume_name))
        try:
            self.fs.stat(self.VOLUME_PREFIX)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(self.VOLUME_PREFIX, 0755)

        client_entity = "client.{0}".format(volume_name)

        volume_path = os.path.join(self.VOLUME_PREFIX, volume_name)

        caps = self._rados_command(
            'auth get-or-create',
            {
                'entity': client_entity,
                'caps': [
                    'mds', 'allow rw path={0}'.format(volume_path),
                    'osd', 'allow rw',
                    'mon', 'allow r']
            })
        # Result expected like this:
        # [
        #     {
        #         "entity": "client.foobar",
        #         "key": "AQBY0\/pViX\/wBBAAUpPs9swy7rey1qPhzmDVGQ==",
        #         "caps": {
        #             "mds": "allow *",
        #             "mon": "allow *"
        #         }
        #     }
        # ]

        assert len(caps) == 1
        assert caps[0]['entity'] == client_entity
        volume_key = caps[0]['key']

        try:
            self.fs.stat(volume_path)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(os.path.join(self.VOLUME_PREFIX, volume_name), 0755)
        else:
            log.warning("Volume {0} already exists".format(volume_name))

        if size is not None:
            self.fs.setxattr(volume_path, 'ceph.quota.max_bytes', size.__str__(), 0)

        if data_isolated:
            pool_name = "{0}{1}".format(self.POOL_PREFIX, volume_name)
            pool_id = self._create_volume_pool(pool_name)
            mds_map = self._rados_command("mds dump", {})
            if pool_id not in mds_map['data_pools']:
                self._rados_command("mds add_data_pool", {
                    'pool': pool_name
                })
            self.fs.setxattr(volume_path, 'ceph.dir.layout.pool', pool_name, 0)

        return {
            'volume_name': volume_name,
            'volume_key': volume_key,
            'mount_path': volume_path
        }

    def delete_volume(self, volume_name, data_isolated=False):
        """
        Remove all trace of a volume from the Ceph cluster.  This function is
        idempotent.

        :param volume_name: Same name used in create_volume
        :return:
        """

        # TODO: evict any clients that were using this: deleting the auth key
        # will stop new clients connecting, but it doesn't guarantee that any
        # existing clients have gone.
        log.info("delete_volume: {0}".format(volume_name))

        client_entity = "client.{0}".format(volume_name)

        # Remove the auth key for this volume
        self._rados_command('auth del', {'entity': client_entity}, decode=False)

        # Create the trash folder if it doesn't already exist
        trash = os.path.join(self.VOLUME_PREFIX, "_deleting")
        try:
            self.fs.stat(trash)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(trash, 0755)

        # We'll move it to here
        trashed_volume = os.path.join(trash, volume_name)

        # Move the volume's data to the trash folder
        volume_path = os.path.join(self.VOLUME_PREFIX, volume_name)
        try:
            self.fs.stat(volume_path)
        except cephfs.ObjectNotFound:
            log.warning("Trying to delete volume '{0}' but it's already gone".format(
                volume_path))
        else:
            self.fs.rename(volume_path, trashed_volume)

        try:
            self.fs.stat(trashed_volume)
        except cephfs.ObjectNotFound:
            log.warning("Trying to purge volume '{0}' but it's already been purged".format(
                trashed_volume))
            return

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            dir_handle = self.fs.opendir(root_path)
            d = self.fs.readdir(dir_handle)
            while d:
                if d.d_name not in [".", ".."]:
                    d_full = os.path.join(root_path, d.d_name)
                    if d.is_dir():
                        rmtree(d_full)
                    else:
                        self.fs.unlink(d_full)

                d = self.fs.readdir(dir_handle)
            self.fs.closedir(dir_handle)

            self.fs.rmdir(root_path)

        rmtree(trashed_volume)

        if data_isolated:
            pool_name = "{0}{1}".format(self.POOL_PREFIX, volume_name)
            osd_map = self._rados_command("osd dump", {})
            pool_id = self._get_pool_id(osd_map, pool_name)
            mds_map = self._rados_command("mds dump", {})
            if pool_id in mds_map['data_pools']:
                self._rados_command("mds remove_data_pool", {
                    'pool': pool_name
                })
            self._rados_command("osd pool delete",
                                {
                                    "pool": pool_name,
                                    "pool2": pool_name,
                                    "sure": "--yes-i-really-really-mean-it"
                                })

    def _rados_command(self, prefix, args=None, decode=True):
        """
        Safer wrapper for ceph_argparse.json_command, which raises
        Error exception instead of relying on caller to check return
        codes.

        Error exception can result from:
        * Timeout
        * Actual legitimate errors
        * Malformed JSON output

        return: Decoded object from ceph, or None if empty string returned.
                If decode is False, return a string (the data returned by
                ceph command)
        """
        if args is None:
            args = {}

        argdict = args.copy()
        argdict['format'] = 'json'

        ret, outbuf, outs = json_command(self.rados,
                                         prefix=prefix,
                                         argdict=argdict,
                                         timeout=RADOS_TIMEOUT)
        if ret != 0:
            raise rados.Error(outs)
        else:
            if decode:
                if outbuf:
                    try:
                        return json.loads(outbuf)
                    except (ValueError, TypeError):
                        raise RadosError("Invalid JSON output for command {0}".format(argdict))
                else:
                    return None
            else:
                return outbuf

    def get_used_bytes(self, volume_name):
        volume_path = os.path.join(self.VOLUME_PREFIX, volume_name)
        return int(self.fs.getxattr(volume_path, "ceph.dir.rbytes"))

    def set_max_bytes(self, volume_name, max_bytes):
        volume_path = os.path.join(self.VOLUME_PREFIX, volume_name)
        self.fs.setxattr(volume_path, 'ceph.quota.max_bytes',
                         max_bytes.__str__() if max_bytes is not None else "0",
                         0)

if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())

    import sys
    vc = CephFSVolumeClient()
    vc.connect()
    vc.create_volume(sys.argv[1])
    vc.disconnect()