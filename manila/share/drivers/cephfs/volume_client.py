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

    def __init__(self):
        self.fs = None
        self.rados = None
        self.connected = False

    def connect(self):
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

    def create_volume(self, volume_name):
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
            return None

        return {
            'volume_name': volume_name,
            'volume_key': volume_key,
            'mount_path': volume_path
        }

    def delete_volume(self, volume_name):
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

        # Move the volume's data to the trash folder
        volume_path = os.path.join(self.VOLUME_PREFIX, volume_name)
        try:
            self.fs.stat(volume_path)
        except cephfs.ObjectNotFound:
            log.warning("Trying to delete volume '{0}' but it's already gone".format(
                volume_name
            ))
        else:
            self.fs.rename(volume_path, os.path.join(trash, volume_name))

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

if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())

    import sys
    vc = CephFSVolumeClient()
    vc.connect()
    vc.create_volume(sys.argv[1])
    vc.disconnect()