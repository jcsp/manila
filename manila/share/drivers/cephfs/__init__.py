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

import manila.exception as exception
from manila.share import driver



from oslo_log import log
log = log.getLogger(__name__)


# TODO consistency groups: use a parent dir to define consistency group, then
# directory within that for each share?


class CephFSNativeDriver(driver.ShareDriver,):
    """
    This driver is 'native' in the sense that it exposes a CephFS filesystem
    for use directly by guests, with no intermediate layer.
    """
    supported_protocols = ('CEPHFS',)

    driver_handles_share_servers = True

    def _to_bytes(self, gigs):
        """
        Convert a Manila size into bytes.  Manila uses gigs everywhere.  If the input
        is None, return None.

        :param gigs: integer number of gigabytes
        :return: integer number of bytes
        """
        if gigs is not None:
            return gigs * 1024 * 1024 * 1024
        else:
            return None

    @property
    def volume_client(self):
        if self._volume_client:
            return self._volume_client

        try:
            from volume_client import CephFSVolumeClient
        except ImportError as e:
            raise exception.ManilaException(
                "Ceph client libraries not found: {0}".format(
                    e
                )
            )
        else:
            self._volume_client = CephFSVolumeClient()
            log.info("Ceph client found, connecting...")
            self._volume_client.connect()
            log.info("Ceph client connection complete")

            return self._volume_client

    def __init__(self, *args, **kwargs):
        super(CephFSNativeDriver, self).__init__(
            True, *args, **kwargs)
        self.backend_name = self.configuration.safe_get(
            'share_backend_name') or 'CephFS-Native'

        self._volume_client = None

    def create_share(self, context, share, share_server=None):
        """

        :param context: A RequestContext
        :param share: A ShareInstance
        :param share_server: Always None for CephFS native
        :return:
        """
        assert share_server is None

        # `share` is a ShareInstance
        log.info("create_share name={0} size={1} cg_id={2}".format(
            share['share_id'], share['size'], share['consistency_group_id']))

        name = share['share_id']
        size = self._to_bytes(share['size'])

        volume = self.volume_client.create_volume(volume_name=name, size=size, data_isolated=True)

        # To mount this you need to know the mon IPs, the volume name, and they key
        key = volume['volume_key']
        mon_addrs = self.volume_client.get_mon_addrs()

        export_location = "{0}:{1}:{2}".format(
            ",".join(mon_addrs),
            volume['mount_path'],
            key)

        log.info("Calculated export location: {0}".format(export_location))

        return export_location

    def delete_share(self, context, share, share_server=None):
        self.volume_client.delete_volume(share['share_id'], data_isolated=True)

    def ensure_share(self, context, share, share_server=None):
        # Creation is idempotent
        assert share is not None
        return self.create_share(context, share, share_server)

    def extend_share(self, share, new_size, share_server=None):
        log.info("extend_share {0} {1}".format(share['share_id'], new_size))
        self.volume_client.set_max_bytes(share['share_id'], self._to_bytes(new_size))

    def shrink_share(self, share, new_size, share_server=None):
        log.info("shrink_share {0} {1}".format(share['share_id'], new_size))
        new_bytes = self._to_bytes(new_size)
        used = self.volume_client.get_used_bytes(share['share_id'])
        if used > new_bytes:
            # While in fact we can "shrink" our volumes to less than their
            # used bytes (it's just a quota), raise error anyway to avoid
            # confusing API consumers that might depend on typical shrink
            # behaviour.
            raise exception.ShareShrinkingPossibleDataLoss()

        self.volume_client.set_max_bytes(share['share_id'], new_bytes)

    def __del__(self):
        if self._volume_client:
            self._volume_client.disconnect()
            self._volume_client = None
