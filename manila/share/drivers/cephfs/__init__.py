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

from manila.exception import ManilaException
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

    @property
    def volume_client(self):
        if self._volume_client:
            return self._volume_client

        try:
            from volume_client import CephFSVolumeClient
        except ImportError as e:
            raise ManilaException(
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

        # TODO carry forward e.g. project_id from RequestContext into
        # some metadata in Ceph and/or metadata for the client mount?

        # `share` is a ShareInstance
        log.info("create_share {0}".format(share['name']))

        name = share['name']
        volume = self.volume_client.create_volume(name)


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
        self.volume_client.delete_volume(share['name'])
        # TODO: clear out the _deleting directory in the background (rm -rf
        # is potentially pretty slow).

    def ensure_share(self, context, share, share_server=None):
        # TODO lets check the dir an auth exist
        pass

    def __del__(self):
        if self.volume_client:
            self.volume_client.disconnect()
            self.volume_client = None
