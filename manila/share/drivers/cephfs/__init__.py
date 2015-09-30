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


from oslo_log import log
from oslo_config import cfg

import manila.exception as exception
from manila.share import driver



log = log.getLogger(__name__)


cephfs_native_opts = [
    cfg.StrOpt('cephfs_conf_path',
               default="",
               help=""),
    cfg.StrOpt('cephfs_cluster_name',
               default=None,
               help="The name of the cluster in use, if it is not the default ('ceph')"
               )
]


CONF = cfg.CONF
CONF.register_opts(cephfs_native_opts)


class CephFSNativeDriver(driver.ShareDriver,):
    """
    This driver is 'native' in the sense that it exposes a CephFS filesystem
    for use directly by guests, with no intermediate layer.
    """

    supported_protocols = ('CEPHFS',)

    driver_handles_share_servers = True

    def get_share_stats(self, refresh=False):
        data = super(CephFSNativeDriver, self).get_share_stats(refresh)
        data['consistency_group_support'] = 'pool'
        data['vendor_name'] = 'Red Hat'
        data['driver_version'] = '1.0'
        return data

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
            conf_path = self.configuration.safe_get('cephfs_conf_path')
            cluster_name = self.configuration.safe_get('cephfs_cluster_name')
            self._volume_client = CephFSVolumeClient(conf_path, cluster_name)
            log.info("Ceph client found, connecting...")
            try:
                self._volume_client.connect()
            except Exception:
                self._volume_client = None
                raise
            else:
                log.info("Ceph client connection complete")

            return self._volume_client

    def __init__(self, *args, **kwargs):
        super(CephFSNativeDriver, self).__init__(
            True, *args, **kwargs)
        self.backend_name = self.configuration.safe_get(
            'share_backend_name') or 'CephFS-Native'

        self._volume_client = None

        self.configuration.append_config_values(cephfs_native_opts)

    def _share_path(self, share):
        """
        Get VolumePath from ShareInstance
        """
        from manila.share.drivers.cephfs.volume_client import VolumePath
        return VolumePath(share['consistency_group_id'], share['share_id'])

    def _teardown_server(self, server_details, security_services=None):
        log.warning("Skipping teardown_server, this driver doesn't use that")

    def _setup_server(self, network_info, metadata=None):
        log.warning("Skipping teardown_server, this driver doesn't use that")

    def create_share(self, context, share, share_server=None):
        """

        :param context: A RequestContext
        :param share: A ShareInstance
        :param share_server: Always None for CephFS native
        :return:
        """

        if share_server is not None:
            log.warning("You specified a share server, but this driver doesn't use that")

        # `share` is a ShareInstance
        log.info("create_share name={0} size={1} cg_id={2}".format(
            share['share_id'], share['size'], share['consistency_group_id']))

        # TODO: resolve share_type_id to a sharetype and look at its KVs
        # to decide whether it should be data isolated.

        size = self._to_bytes(share['size'])

        volume = self.volume_client.create_volume(
            self._share_path(share), size=size, data_isolated=True)

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
        self.volume_client.delete_volume(self._share_path(share), data_isolated=True)

    def ensure_share(self, context, share, share_server=None):
        # Creation is idempotent
        assert share is not None
        return self.create_share(context, share, share_server)

    def extend_share(self, share, new_size, share_server=None):
        log.info("extend_share {0} {1}".format(share['share_id'], new_size))
        self.volume_client.set_max_bytes(self._share_path(share), self._to_bytes(new_size))

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

        self.volume_client.set_max_bytes(self._share_path(share), new_bytes)

    def create_snapshot(self, context, snapshot, share_server=None):
        self.volume_client.create_snapshot_volume(
            self._share_path(snapshot['share']),
            snapshot['name'])

    def delete_snapshot(self, context, snapshot, share_server=None):
        # We assign names to created snapshots
        assert snapshot['name'] is not None

        self.volume_client.destroy_snapshot_volume(self._share_path(snapshot['share']), snapshot['name'])
        return None

    def create_share_from_snapshot(self, context, share, snapshot, share_server=None):
        # TODO
        # Create the new share as requested

        # cp -r from the snapshot to the new share

        raise NotImplementedError()


    def create_consistency_group_from_cgsnapshot(self, context, cg_dict, cgsnapshot_dict, share_server=None):
        # TODO
        raise NotImplementedError()

    def create_consistency_group(self, context, cg_dict, share_server=None):
        self.volume_client.create_group(cg_dict['id'])

    def delete_consistency_group(self, context, cg_dict, share_server=None):
        self.volume_client.destroy_group(cg_dict['id'])

    def delete_cgsnapshot(self, context, snap_dict, share_server=None):
        self.volume_client.destroy_snapshot_group(
            snap_dict['consistency_group_id'],
            snap_dict['name']
        )

    def create_cgsnapshot(self, context, snap_dict, share_server=None):
        self.volume_client.create_snapshot_group(
            snap_dict['id'],
            snap_dict['name'])

        return None, []

    def __del__(self):
        if self._volume_client:
            self._volume_client.disconnect()
            self._volume_client = None

    def get_network_allocations_number(self):
        # I consume no manila-tracked network resources.
        return 0

    # TODO: create a periodic hook for purging deleted volumes, and move the
    # purging out of the delete_share path.

    # TODO: advertise pool capabilities:
    #  * Support for share types with data isolation
    #  * Support for share types that specify an existing data pool
