# Copyright (c) 2014 Red Hat, Inc.
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

""" GlusterFS native protocol (glusterfs) driver for shares.

Test cases for GlusterFS native protocol driver.
"""

import ddt
import mock
from oslo_config import cfg

from manila.common import constants
from manila import context
from manila import exception
from manila.share import configuration as config
from manila.share.drivers.glusterfs import common
from manila.share.drivers import glusterfs_native
from manila import test
from manila.tests import fake_utils


CONF = cfg.CONF


def new_share(**kwargs):
    share = {
        'id': 'fakeid',
        'name': 'fakename',
        'size': 1,
        'share_proto': 'glusterfs',
    }
    share.update(kwargs)
    return share


@ddt.ddt
class GlusterfsNativeShareDriverTestCase(test.TestCase):
    """Tests GlusterfsNativeShareDriver."""

    def setUp(self):
        super(GlusterfsNativeShareDriverTestCase, self).setUp()
        fake_utils.stub_out_utils_execute(self)
        self._execute = fake_utils.fake_execute
        self._context = context.get_admin_context()

        self.glusterfs_target1 = 'root@host1:/gv1'
        self.glusterfs_target2 = 'root@host2:/gv2'
        self.glusterfs_server1 = 'root@host1'
        self.glusterfs_server2 = 'root@host2'
        self.glusterfs_server1_volumes = 'manila-share-1-1G\nshare1'
        self.glusterfs_server2_volumes = 'manila-share-2-2G\nshare2'
        self.share1 = new_share(
            export_location=self.glusterfs_target1,
            status=constants.STATUS_AVAILABLE)
        self.share2 = new_share(
            export_location=self.glusterfs_target2,
            status=constants.STATUS_AVAILABLE)
        self.gmgr1 = common.GlusterManager(self.glusterfs_server1,
                                           self._execute, None, None,
                                           requires={'volume': False})
        self.gmgr2 = common.GlusterManager(self.glusterfs_server2,
                                           self._execute, None, None,
                                           requires={'volume': False})
        self.glusterfs_volumes_dict = (
            {'root@host1:/manila-share-1-1G': {'size': 1},
             'root@host2:/manila-share-2-2G': {'size': 2}})
        self.glusterfs_used_vols = set([
            'root@host1:/manila-share-1-1G',
            'root@host2:/manila-share-2-2G'])

        CONF.set_default('glusterfs_volume_pattern',
                         'manila-share-\d+-#{size}G$')
        CONF.set_default('driver_handles_share_servers', False)

        self.fake_conf = config.Configuration(None)
        self.mock_object(common.GlusterManager, 'make_gluster_call')

        self._driver = glusterfs_native.GlusterfsNativeShareDriver(
            execute=self._execute,
            configuration=self.fake_conf)
        self.addCleanup(fake_utils.fake_execute_set_repliers, [])
        self.addCleanup(fake_utils.fake_execute_clear_log)

    def test_supported_protocols(self):
        self.assertEqual(('GLUSTERFS', ),
                         self._driver.supported_protocols)

    def test_setup_via_manager(self):
        gmgr = mock.Mock()
        gmgr.gluster_call = mock.Mock()
        gmgr.volume = 'fakevol'
        gmgr.get_gluster_vol_option = mock.Mock(
            return_value='glusterfs-server-name,some-other-name')

        self._driver._setup_via_manager(gmgr)

        gmgr.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        args = (
            ('volume', 'set', 'fakevol', 'nfs.export-volumes', 'off'),
            ('volume', 'set', 'fakevol', 'client.ssl', 'on'),
            ('volume', 'set', 'fakevol', 'server.ssl', 'on'),
            ('volume', 'stop', 'fakevol', '--mode=script'),
            ('volume', 'start', 'fakevol'))
        gmgr.gluster_call.assert_has_calls([mock.call(*a) for a in args])

    def test_setup_via_manager_with_parent(self):
        gmgr = mock.Mock()
        gmgr.gluster_call = mock.Mock()
        gmgr.volume = 'fakevol'
        gmgr_parent = mock.Mock()
        gmgr_parent.get_gluster_vol_option = mock.Mock(
            return_value='glusterfs-server-name,some-other-name')

        self._driver._setup_via_manager(gmgr, gmgr_parent)

        gmgr_parent.get_gluster_vol_option.assert_called_once_with(
            'auth.ssl-allow')
        args = (
            ('volume', 'set', 'fakevol', 'auth.ssl-allow',
             'glusterfs-server-name'),
            ('volume', 'set', 'fakevol', 'nfs.export-volumes', 'off'),
            ('volume', 'set', 'fakevol', 'client.ssl', 'on'),
            ('volume', 'set', 'fakevol', 'server.ssl', 'on'),
            ('volume', 'start', 'fakevol'))
        gmgr.gluster_call.assert_has_calls([mock.call(*a) for a in args])

    @ddt.data(True, False)
    def test_setup_via_manager_no_option_data(self, has_parent):
        gmgr = mock.Mock()
        if has_parent:
            gmgr_parent = mock.Mock()
            gmgr_queried = gmgr_parent
        else:
            gmgr_parent = None
            gmgr_queried = gmgr
        gmgr_queried.get_gluster_vol_option = mock.Mock(return_value='')

        self.assertRaises(exception.GlusterfsException,
                          self._driver._setup_via_manager,
                          gmgr, gluster_mgr_parent=gmgr_parent)

        gmgr_queried.get_gluster_vol_option.assert_called_once_with(
            'auth.ssl-allow')

    @ddt.data(exception.ProcessExecutionError, RuntimeError)
    def test_setup_via_manager_exception(self, _exception):
        gmgr = mock.Mock()
        gmgr.gluster_call = mock.Mock(side_effect=_exception)
        gmgr.get_gluster_vol_option = mock.Mock()

        self.assertRaises(
            {exception.ProcessExecutionError:
             exception.GlusterfsException}.get(
                _exception, _exception), self._driver._setup_via_manager, gmgr)

    def test_snapshots_are_supported(self):
        self.assertTrue(self._driver.snapshots_are_supported)

    def test_allow_access_via_manager(self):
        common._restart_gluster_vol = mock.Mock()
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(gmgr1, 'get_gluster_vol_option',
                         mock.Mock(return_value='some.common.name'))
        test_args = ('volume', 'set', 'gv1', 'auth.ssl-allow',
                     'some.common.name,' + access['access_to'])

        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])

        self._driver._allow_access_via_manager(gmgr1, self._context,
                                               self.share1, access)
        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        gmgr1.gluster_call.assert_called_once_with(*test_args)
        common._restart_gluster_vol.assert_called_once_with(gmgr1)

    def test_allow_access_via_manager_with_share_having_access(self):
        common._restart_gluster_vol = mock.Mock()
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(
            gmgr1, 'get_gluster_vol_option',
            mock.Mock(return_value='some.common.name,' + access['access_to']))

        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])

        self._driver._allow_access_via_manager(gmgr1, self._context,
                                               self.share1, access)
        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        self.assertFalse(gmgr1.gluster_call.called)
        self.assertFalse(common._restart_gluster_vol.called)

    def test_allow_access_via_manager_invalid_access_type(self):
        common._restart_gluster_vol = mock.Mock()
        access = {'access_type': 'invalid', 'access_to': 'client.example.com'}
        expected_exec = []

        self.assertRaises(exception.InvalidShareAccess,
                          self._driver._allow_access_via_manager,
                          self.gmgr1, self._context, self.share1, access)

        self.assertFalse(common._restart_gluster_vol.called)
        self.assertEqual(expected_exec, fake_utils.fake_execute_get_log())

    def test_allow_access_via_manager_excp(self):
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        test_args = ('volume', 'set', 'gv1', 'auth.ssl-allow',
                     'some.common.name,' + access['access_to'])

        def raise_exception(*args, **kwargs):
            if (args == test_args):
                raise exception.ProcessExecutionError()

        common._restart_gluster_vol = mock.Mock()
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(gmgr1, 'get_gluster_vol_option',
                         mock.Mock(return_value='some.common.name'))
        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])
        self.mock_object(gmgr1, 'gluster_call',
                         mock.Mock(side_effect=raise_exception))

        self.assertRaises(exception.GlusterfsException,
                          self._driver._allow_access_via_manager, gmgr1,
                          self._context, self.share1, access)

        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        gmgr1.gluster_call.assert_called_once_with(*test_args)
        self.assertFalse(common._restart_gluster_vol.called)

    def test_deny_access_via_manager(self):
        common._restart_gluster_vol = mock.Mock()
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(
            gmgr1, 'get_gluster_vol_option',
            mock.Mock(return_value='some.common.name,' + access['access_to']))
        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])

        self._driver._deny_access_via_manager(gmgr1, self._context,
                                              self.share1, access)

        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        test_args = ('volume', 'set', 'gv1', 'auth.ssl-allow',
                     'some.common.name')
        gmgr1.gluster_call.assert_called_once_with(*test_args)
        common._restart_gluster_vol.assert_called_once_with(gmgr1)

    def test_deny_access_via_manager_with_share_having_no_access(self):
        common._restart_gluster_vol = mock.Mock()
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(gmgr1, 'get_gluster_vol_option',
                         mock.Mock(return_value='some.common.name'))
        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])

        self._driver._deny_access_via_manager(gmgr1, self._context,
                                              self.share1, access)

        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        self.assertFalse(gmgr1.gluster_call.called)
        self.assertFalse(common._restart_gluster_vol.called)

    def test_deny_access_via_manager_invalid_access_type(self):
        common._restart_gluster_vol = mock.Mock()

        access = {'access_type': 'invalid', 'access_to': 'NotApplicable'}
        self.assertRaises(exception.InvalidShareAccess,
                          self._driver._deny_access_via_manager, self.gmgr1,
                          self._context, self.share1, access)

        self.assertFalse(common._restart_gluster_vol.called)

    def test_deny_access_via_manager_excp(self):
        access = {'access_type': 'cert', 'access_to': 'client.example.com'}
        test_args = ('volume', 'set', 'gv1', 'auth.ssl-allow',
                     'some.common.name')

        def raise_exception(*args, **kwargs):
            if (args == test_args):
                raise exception.ProcessExecutionError()

        common._restart_gluster_vol = mock.Mock()
        gmgr1 = common.GlusterManager(self.glusterfs_target1, self._execute,
                                      None, None)
        self.mock_object(
            gmgr1, 'get_gluster_vol_option',
            mock.Mock(return_value='some.common.name,' + access['access_to']))
        self._driver.layout.gluster_used_vols = set([self.glusterfs_target1])

        self.mock_object(gmgr1, 'gluster_call',
                         mock.Mock(side_effect=raise_exception))

        self.assertRaises(exception.GlusterfsException,
                          self._driver._deny_access_via_manager, gmgr1,
                          self._context, self.share1, access)

        gmgr1.get_gluster_vol_option.assert_called_once_with('auth.ssl-allow')
        gmgr1.gluster_call.assert_called_once_with(*test_args)
        self.assertFalse(common._restart_gluster_vol.called)

    def test_update_share_stats(self):
        self._driver._update_share_stats()

        test_data = {
            'share_backend_name': 'GlusterFS-Native',
            'driver_handles_share_servers': False,
            'vendor_name': 'Red Hat',
            'driver_version': '1.1',
            'storage_protocol': 'glusterfs',
            'reserved_percentage': 0,
            'QoS_support': False,
            'total_capacity_gb': 'unknown',
            'free_capacity_gb': 'unknown',
            'pools': None,
            'snapshot_support': True,
        }
        self.assertEqual(test_data, self._driver._stats)

    def test_get_network_allocations_number(self):
        self.assertEqual(0, self._driver.get_network_allocations_number())
