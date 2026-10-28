import os
import stat

from typing import Any
from unittest.mock import patch

from patroni.config import Config
from patroni.dcs import Leader, Member
from patroni.exceptions import ConfigParseError, PatroniException
from patroni.file_perm import pg_perm
from patroni.postgresql.mpp import AbstractMPP, get_mpp, Null
from patroni.postgresql.mpp.greengage import get_gp_role, Greengage

from . import BaseTestPostgresql
from .test_ha import get_cluster_initialized_with_leader


class TestMPP(BaseTestPostgresql):

    def setUp(self):
        super(TestMPP, self).setUp()
        self.cluster = get_cluster_initialized_with_leader()

    def test_get_handler_impl_exception(self):
        class DummyMPP(AbstractMPP):
            def __init__(self) -> None:
                super().__init__({})

            @staticmethod
            def validate_config(config: Any) -> bool:
                return True

            @property
            def group(self) -> None:
                return None

            @property
            def coordinator_group_id(self) -> None:
                return None

            @property
            def type(self) -> str:
                return "dummy"

        mpp = DummyMPP()
        self.assertRaises(PatroniException, mpp.get_handler_impl, self.p)

    def test_null_handler(self):
        config = {}
        mpp = get_mpp(config)
        self.assertIsInstance(mpp, Null)
        self.assertIsNone(mpp.group)
        self.assertTrue(mpp.validate_config(config))
        nullHandler = mpp.get_handler_impl(self.p)
        self.assertIsNone(nullHandler.handle_event(self.cluster, {}))
        self.assertIsNone(nullHandler.sync_meta_data(self.cluster))
        self.assertIsNone(nullHandler.on_demote())
        self.assertIsNone(nullHandler.schedule_cache_rebuild())
        self.assertIsNone(nullHandler.bootstrap())
        self.assertIsNone(nullHandler.adjust_postgres_gucs({}))
        self.assertEqual(nullHandler.cmdline_options(), [])
        self.assertEqual(nullHandler.replication_application_name(), self.p.name)
        self.assertIsNone(nullHandler.before_follow(self.cluster.leader))
        self.assertEqual(nullHandler.basebackup_options(self.cluster.leader), [])
        self.assertFalse(nullHandler.ignore_replication_slot({}))

    def test_greengage_handler(self):
        config = {'greengage': {}}
        mpp = get_mpp(config)
        self.assertIsInstance(mpp, Greengage)
        self.assertTrue(mpp.validate_config(config['greengage']))
        self.assertFalse(mpp.validate_config(None))
        self.assertIsInstance(get_mpp({'greengage': None}), Null)
        self.assertFalse(mpp.validate_config(1))
        # Greengage clusters are not grouped in DCS, therefore MPP is "disabled" from the Patroni point of view
        self.assertFalse(mpp.is_enabled())
        self.assertFalse(mpp.is_coordinator())
        self.assertFalse(mpp.is_worker())
        self.assertIsNone(mpp.group)
        self.assertIsNone(mpp.coordinator_group_id)

        handler = mpp.get_handler_impl(self.p)
        self.assertIsNone(handler.handle_event(self.cluster, {}))
        self.assertIsNone(handler.sync_meta_data(self.cluster))
        self.assertIsNone(handler.on_demote())
        self.assertIsNone(handler.schedule_cache_rebuild())
        self.assertIsNone(handler.bootstrap())
        self.assertFalse(handler.ignore_replication_slot({}))
        self.assertEqual(handler.cmdline_options(), ['--gp_role=dispatch'])

    def test_greengage_replication_application_name(self):
        self.p.mpp_handler = get_mpp({'greengage': {}}).get_handler_impl(self.p)
        self.p._major_version = 120000
        for name in ('mdw', 'smdw'):
            self.p.name = name
            params = self.p.config.build_recovery_params(self.cluster.leader)
            self.assertEqual(params['primary_conninfo']['application_name'], 'gp_walreceiver')
            self.assertIn('application_name=gp_walreceiver',
                          self.p.config.format_dsn(params['primary_conninfo']))
            # Slots still identify the Patroni member, including after a switchover.
            self.assertEqual(params['primary_slot_name'], name)
        self.assertIsNone(self.p.config.primary_conninfo_params(None))
        member = Member(0, self.p.name, None, {'conn_url': 'postgres://localhost:5432/postgres'})
        self.assertIsNone(self.p.config.primary_conninfo_params(member))

    def test_greengage_adjust_postgres_gucs(self):
        handler = get_mpp({'greengage': {}}).get_handler_impl(self.p)

        # max_prepared_transactions is deduced, other GUCs are left to the user
        parameters = {'max_locks_per_transaction': 64, 'max_worker_processes': 8,
                      'max_prepared_transactions': 0, 'max_connections': 150}
        handler.adjust_postgres_gucs(parameters)
        self.assertEqual(parameters, {'max_locks_per_transaction': 64, 'max_worker_processes': 8,
                                      'max_prepared_transactions': 150, 'max_connections': 150})
        # explicitly configured max_prepared_transactions is not touched
        parameters = {'max_prepared_transactions': '200', 'max_connections': '150'}
        handler.adjust_postgres_gucs(parameters)
        self.assertEqual(parameters, {'max_prepared_transactions': '200', 'max_connections': '150'})

    def test_greengage_check_config(self):
        utility = {'username': 'foo', 'options': '-c statement_timeout=0 -c gp_role=utility'}
        # nothing to check without postgresql.data_dir, e.g. in the patronictl configuration
        self.assertIsInstance(get_mpp({'greengage': {}}), Greengage)
        config = Config(None, validator=None).copy()
        config['greengage'] = {}
        self.assertIsInstance(get_mpp(config), Greengage)
        # the superuser is required to use utility mode, the rewind user only if it is configured
        config = {'greengage': {}, 'postgresql': {'data_dir': 'foo', 'authentication': {'superuser': {}}}}
        self.assertRaisesRegex(ConfigParseError, r'in postgresql\.authentication\.superuser: in dispatch mode',
                               get_mpp, config)
        config['postgresql']['authentication']['superuser'] = utility
        self.assertIsInstance(get_mpp(config), Greengage)
        config['postgresql']['authentication']['rewind'] = {'username': 'bar'}
        self.assertRaisesRegex(ConfigParseError, r'in postgresql\.authentication\.rewind: in dispatch mode',
                               get_mpp, config)
        config['postgresql']['authentication']['rewind']['options'] = '--gp_role=utility'
        self.assertIsInstance(get_mpp(config), Greengage)
        # the last gp_role wins
        config['postgresql']['authentication']['superuser'] = {'options': '-c gp_role=utility -cgp_role=dispatch'}
        self.assertRaisesRegex(ConfigParseError, r'in postgresql\.authentication\.superuser: in dispatch mode',
                               get_mpp, config)
        config['postgresql']['authentication']['rewind'] = {'username': 'bar'}
        self.assertRaisesRegex(ConfigParseError, r'in postgresql\.authentication\.superuser and '
                               r'postgresql\.authentication\.rewind: in dispatch mode', get_mpp, config)
        config['postgresql']['authentication'] = {}
        self.assertRaisesRegex(ConfigParseError, r'in postgresql\.authentication\.superuser: in dispatch mode',
                               get_mpp, config)
        self.assertIsNone(get_gp_role(None))
        self.assertIsNone(get_gp_role('-c work_mem=1MB'))

    @patch('patroni.postgresql.mpp.greengage.get_connection_cursor')
    def test_greengage_standby_registration(self, mock_get_cursor):
        handler = get_mpp({'greengage': {}}).get_handler_impl(self.p)
        leader = self.cluster.leader
        internal_auto_conf = os.path.join(self.p.data_dir, 'internal.auto.conf')
        cur = mock_get_cursor.return_value.__enter__.return_value
        # how this node is known to the cluster: host and port from connect_address, absolute data_dir (the one in
        # the test config is relative)
        data_dir = os.path.abspath(self.p.data_dir)
        registered = (8, 'mdw', '127.0.0.2', 5432, data_dir + '/')

        # the promotion of the leader isn't over after the timeout: its row in its catalog is missing or still
        # has role = 'm', even if it is already out of recovery
        with patch('patroni.postgresql.mpp.greengage.time') as mock_time:
            mock_time.time.side_effect = [0, 1, 31]
            cur.fetchone.side_effect = [None, (False,)]
            self.assertIsNone(handler._register_standby(leader))
            mock_time.sleep.assert_called_once_with(1)

        # the connection is a utility mode one: options of the superuser are passed to get_connection_cursor()
        self.p.config.superuser['options'] = '-c gp_role=utility'
        cur.fetchone.side_effect = [(True,), registered]
        self.assertEqual(handler._register_standby(leader), 8)
        self.assertEqual(mock_get_cursor.call_args[1]['options'], '-c gp_role=utility')

        # the leader finishes its promotion while we wait, e.g. right after a switchover
        with patch('patroni.postgresql.mpp.greengage.time.sleep') as mock_sleep:
            cur.fetchone.side_effect = [(False,), (True,), registered]
            self.assertEqual(handler._register_standby(leader), 8)
            mock_sleep.assert_called_once_with(1)

        # another standby is registered already, we don't replace it
        cur.fetchone.side_effect = [(True,), (8, 'other', 'other', 5432, '/data')]
        self.assertIsNone(handler._register_standby(leader))
        # the same host and port, but a different data directory: a different standby as well
        cur.fetchone.side_effect = [(True,), (8, 'mdw', '127.0.0.2', 5432, '/data')]
        self.assertIsNone(handler._register_standby(leader))
        # registered by the host name, which matches connect_address, while the address is an IP
        cur.fetchone.side_effect = [(True,), (8, '127.0.0.2', '10.0.0.1', 5432, data_dir)]
        self.assertEqual(handler._register_standby(leader), 8)

        # no standby, register this node
        cur.fetchone.side_effect = [(True,), None, (9,)]
        self.assertEqual(handler._register_standby(leader), 9)
        cur.execute.assert_called_with('SELECT pg_catalog.gp_add_coordinator_standby(%s, %s, %s, %s)',
                                       ('127.0.0.2', '127.0.0.2', data_dir, 5432))

        # the port is taken from connect_address as well, it may differ from listen, e.g. behind NAT
        with patch.dict(self.p.config._config, {'connect_address': '127.0.0.2:6432'}):
            cur.fetchone.side_effect = [(True,), None, (10,)]
            self.assertEqual(handler._register_standby(leader), 10)
            cur.execute.assert_called_with('SELECT pg_catalog.gp_add_coordinator_standby(%s, %s, %s, %s)',
                                           ('127.0.0.2', '127.0.0.2', data_dir, 6432))

        # without connect_address the node can't be registered, listen may be a wildcard
        with patch.dict(self.p.config._config, {'connect_address': None}):
            mock_get_cursor.reset_mock()
            self.assertIsNone(handler._register_standby(leader))
            mock_get_cursor.assert_not_called()

        # a leader without conn_url, e.g. when the leader key outlives the member key in DCS: there is nothing to
        # connect to, falling back to the libpq defaults must not happen
        mock_get_cursor.reset_mock()
        self.assertIsNone(handler._register_standby(Leader(0, 0, Member(0, '', 28, {}))))
        mock_get_cursor.assert_not_called()

        # the leader is unreachable
        mock_get_cursor.side_effect = Exception
        self.assertIsNone(handler._register_standby(leader))
        self.assertIsNone(handler.basebackup_options(leader))
        self.assertIsNone(handler.basebackup_options(None))
        mock_get_cursor.side_effect = None

        # before_follow() doesn't touch gp_dbid if the node couldn't be registered
        cur.fetchone.side_effect = [(False,)]
        handler.before_follow(leader)
        self.assertFalse(os.path.exists(internal_auto_conf))
        self.assertIsNone(handler._get_local_dbid())

        # gp_dbid is written if it is missing, other settings in the file are kept
        cur.fetchone.side_effect = [(True,), registered]
        handler.before_follow(leader)
        self.assertEqual(handler._get_local_dbid(), 8)
        with open(internal_auto_conf, 'w') as f:
            f.write("foo = 'bar'\ngp_dbid='1'\n")
        self.assertEqual(handler._get_local_dbid(), 1)
        cur.fetchone.side_effect = [(True,), registered]
        with patch('patroni.postgresql.mpp.greengage.fsync_dir') as mock_fsync_dir:
            handler.before_follow(leader)
            mock_fsync_dir.assert_called_once_with(self.p.data_dir)
        with open(internal_auto_conf) as f:
            self.assertEqual(f.read(), "foo = 'bar'\ngp_dbid=8\n")
        # the file gets the permissions of files in PGDATA and is replaced atomically
        self.assertEqual(stat.S_IMODE(os.stat(internal_auto_conf).st_mode), pg_perm.file_create_mode)
        self.assertFalse(os.path.exists(internal_auto_conf + '.tmp'))

        # gp_dbid matches, nothing to do
        cur.fetchone.side_effect = [(True,), registered]
        with patch.object(handler, '_set_local_dbid') as mock_set_local_dbid:
            handler.before_follow(leader)
            mock_set_local_dbid.assert_not_called()

        cur.fetchone.side_effect = [(True,), registered]
        self.assertEqual(handler.basebackup_options(leader), ['--target-gp-dbid=8', '--no-verify-checksums'])
