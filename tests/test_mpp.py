import os

from typing import Any
from unittest.mock import patch

from patroni.exceptions import ConfigParseError, PatroniException
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
        self.assertIsNone(nullHandler.before_follow(self.cluster.leader))
        self.assertEqual(nullHandler.basebackup_options(self.cluster.leader), [])
        self.assertFalse(nullHandler.ignore_replication_slot({}))

    def test_greengage_handler(self):
        config = {'greengage': {}}
        mpp = get_mpp(config)
        self.assertIsInstance(mpp, Greengage)
        self.assertTrue(mpp.validate_config(config['greengage']))
        self.assertTrue(mpp.validate_config(None))
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

        # Greengage minimums are enforced, bigger values are kept, max_prepared_transactions is deduced
        parameters = {'max_locks_per_transaction': 64, 'max_worker_processes': 20,
                      'max_prepared_transactions': 0, 'max_connections': 150}
        handler.adjust_postgres_gucs(parameters)
        self.assertEqual(parameters, {'max_locks_per_transaction': 128, 'max_worker_processes': 20,
                                      'max_prepared_transactions': 150, 'max_connections': 150})
        # explicitly configured max_prepared_transactions is not touched, missing values get the minimums
        parameters = {'max_prepared_transactions': '200', 'max_connections': '150'}
        handler.adjust_postgres_gucs(parameters)
        self.assertEqual(parameters, {'max_locks_per_transaction': 128, 'max_worker_processes': 13,
                                      'max_prepared_transactions': '200', 'max_connections': '150'})

    def test_greengage_check_config(self):
        utility = {'username': 'foo', 'options': '-c statement_timeout=0 -c gp_role=utility'}
        # nothing to check without the postgresql section, e.g. in the patronictl configuration
        self.assertIsInstance(get_mpp({'greengage': {}}), Greengage)
        # the superuser is required to use utility mode, the rewind user only if it is configured
        config = {'greengage': {}, 'postgresql': {'authentication': {'superuser': utility}}}
        self.assertIsInstance(get_mpp(config), Greengage)
        config['postgresql']['authentication']['rewind'] = {'username': 'bar'}
        self.assertRaisesRegex(ConfigParseError, 'for the rewind in', get_mpp, config)
        config['postgresql']['authentication']['rewind']['options'] = '--gp_role=utility'
        self.assertIsInstance(get_mpp(config), Greengage)
        # the last gp_role wins
        config['postgresql']['authentication']['superuser'] = {'options': '-c gp_role=utility -cgp_role=dispatch'}
        self.assertRaisesRegex(ConfigParseError, 'for the superuser in', get_mpp, config)
        config['postgresql']['authentication'] = {}
        self.assertRaisesRegex(ConfigParseError, 'for the superuser in', get_mpp, config)
        self.assertIsNone(get_gp_role(None))
        self.assertIsNone(get_gp_role('-c work_mem=1MB'))

    @patch('patroni.postgresql.mpp.greengage.get_connection_cursor')
    def test_greengage_standby_registration(self, mock_get_cursor):
        handler = get_mpp({'greengage': {}}).get_handler_impl(self.p)
        leader = self.cluster.leader
        internal_auto_conf = os.path.join(self.p.data_dir, 'internal.auto.conf')
        cur = mock_get_cursor.return_value.__enter__.return_value
        # how this node is known to the cluster: connect_address, port from listen, normalized data_dir
        registered = (8, '127.0.0.2', 5432, self.p.data_dir + '/')

        # the leader is still in recovery
        cur.fetchone.side_effect = [(True,)]
        self.assertIsNone(handler.register_standby(leader))

        # the connection is a utility mode one: options of the superuser are passed to get_connection_cursor()
        self.p.config.superuser['options'] = '-c gp_role=utility'
        cur.fetchone.side_effect = [(False,), registered]
        self.assertEqual(handler.register_standby(leader), 8)
        self.assertEqual(mock_get_cursor.call_args[1]['options'], '-c gp_role=utility')

        # another standby is registered already, we don't replace it
        cur.fetchone.side_effect = [(False,), (8, 'other', 5432, '/data')]
        self.assertIsNone(handler.register_standby(leader))

        # no standby, register this node
        cur.fetchone.side_effect = [(False,), None, (9,)]
        self.assertEqual(handler.register_standby(leader), 9)
        cur.execute.assert_called_with('SELECT pg_catalog.gp_add_coordinator_standby(%s, %s, %s, %s)',
                                       ('127.0.0.2', '127.0.0.2', self.p.data_dir, 5432))

        # the leader is unreachable
        mock_get_cursor.side_effect = Exception
        self.assertIsNone(handler.register_standby(leader))
        self.assertIsNone(handler.basebackup_options(leader))
        self.assertIsNone(handler.basebackup_options(None))
        mock_get_cursor.side_effect = None

        # before_follow() doesn't touch gp_dbid if the node couldn't be registered
        cur.fetchone.side_effect = [(True,)]
        handler.before_follow(leader)
        self.assertFalse(os.path.exists(internal_auto_conf))
        self.assertIsNone(handler.get_local_dbid())

        # gp_dbid is written if it is missing, other settings in the file are kept
        cur.fetchone.side_effect = [(False,), registered]
        handler.before_follow(leader)
        self.assertEqual(handler.get_local_dbid(), 8)
        with open(internal_auto_conf, 'w') as f:
            f.write("foo = 'bar'\ngp_dbid='1'\n")
        self.assertEqual(handler.get_local_dbid(), 1)
        cur.fetchone.side_effect = [(False,), registered]
        handler.before_follow(leader)
        with open(internal_auto_conf) as f:
            self.assertEqual(f.read(), "foo = 'bar'\ngp_dbid=8\n")

        # gp_dbid matches, nothing to do
        cur.fetchone.side_effect = [(False,), registered]
        with patch.object(handler, 'set_local_dbid') as mock_set_local_dbid:
            handler.before_follow(leader)
            mock_set_local_dbid.assert_not_called()

        cur.fetchone.side_effect = [(False,), registered]
        self.assertEqual(handler.basebackup_options(leader), ['--target-gp-dbid=8'])
