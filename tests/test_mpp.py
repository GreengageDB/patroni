from typing import Any

from patroni.exceptions import PatroniException
from patroni.postgresql.mpp import AbstractMPP, get_mpp, Null
from patroni.postgresql.mpp.greengage import Greengage

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
