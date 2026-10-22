"""Greengage DB support.

Greengage DB is an MPP database built on top of PostgreSQL, where a coordinator and a number of segments are
PostgreSQL clusters on their own. Patroni manages high availability of the coordinator, while the segments are
managed by Greengage itself. There is no cluster topology that Patroni would have to maintain, therefore this
module only adjusts the way Patroni starts and configures postgres.
"""
from typing import Any, Dict, List, Union, TYPE_CHECKING

from ...dcs import Cluster
from ...utils import parse_int
from . import AbstractMPP, AbstractMPPHandler

if TYPE_CHECKING:  # pragma: no cover
    from .. import Postgresql


class Greengage(AbstractMPP):
    """Greengage specific implementation of :class:`AbstractMPP`."""

    @staticmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for Greengage.

        :param config: configuration of ``greengage`` MPP section.

        :returns: ``True`` if config passes validation, otherwise ``False``.
        """
        return config is None or isinstance(config, dict)

    def is_enabled(self) -> bool:
        """Check whether this node is a part of the MPP cluster from the Patroni point of view.

        .. note::
            Unlike Citus, Greengage doesn't require Patroni to maintain the cluster topology, hence there are no
            groups and no coordinator/worker distinction. Returning ``False`` here keeps the keys of all Greengage
            clusters on the same level in DCS, exactly like for a standalone PostgreSQL cluster.

        :returns: always ``False``.
        """
        return False

    @property
    def group(self) -> None:
        """The group of this Greengage node.

        :returns: always ``None``.
        """
        return None

    @property
    def coordinator_group_id(self) -> None:
        """The group id of the coordinator PostgreSQL cluster.

        :returns: always ``None``.
        """
        return None


class GreengageHandler(Greengage, AbstractMPPHandler):
    """Greengage specific implementation of :class:`AbstractMPPHandler`."""

    # GUCs for which Greengage uses values higher than the PostgreSQL defaults Patroni relies on. Setting them
    # lower than that makes the coordinator refuse to start or fail to run distributed queries.
    MIN_GUC_VALUES = {
        'max_locks_per_transaction': 128,
        'max_worker_processes': 13,
    }

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        """Init method for :class:`GreengageHandler`.

        :param postgresql: a reference to :class:`Postgresql` object.
        :param config: the ``greengage`` MPP config section.
        """
        AbstractMPPHandler.__init__(self, postgresql, config)

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        """Handle an event sent from a worker node.

        Not applicable to Greengage, segments don't report to the coordinator through Patroni.
        """

    def sync_meta_data(self, cluster: Cluster) -> None:
        """Sync meta data on the coordinator.

        Not applicable to Greengage, the segment configuration is maintained by Greengage itself.
        """

    def on_demote(self) -> None:
        """On demote handler.

        Is called when the primary was demoted.
        """

    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """

    def bootstrap(self) -> None:
        """Bootstrap handler.

        A Greengage cluster is created by ``gpinitsystem``, which Patroni can only run as a custom bootstrap method.
        """

    def cmdline_options(self) -> List[str]:
        """Additional command line options for the ``postgres`` process.

        .. note::
            ``gp_role`` can't be written to ``postgresql.conf``, because its check hook rejects the ``dispatch``
            value when it doesn't come from the command line. Besides that, the GUC is not reported by
            ``postgres --describe-config``, therefore Patroni would silently drop it from ``postgresql.conf``.

        :returns: a list with the ``gp_role`` command line option.
        """
        return ['--gp_role=dispatch']

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        .. note::
            Only the values that Greengage can't work without are enforced here. Everything else, for example
            ``max_connections``, is left to the user, who is expected to configure it in the DCS.

        :param parameters: dictionary of GUCs, with key as GUC name and the corresponding value as current GUC value.
        """
        for name, minimum in self.MIN_GUC_VALUES.items():
            value = parse_int(parameters.get(name))
            if value is None or value < minimum:
                parameters[name] = minimum

        # Greengage implements distributed transactions on top of two-phase commit, therefore prepared
        # transactions must be enabled. If not explicitly configured we deduce the value from max_connections,
        # the same way as it is done for Citus.
        max_connections = parse_int(parameters.get('max_connections'))
        if max_connections and not parse_int(parameters.get('max_prepared_transactions')):
            parameters['max_prepared_transactions'] = max_connections

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """Check whether provided replication *slot* existing in the database should not be removed.

        .. note::
            Greengage doesn't use logical replication for its internal needs, the segment mirroring is implemented
            with streaming replication to a known host, i.e. without replication slots.

        :param slot: dictionary containing the replication slot settings, like ``name``, ``database``, ``type``, and
                     ``plugin``.

        :returns: always ``False``.
        """
        return False

