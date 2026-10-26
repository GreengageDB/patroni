"""Greengage DB support.

Greengage DB is an MPP database built on top of PostgreSQL, where a coordinator and a number of segments are
PostgreSQL clusters on their own. Patroni manages high availability of the coordinator, while the segments are
managed by Greengage itself. There is no cluster topology that Patroni would have to maintain, therefore this
module mostly adjusts the way Patroni starts and configures postgres.

The only piece of topology Patroni has to care about are the coordinators themselves. Greengage keeps them in
``gp_segment_configuration`` as the rows with ``content = -1``. When the standby is promoted, the startup process
updates the catalog itself: the row of the old coordinator is removed and the row of the standby, found by its
``gp_dbid``, becomes the primary one. Nothing puts the old coordinator back, therefore before a node starts to
replicate from the leader it registers itself there as the coordinator standby (the way ``gpinitstandby`` does it)
and makes sure that its ``gp_dbid`` matches the dbid assigned in the catalog. Otherwise the next promotion of the
node wouldn't find its row.

Only Greengage 7 (based on PostgreSQL 12) is supported, it was tested with 7.5.0. Greengage 6, based on PostgreSQL
9.4, is not: it predates the renaming of "master" to "coordinator", which the catalog functions used here, such as
``gp_add_coordinator_standby()``, rely on (Greengage 7 keeps the old names only as aliases), and it was never tested.
"""
import logging
import os
import re

from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from ...dcs import Cluster, Leader, Member
from ...exceptions import ConfigParseError
from ...file_perm import pg_perm
from ...utils import parse_int, split_host_port
from ..connection import get_connection_cursor
from ..misc import fsync_dir
from . import AbstractMPP, AbstractMPPHandler

if TYPE_CHECKING:  # pragma: no cover
    from ...config import Config
    from .. import Postgresql

logger = logging.getLogger(__name__)

# Greengage keeps gp_dbid there, the file isn't touched by pg_rewind and is written by pg_basebackup
INTERNAL_AUTO_CONF = 'internal.auto.conf'

# libpq `options` for a utility mode connection to the coordinator
UTILITY_MODE = '-c gp_role=utility'

# `-c gp_role=X`, `-cgp_role=X` or `--gp_role=X` in the libpq `options`
_GP_ROLE_RE = re.compile(r'(?:^|\s)(?:-c\s*|--)gp_role=(\S+)')


def get_gp_role(options: Optional[str]) -> Optional[str]:
    """Get ``gp_role`` set in the libpq *options*.

    :param options: value of the libpq ``options`` connection parameter.

    :returns: the last ``gp_role`` value set in *options*, the one that takes effect, or ``None``.
    """
    roles = _GP_ROLE_RE.findall(options or '')
    return roles[-1] if roles else None


class Greengage(AbstractMPP):
    """Greengage specific implementation of :class:`AbstractMPP`."""

    @staticmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for Greengage.

        :param config: configuration of ``greengage`` MPP section.

        :returns: ``True`` if config passes validation, otherwise ``False``.
        """
        return config is None or isinstance(config, dict)

    @staticmethod
    def check_config(config: Union['Config', Dict[str, Any]]) -> None:
        """Check that connections Patroni opens to the coordinator are utility mode ones.

        .. note::
            A dispatch mode connection to the coordinator can't be relied upon: on a standby, whose dbid isn't
            yet in the replicated ``gp_segment_configuration``, it fails with "cannot locate entry database", and
            ``pg_rewind`` silently produces a broken data directory, because the temporary table it creates is
            distributed and the files are then read on the segments. Therefore ``-c gp_role=utility`` is required
            in the ``options`` of the superuser, used for the local connections, and of the rewind user, if it is
            configured (otherwise ``pg_rewind`` uses the superuser). Replication connections don't depend on
            ``gp_role``.

        :param config: object or dictionary with Patroni configuration. Nothing is checked if it doesn't have the
            ``postgresql`` section, e.g. a configuration of ``patronictl``.

        :raises:
            :exc:`~patroni.exceptions.ConfigParseError`: if the superuser or the rewind user don't use utility mode.
        """
        postgresql = config.get('postgresql')
        if not isinstance(postgresql, dict):
            return
        authentication = postgresql.get('authentication') or {}
        users = ['superuser'] + (['rewind'] if authentication.get('rewind') else [])
        wrong = [user for user in users if get_gp_role((authentication.get(user) or {}).get('options')) != 'utility']
        if wrong:
            raise ConfigParseError('Greengage requires `options: {0}` in {1}: in dispatch mode connections to a standby'
                                   ' coordinator fail and pg_rewind silently produces a broken data directory'
                                   .format(UTILITY_MODE,
                                           ' and '.join('postgresql.authentication.' + user for user in wrong)))

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

    @property
    def _internal_auto_conf(self) -> str:
        return os.path.join(self._postgresql.data_dir, INTERNAL_AUTO_CONF)

    def _local_address(self) -> Tuple[str, int, str]:
        """:returns: host, port and data directory under which this node is known to the cluster."""
        config = self._postgresql.config
        port = int(config.local_replication_address['port'])  # the port Patroni starts postgres with
        host = split_host_port(config.get('connect_address') or config.get('listen') or '', port)[0]
        return host, port, os.path.normpath(self._postgresql.data_dir)

    def get_local_dbid(self) -> Optional[int]:
        """:returns: ``gp_dbid`` from ``internal.auto.conf`` of this node, or ``None`` if it couldn't be read."""
        try:
            with open(self._internal_auto_conf) as f:
                for line in f:
                    name, _, value = line.partition('=')
                    if name.strip() == 'gp_dbid':
                        return int(value.strip().strip("'"))
        except Exception as e:
            logger.warning('Failed to read gp_dbid from %s: %r', self._internal_auto_conf, e)

    def set_local_dbid(self, dbid: int) -> None:
        """Write *dbid* as ``gp_dbid`` to ``internal.auto.conf`` of this node, keeping the rest of the file."""
        lines = []
        if os.path.exists(self._internal_auto_conf):
            with open(self._internal_auto_conf) as f:
                lines = [line for line in f if line.partition('=')[0].strip() != 'gp_dbid']
        lines.append('gp_dbid={0}\n'.format(dbid))
        # the file is replaced atomically, with the permissions of the files in PGDATA, the same way as
        # Patroni writes replication slots
        pg_perm.set_permissions_from_data_directory(self._postgresql.data_dir)
        tmp = self._internal_auto_conf + '.tmp'
        with open(tmp, 'w') as f:
            os.chmod(tmp, pg_perm.file_create_mode)
            f.writelines(lines)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, self._internal_auto_conf)
        fsync_dir(self._postgresql.data_dir)

    def _register(self, cur: Any, host: str, port: int, data_dir: str) -> Optional[int]:
        cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
        row = cur.fetchone()
        if not row or row[0]:
            logger.info('Greengage: the leader is still in recovery, postponing the standby registration')
            return None

        cur.execute("SELECT dbid, hostname, address, port, datadir FROM pg_catalog.gp_segment_configuration"
                    " WHERE content = -1 AND role = 'm'")
        row = cur.fetchone()
        if row:
            # gpinitstandby may have registered this node by its host name or by its address, e.g. an IP, while the
            # host from connect_address could be either of them
            if host in (row[1], row[2]) and (row[3], os.path.normpath(row[4])) == (port, data_dir):
                return row[0]
            logger.warning('Greengage: the leader already has a different coordinator standby registered'
                           ' (dbid=%s %s/%s:%s %s), not registering %s:%s %s', *row, host, port, data_dir)
            return None

        cur.execute('SELECT pg_catalog.gp_add_coordinator_standby(%s, %s, %s, %s)', (host, host, data_dir, port))
        row = cur.fetchone()
        dbid = row and row[0]
        logger.info('Greengage: registered %s:%s %s as the coordinator standby with dbid=%s',
                    host, port, data_dir, dbid)
        return dbid

    def register_standby(self, leader: Union[Leader, Member]) -> Optional[int]:
        """Make sure that this node is the coordinator standby of the *leader* in ``gp_segment_configuration``.

        :param leader: the primary coordinator.

        :returns: dbid of this node in the catalog of the *leader*, or ``None`` if it could not be registered.
        """
        host, port, data_dir = self._local_address()
        conn_kwargs = leader.conn_kwargs(self._postgresql.config.superuser)
        conn_kwargs.setdefault('dbname', self._postgresql.database)
        # gp_add_coordinator_standby() works only in utility mode, `-c gp_role=utility` is expected to come from
        # the `options` of the superuser in the `authentication` section. Member.conn_kwargs() doesn't pass them
        # on, hence they are added explicitly, and get_connection_cursor() appends them to the default ones.
        if self._postgresql.config.superuser.get('options'):
            conn_kwargs['options'] = self._postgresql.config.superuser['options']
        try:
            with get_connection_cursor(**conn_kwargs) as cur:
                return self._register(cur, host, port, data_dir)
        except Exception as e:
            logger.error('Greengage: failed to register the coordinator standby on %s: %r', leader.name, e)

    def before_follow(self, leader: Union[Leader, Member]) -> None:
        """Register this node on the *leader* and bring ``gp_dbid`` in ``internal.auto.conf`` in line with it.

        .. note::
            ``pg_rewind`` doesn't touch ``internal.auto.conf``, therefore the file of the former primary still
            has its old ``gp_dbid``, while its row has already been removed from the catalog by the promotion.

        :param leader: the member this node is going to replicate from.
        """
        dbid = self.register_standby(leader)
        if dbid is None:
            return
        local_dbid = self.get_local_dbid()
        if local_dbid != dbid:
            logger.info('Greengage: changing gp_dbid in %s from %s to %s', self._internal_auto_conf, local_dbid, dbid)
            self.set_local_dbid(dbid)

    def basebackup_options(self, leader: Optional[Leader]) -> Optional[List[str]]:
        """Additional command line options for ``pg_basebackup``.

        .. note::
            ``pg_basebackup`` of Greengage requires the dbid of the new instance. It names the tablespace
            directories after it and writes it as ``gp_dbid`` to ``internal.auto.conf``. The dbid is assigned by
            registering the new node as the coordinator standby on the *leader*, which is not necessarily the
            member the replica is cloned from.

        :param leader: the leader of the cluster, if known.

        :returns: a list with the ``--target-gp-dbid`` option, or ``None`` if the node couldn't be registered.
        """
        dbid = self.register_standby(leader) if leader else None
        if dbid is None:
            logger.error('Greengage: no dbid for the new coordinator standby, pg_basebackup is not possible')
            return None
        return ['--target-gp-dbid={0}'.format(dbid)]

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
