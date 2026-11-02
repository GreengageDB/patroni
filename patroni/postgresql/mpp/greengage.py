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
import time

from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from ...dcs import Cluster, Leader, Member
from ...exceptions import ConfigParseError
from ...file_perm import pg_perm
from ...utils import parse_int, split_host_port
from ..connection import get_connection_cursor, remote_conn_kwargs
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

# how long to wait for the leader to finish its promotion before giving up the standby registration
PROMOTION_TIMEOUT = 30

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
        return isinstance(config, dict)

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

            The check is called only when the DCS object is created, i.e. when Patroni starts, and not when the
            configuration is reloaded.

        :param config: object or dictionary with Patroni configuration. Nothing is checked if it doesn't have
            ``postgresql.data_dir``, e.g. a configuration of ``patronictl``. The ``postgresql`` section itself can't
            be relied upon for that, because :class:`~patroni.config.Config` always fills it with the defaults.

        :raises:
            :exc:`~patroni.exceptions.ConfigParseError`: if the superuser or the rewind user don't use utility mode.
        """
        postgresql = config.get('postgresql')
        if not isinstance(postgresql, dict) or not postgresql.get('data_dir'):
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

    def replication_application_name(self) -> str:
        """Preserve Greengage's native coordinator synchronous replication behavior.

        The coordinator's commit path only waits for a receiver identified by this exact name,
        independently of ``synchronous_standby_names``.

        :returns: Greengage's required WAL receiver application name.
        """
        return 'gp_walreceiver'

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

    def _local_address(self) -> Optional[Tuple[str, int, str]]:
        """:returns: host, port and absolute data directory under which this node is known to the cluster, or
            ``None`` if ``connect_address`` isn't set: ``listen`` may be a wildcard or a list of addresses."""
        config = self._postgresql.config
        connect_address = config.get('connect_address')
        if not connect_address:
            logger.error('Greengage: postgresql.connect_address is not set, the node can not be registered'
                         ' as the coordinator standby')
            return None
        # both the host and the port are taken from connect_address: the one in listen is the port postgres is
        # started with, which, e.g. behind NAT or port forwarding, isn't necessarily reachable from other hosts
        host, port = split_host_port(connect_address, int(config.local_replication_address['port']))
        return host, port, os.path.abspath(self._postgresql.data_dir)

    def _get_local_dbid(self) -> Optional[int]:
        """:returns: ``gp_dbid`` from ``internal.auto.conf`` of this node, or ``None`` if it couldn't be read."""
        try:
            with open(self._internal_auto_conf) as f:
                for line in f:
                    name, _, value = line.partition('=')
                    if name.strip() == 'gp_dbid':
                        return int(value.strip().strip("'"))
        except Exception as e:
            logger.warning('Failed to read gp_dbid from %s: %r', self._internal_auto_conf, e)

    def _set_local_dbid(self, dbid: int) -> None:
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

    def _wait_for_promotion(self, cur: Any) -> bool:
        """Wait until the leader is the primary coordinator in its own catalog.

        .. note::
            The new leader takes the leader key before its promotion finishes, while the former primary starts to
            follow it soon after a switchover. Until the promotion is over, the catalog of the leader still has the
            row of the former primary, which the promotion is about to delete. Registering later would require a
            restart, because ``gp_dbid`` is read only at postmaster start, and nothing would trigger it.

            ``pg_is_in_recovery()`` can't be relied upon for that: ``StartupXLOG()`` leaves recovery before
            ``UpdateCatalogForStandbyPromotion()`` updates the catalog, hence the row of the leader itself is
            checked, which gets ``role = 'p'`` in the same transaction that deletes the row of the former primary.

        :param cur: cursor of a connection to the leader.

        :returns: ``True`` if the leader is the primary coordinator in its catalog, ``False`` if it still isn't
            after :data:`PROMOTION_TIMEOUT`.
        """
        deadline = time.time() + PROMOTION_TIMEOUT
        while True:
            cur.execute("SELECT role = 'p' FROM pg_catalog.gp_segment_configuration"
                        " WHERE content = -1 AND dbid = pg_catalog.current_setting('gp_dbid')::int")
            row = cur.fetchone()
            if row and row[0]:
                return True
            if time.time() >= deadline:
                logger.warning('Greengage: the leader is still not the primary coordinator in its catalog after %s'
                               ' seconds, not registering the coordinator standby', PROMOTION_TIMEOUT)
                return False
            logger.info('Greengage: the leader is not the primary coordinator in its catalog yet, waiting for its'
                        ' promotion to finish')
            time.sleep(1)

    def _register(self, cur: Any, host: str, port: int, data_dir: str) -> Optional[int]:
        if not self._wait_for_promotion(cur):
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

    def _register_standby(self, leader: Union[Leader, Member]) -> Optional[int]:
        """Make sure that this node is the coordinator standby of the *leader* in ``gp_segment_configuration``.

        :param leader: the primary coordinator.

        :returns: dbid of this node in the catalog of the *leader*, or ``None`` if it could not be registered.
        """
        address = self._local_address()
        if not address:
            return None
        host, port, data_dir = address
        conn_kwargs = remote_conn_kwargs(leader, self._postgresql.config.superuser)
        if not conn_kwargs.get('host'):
            # Member.conn_kwargs() provides no host if the leader doesn't announce a conn_url, e.g. when the leader
            # key outlives the member key in DCS. libpq would then connect to its built-in defaults, possibly to an
            # entirely different postgres, where the standby must not be registered.
            logger.error('Greengage: the leader %s has no connection address, can not register the coordinator'
                         ' standby', leader.name)
            return None
        conn_kwargs.setdefault('dbname', self._postgresql.database)
        # gp_add_coordinator_standby() needs the utility mode options from the superuser configuration.
        try:
            with get_connection_cursor(**conn_kwargs) as cur:
                return self._register(cur, host, port, data_dir)
        except Exception as e:
            logger.error('Greengage: failed to register the coordinator standby on %s: %r', leader.name, e)

    def _prepare_standby(self, leader: Union[Leader, Member]) -> bool:
        """Register this node on the *leader* and bring ``gp_dbid`` in ``internal.auto.conf`` in line with it.

        .. note::
            ``pg_rewind`` doesn't touch ``internal.auto.conf``, therefore the file of the former primary still
            has its old ``gp_dbid``, while its row has already been removed from the catalog by the promotion.
            Preparation must happen before rewind reads that dbid to create new tablespace directories.

        :param leader: the member this node is going to replicate from. It has to be the leader, the
            ``replicatefrom`` tag isn't supported.

        :returns: ``True`` if the local dbid matches the standby registration, ``False`` if registration failed.
        """
        dbid = self._register_standby(leader)
        if dbid is None:
            return False
        local_dbid = self._get_local_dbid()
        if local_dbid != dbid:
            logger.info('Greengage: changing gp_dbid in %s from %s to %s', self._internal_auto_conf, local_dbid, dbid)
            self._set_local_dbid(dbid)
        return True

    def before_rewind(self, leader: Union[Leader, Member]) -> bool:
        """Register the target and record its dbid before rewind creates tablespace directories."""
        try:
            return self._prepare_standby(leader)
        except OSError as e:
            logger.error('Greengage: failed to record the coordinator standby dbid before rewind: %r', e)
            return False

    def before_follow(self, leader: Union[Leader, Member]) -> None:
        """Prepare the standby also when following without rewind, e.g. after a clean switchover."""
        self._prepare_standby(leader)

    def basebackup_options(self, leader: Optional[Leader]) -> Optional[List[str]]:
        """Additional command line options for ``pg_basebackup``.

        .. note::
            ``pg_basebackup`` of Greengage requires the dbid of the new instance. It names the tablespace
            directories after it and writes it as ``gp_dbid`` to ``internal.auto.conf``. The dbid is assigned by
            registering the new node as the coordinator standby on the *leader*, which is not necessarily the
            member the replica is cloned from.

            Checksums are not verified, the same way as ``gpinitstandby`` does it: the verification fails on
            append-optimized tables, whose files don't have the page layout it expects.

        :param leader: the leader of the cluster, if known.

        :returns: a list with the ``--target-gp-dbid`` and ``--no-verify-checksums`` options, or ``None`` if the node
            couldn't be registered.
        """
        dbid = self._register_standby(leader) if leader else None
        if dbid is None:
            logger.error('Greengage: no dbid for the new coordinator standby, pg_basebackup is not possible')
            return None
        return ['--target-gp-dbid={0}'.format(dbid), '--no-verify-checksums']

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        .. note::
            Only prepared transactions, which distributed transactions can't work without, are enforced here.
            Everything else is left to the user, who is expected to configure it in the DCS.

        :param parameters: dictionary of GUCs, with key as GUC name and the corresponding value as current GUC value.
        """
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
