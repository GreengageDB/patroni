import logging

from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection, Cursor
    from psycopg2 import connection, cursor
    from ..dcs import Leader, Member

from .misc import merge_options
from .. import psycopg
from ..exceptions import PostgresConnectionException

logger = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT = 3
DEFAULT_CONNECTION_OPTIONS = '-c statement_timeout=2000'


def remote_conn_kwargs(member: Union['Leader', 'Member'], auth: Dict[str, Any]) -> Dict[str, Any]:
    """Build connection parameters for a remote member, preserving trusted authentication ``options``.

    ``Member.conn_kwargs()`` deliberately excludes ``options`` from *auth*. Some callers need these locally
    configured libpq options, for example Greengage's ``gp_role=utility``. Other excluded parameters such as
    ``connect_timeout`` remain excluded.
    """
    ret = member.conn_kwargs(auth)
    if auth.get('options'):
        ret['options'] = auth['options']
    return ret


class NamedConnection:
    """Helper class to manage ``psycopg`` connections from Patroni to PostgreSQL.

    :ivar server_version: PostgreSQL version in integer format where we are connected to.
    """

    server_version: int

    def __init__(self, pool: 'ConnectionPool', name: str, kwargs_override: Optional[Dict[str, Any]]) -> None:
        """Create an instance of :class:`NamedConnection` class.

        :param pool: reference to a :class:`ConnectionPool` object.
        :param name: name of the connection.
        :param kwargs_override: :class:`dict` object with connection parameters that should be
                                different from default values provided by connection *pool*.
        """
        self._pool = pool
        self._name = name
        self._kwargs_override = kwargs_override or {}
        self._lock = Lock()  # used to make sure that only one connection to postgres is established
        self._connection = None

    @property
    def _conn_kwargs(self) -> Dict[str, Any]:
        """Connection parameters for this :class:`NamedConnection`."""
        ret = {**self._pool.conn_kwargs, **self._kwargs_override, 'application_name': f'Patroni {self._name}'}
        if 'options' in self._kwargs_override:
            # the libpq `options` configured for the whole pool (e.g. from `authentication.superuser.options`)
            # must not be lost, the connection specific ones are appended and therefore take precedence
            ret['options'] = merge_options(self._pool.conn_kwargs.get('options'), self._kwargs_override['options'])
        return ret

    def get(self) -> Union['connection', 'Connection[Any]']:
        """Get ``psycopg``/``psycopg2`` connection object.

        .. note::
            Opens a new connection if necessary.

        :returns: ``psycopg`` or ``psycopg2`` connection object.
        """
        with self._lock:
            if not self._connection or self._connection.closed != 0:
                logger.info("establishing a new patroni %s connection to postgres", self._name)
                self._connection = psycopg.connect(**self._conn_kwargs)
                self.server_version = getattr(self._connection, 'server_version', 0)
        return self._connection

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute a query with parameters and optionally returns a response.

        :param sql: SQL statement to execute.
        :param params: parameters to pass.

        :returns: a query response as a list of tuples if there is any.
        :raises:
            :exc:`~psycopg.Error` if had issues while executing *sql*.

            :exc:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.
        """
        cursor = None
        try:
            with self.get().cursor() as cursor:
                cursor.execute(sql.encode('utf-8'), params or None)
                return cursor.fetchall() if cursor.rowcount and cursor.rowcount > 0 else []
        except psycopg.Error as exc:
            if cursor and cursor.connection.closed == 0:
                # When connected via unix socket, psycopg2 can't recognize 'connection lost' and leaves
                # `self._connection.closed == 0`, but the generic exception is raised. It doesn't make
                # sense to continue with existing connection and we will close it, to avoid its reuse.
                if type(exc) in (psycopg.DatabaseError, psycopg.OperationalError):
                    self.close()
                else:
                    raise exc
            raise PostgresConnectionException('connection problems') from exc

    def close(self, silent: bool = False) -> bool:
        """Close the psycopg connection to postgres.

        :param silent: whether the method should not write logs.

        :returns: ``True`` if ``psycopg`` connection was closed, ``False`` otherwise.``
        """
        ret = False
        if self._connection and self._connection.closed == 0:
            self._connection.close()
            if not silent:
                logger.info("closed patroni %s connection to postgres", self._name)
            ret = True
        self._connection = None
        return ret


class ConnectionPool:
    """Helper class to manage named connections from Patroni to PostgreSQL.

    The instance keeps named :class:`NamedConnection` objects and parameters that must be used for new connections.
    """

    def __init__(self) -> None:
        """Create an instance of :class:`ConnectionPool` class."""
        self._lock = Lock()
        self._connections: Dict[str, NamedConnection] = {}
        self._conn_kwargs: Dict[str, Any] = {}

    @property
    def conn_kwargs(self) -> Dict[str, Any]:
        """Connection parameters that must be used for new ``psycopg`` connections."""
        with self._lock:
            return self._conn_kwargs.copy()

    @conn_kwargs.setter
    def conn_kwargs(self, value: Dict[str, Any]) -> None:
        """Set new connection parameters.

        :param value: :class:`dict` object with connection parameters.
        """
        with self._lock:
            # user supplied options (e.g. from `authentication.superuser.options`) come last,
            # so that it is possible to override our defaults
            options = merge_options(DEFAULT_CONNECTION_OPTIONS, '-c pg_stat_statements.track=none',
                                    value.get('options'))
            self._conn_kwargs = {'connect_timeout': DEFAULT_CONNECT_TIMEOUT, 'fallback_application_name': 'Patroni',
                                 **value, 'options': options}

    def get(self, name: str, kwargs_override: Optional[Dict[str, Any]] = None) -> NamedConnection:
        """Get a new named :class:`NamedConnection` object from the pool.

        .. note::
            Creates a new :class:`NamedConnection` object if it doesn't yet exist in the pool.

        :param name: name of the connection.
        :param kwargs_override: :class:`dict` object with connection parameters that should be
                                different from default values provided by :attr:`conn_kwargs`.

        :returns: :class:`NamedConnection` object.
        """
        with self._lock:
            if name not in self._connections:
                self._connections[name] = NamedConnection(self, name, kwargs_override)
        return self._connections[name]

    def close(self) -> None:
        """Close all named connections from Patroni to PostgreSQL registered in the pool."""
        with self._lock:
            closed_connections = [conn.close(True) for conn in self._connections.values()]
            if any(closed_connections):
                logger.info("closed patroni connections to postgres")


@contextmanager
def get_connection_cursor(**kwargs: Any) -> Generator[Union['cursor', 'Cursor[Any]'], None, None]:
    # options passed by the caller, e.g. the ones of the user from the `authentication` section,
    # are appended to the default ones and therefore take precedence
    conn_kwargs = {'connect_timeout': DEFAULT_CONNECT_TIMEOUT, **kwargs,
                   'options': merge_options(DEFAULT_CONNECTION_OPTIONS, kwargs.get('options'))}
    conn = psycopg.connect(**conn_kwargs)
    with conn.cursor() as cur:
        yield cur
    conn.close()
