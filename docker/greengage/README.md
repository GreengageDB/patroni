# Patroni with a Greengage coordinator

A development setup for running Patroni on top of the coordinator (`mdw`) and the
standby coordinator (`smdw`) of a Greengage demo cluster:

* `docker-compose-greengage.yml` in the repository root starts etcd and a Greengage
  container, with this repository mounted at `/home/gpadmin/patroni`;
* `mdw.yml` and `smdw.yml` in this directory are the Patroni configs of the two
  coordinators.

It is meant for development and testing only. Only Greengage 7 is supported, the
container uses `ggdb7_ubuntu:7.5.0`.

Greengage supports only one coordinator standby, therefore exactly two Patroni members
may be promoted. Any additional member needs `nofailover: true` in its `tags`, otherwise
its promotion leaves Greengage's catalog wrong, see the Greengage section of
`docs/yaml_configuration.rst`.

Patroni doesn't change `pg_hba.conf` of the segments. In this setup all instances run on
one host, which the segments already accept. On a multi-host cluster every host that can
become the coordinator must be allowed in `pg_hba.conf` of all segments, as `gpinitstandby`
does it, see the same section.

Steps (from the repository root):

```
# remove the containers of a previous run, if any: etcd must start empty, otherwise it
# keeps the cluster ID of the previous Greengage cluster and Patroni exits with "system ID mismatch"
docker compose -f docker-compose-greengage.yml down
docker compose -f docker-compose-greengage.yml up -d
docker compose -f docker-compose-greengage.yml exec greengage bash
```


in the docker container:
```
source /home/gpadmin/gpdb_src/concourse/scripts/common.bash
install_gpdb
/home/gpadmin/gpdb_src/concourse/scripts/setup_gpadmin_user.bash
make_cluster

cd /home/gpadmin/patroni
pip install .[etcd3]

su - gpadmin
```

Under gpadmin:

```
. /usr/local/greengage-db-devel/greengage_path.sh
. gpdb_src/gpAux/gpdemo/gpdemo-env.sh

psql -v ON_ERROR_STOP=1 postgres <<'SQL'
CREATE USER patroni WITH SUPERUSER ENCRYPTED PASSWORD 'patroni_pass';
-- replication user
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
-- pg_basebackup of Greengage runs `SHOW log_directory` on the source server
GRANT pg_read_all_settings TO replicator;
-- pg_rewind user (optional, but recommended)
CREATE USER rewind_user WITH ENCRYPTED PASSWORD 'rewind_pass';
GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text, boolean, boolean) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text, boolean) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO rewind_user;
GRANT pg_read_all_settings TO rewind_user;
SQL


# Needed: Patroni connects as `patroni` to the already running coordinators
# before it rewrites pg_hba.conf from the yaml, and exits if it is rejected.
echo 'host    all     patroni 127.0.0.1/32    md5' >> $MASTER_DATA_DIRECTORY/pg_hba.conf
echo 'host    all     patroni 127.0.0.1/32    md5' >> /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/standby/pg_hba.conf


# gpconfig needs $USER, which `su - gpadmin` sets, but `docker exec -u gpadmin` doesn't
gpconfig -c hot_standby -v on
gpstop -arf


# run Patroni from the repository: python3 -m patroni then uses the source tree
# instead of the copy installed by pip
cd /home/gpadmin/patroni
export PATH=$PATH:/home/gpadmin/.local/bin/

# Start mdw first and wait until it holds the leader lock, otherwise smdw may
# win the race and Patroni promotes the standby. One log file per node.
python3 -m patroni docker/greengage/mdw.yml > /home/gpadmin/gpAdminLogs/patroni-mdw.log 2>&1 &
until patronictl -c docker/greengage/mdw.yml list -f json | grep -q '"Member": "mdw".*"Role": "Leader"'; do sleep 1; done
python3 -m patroni docker/greengage/smdw.yml > /home/gpadmin/gpAdminLogs/patroni-smdw.log 2>&1 &


# GUCs from Patroni's CMDLINE_OPTIONS are ignored in the yaml and live in DCS.
# Use the gpdemo values, then restart both nodes (members are mdw/smdw) to apply
# them and the other pending changes (cluster_name, listen_addresses, wal_log_hints).
patronictl -c docker/greengage/mdw.yml edit-config test-cluster --force -p max_connections=150 -p max_prepared_transactions=250
patronictl -c docker/greengage/mdw.yml restart test-cluster mdw --force
patronictl -c docker/greengage/mdw.yml restart test-cluster smdw --force

patronictl -c docker/greengage/mdw.yml list
```

Expected result (no "Pending restart" column):

```
+ Cluster: test-cluster (...) ------------------+----+-------------+-----+------------+-----+
| Member | Host           | Role    | State     | TL | Receive LSN | Lag | Replay LSN | Lag |
+--------+----------------+---------+-----------+----+-------------+-----+------------+-----+
| mdw    | greengage:7000 | Leader  | running   |  1 |             |     |            |     |
| smdw   | greengage:7001 | Replica | streaming |  1 |   0/C000000 |   0 |  0/C018E30 |   0 |
+--------+----------------+---------+-----------+----+-------------+-----+------------+-----+
```

Switchover (and back with `--leader smdw --candidate mdw`):

```
patronictl -c docker/greengage/mdw.yml switchover test-cluster --leader mdw --candidate smdw --force

# the new leader is role `p`, the demoted node is registered again as role `m`
# with a new dbid, which matches gp_dbid in its internal.auto.conf
PGOPTIONS='-c gp_role=utility' psql -p 7001 postgres -c "select dbid, role, port from gp_segment_configuration where content = -1"
```
