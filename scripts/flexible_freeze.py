import time
import sys
import argparse
import psycopg2
from tendo import singleton
import logging

me = singleton.SingleInstance()


def parse_arguments():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--minutes", dest="run_min",
                        type=int, default=120,
                        help="Number of minutes to run before halting.  Defaults to 2 hours")
    parser.add_argument("-d", "--databases", dest="dblist",
                        help="List of databases to vacuum, if not all of them")
    parser.add_argument("--vacuum", dest="vacuum", action="store_true",
                        help="Do regular vacuum instead of VACUUM FREEZE")
    parser.add_argument("--pause", dest="pause_time", default=10,
                        help="seconds to pause between vacuums.  Default is 10.")
    parser.add_argument("--freezeage", dest="freezeage",
                        type=int, default=10000000,
                        help="minimum age for freezing.  Default 10m XIDs")
    parser.add_argument("--costdelay", dest="costdelay",
                        type=int, default=20,
                        help="vacuum_cost_delay setting in ms.  Default 20")
    parser.add_argument("--costlimit", dest="costlimit",
                        type=int, default=2000,
                        help="vacuum_cost_limit setting.  Default 2000")
    parser.add_argument("-t", "--print-timestamps", action="store_true",
                        dest="print_timestamps")
    parser.add_argument("--enforce-time", dest="enforcetime", action="store_true",
                        help="enforce time limit by terminating vacuum")
    parser.add_argument("-l", "--log", dest="logfile")
    parser.add_argument("-v", "--verbose", action="store_true",
                        dest="verbose")
    parser.add_argument("-U", "--user", dest="dbuser", default="postgres",
                        help="database user. Default postgres")
    parser.add_argument("-H", "--host", dest="dbhost",
                        help="database hostname")
    parser.add_argument("-p", "--port", dest="dbport",
                        help="database port")
    parser.add_argument("-w", "--password", dest="dbpass",
                        help="database password")
    args = parser.parse_args()


class TableUtil:
    """This class is used to handel tables vacuum and freeze"""

    def __init__(self):
        self.conn = None

    @staticmethod
    def create_connection(dbname, dbuser, dbhost, dbport, dbpass):
        logger = logging.getLogger(__name__)
        if dbname:
            connect_string = "dbname=%s application_name=flexible_freeze" % dbname
        else:
            logger.info("ERROR: a target database is required.")
            return None

        if dbhost:
            connect_string += " host=%s " % dbhost

        if dbuser:
            connect_string += " user=%s " % dbuser

        if dbpass:
            connect_string += " password=%s " % dbpass

        if dbport:
            connect_string += " port=%s " % dbport

        try:
            psycopg2.connect(connect_string).set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        except Exception as ex:
            logger.error("connection to database %s failed, aborting" % dbname)
            logger.error(str(ex))
            sys.exit(1)

        return psycopg2.connect(connect_string)

    @staticmethod
    def get_db_list(conn, dblist, dbuser, dbhost, dbport, dbpass):
        logger = logging.getLogger(__name__)
        if dblist is None:
            conn = TableUtil.create_connection("postgres", dbuser, dbhost, dbport, dbpass)
            cur = conn.cursor()
            cur.execute("""SELECT datname FROM pg_database
                    WHERE datname NOT IN ('postgres','template1','template0')
                    ORDER BY age(datfrozenxid) DESC""")

            db_list = []
            for dbname in cur:
                db_list.append(dbname[0])
            conn.close()
            if db_list is not None:
                return db_list
            else:
                logger.info("No database to vacuum")
                sys.exit(0)
        else:
            return dblist.split(',')

    @staticmethod
    def get_vacuum_list(conn):
        logger = logging.getLogger(__name__)
        query = """WITH deadrow_tables AS (
                SELECT relid::regclass as full_table_name,
                    ((n_dead_tup::numeric) / ( n_live_tup + 1 )) as dead_pct,
                    pg_relation_size(relid) as table_bytes
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 100
                AND ( (now() - last_autovacuum) > INTERVAL '1 hour'
                    OR last_autovacuum IS NULL )
                AND ( (now() - last_vacuum) > INTERVAL '1 hour'
                    OR last_vacuum IS NULL )
            )
            SELECT full_table_name
            FROM deadrow_tables
            WHERE dead_pct > 0.05
            AND table_bytes > 1000000
            ORDER BY dead_pct DESC, table_bytes DESC;"""
        cur = conn.cursor()
        cur.execute(query)
        logger.info("getting list of tables")

        table_result_set = cur.fetchall()
        return map(lambda (row): row[0], table_result_set)

    @staticmethod
    def get_freeze_list(conn, freeze_age):
        logger = logging.getLogger(__name__)
        query = """WITH tabfreeze AS (
                        SELECT pg_class.oid::regclass AS full_table_name,
                        greatest(age(pg_class.relfrozenxid), age(toast.relfrozenxid)) as freeze_age,
                        pg_relation_size(pg_class.oid)
                    FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                        LEFT OUTER JOIN pg_class as toast
                            ON pg_class.reltoastrelid = toast.oid
                    WHERE nspname not in ('pg_catalog', 'information_schema')
                        AND nspname NOT LIKE 'pg_temp%'
                        AND pg_class.relkind = 'r'
                    )
                    SELECT full_table_name
                    FROM tabfreeze
                    WHERE freeze_age > {0}
                    ORDER BY freeze_age DESC
                    LIMIT 1000;""".format(freeze_age)
        cur = conn.cursor()
        cur.execute(query)
        logger.info("getting list of tables")
        table_result_set = cur.fetchall()
        return map(lambda (row): row[0], table_result_set)

    @staticmethod
    def set_vacuum_cost(conn, costdelay, costlimit):
        cur = conn.cursor()
        cur.execute("SET vacuum_cost_delay = {0}".format(costdelay))
        cur.execute("SET vacuum_cost_limit = {0}".format(costlimit))

    @staticmethod
    def vacuum(conn, table, halt_time, vacuum, enforce_time):
        logger = logging.getLogger(__name__)
        if vacuum:
            query = """VACUUM ANALYZE %s""" % table
        else:
            query = """VACUUM FREEZE ANALYZE %s""" % table

        cur = conn.cursor()
        logger.info("vacuuming table %s" % (table))

        timeout_secs = int(halt_time - time.time()) + 30
        timeout_query = """SET statement_timeout = '%ss'""" % timeout_secs
        try:
            if enforce_time:
                cur.execute(timeout_query)
            cur.execute(query)
        except Exception as ex:
            logger.error("Vacuuming %s failed." % table)
            logger.error(str(ex))
            if time.time() >= halt_time:
                logger.debug("halted flexible_freeze due to enforced time limit")
            else:
                logger.error("VACUUMING %s failed." % table[0])
                logger.error(str(ex))
            sys.exit(1)

    def cleanup(self, conn):
        query = """SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='flexible_freeze'
            and pid != pg_backend_pid()"""
        cur = conn.cursor()
        cur.execute(query)
        self.conn.close()

    def main(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        # parse the argument
        parse_arguments()

        # set times
        halt_time = time.time() + (args.run_min * 60)

        # get database list using postgres as a database
        db_list = TableUtil.get_db_list(self.conn,args.dblist, args.dbuser, args.dbhost, args.dbport, args.dbpass)

        logger.info("Flexible Freeze run starting")
        logger.info("list of databases is %s" % (', '.join(db_list)))

        # connect to each database
        time_exit = False
        table_count = 0
        db_count = 0

        for db in db_list:
            self.conn = TableUtil.create_connection(db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
            self.conn.set_isolation_level(0)
            logger.info("working on database {0}".format(db))
            if time_exit:
                break
            else:
                db_count += 1
                TableUtil.set_vacuum_cost(self.conn, args.costdelay, args.costlimit)

            # if vacuuming, get list of top tables to vacuum
            if args.vacuum:
                table_list = TableUtil.get_vacuum_list(self.conn)
            else:
                table_list = TableUtil.get_freeze_list(self.conn, args.freezeage)

            logger.info("list of tables: {l}".format(l=', '.join(table_list)))

            # for each table in list
            for table in table_list:
                logger.info("processing table {t}".format(t=table))
                # check time; if overtime, exit
                if time.time() >= halt_time:
                    logger.info("Reached time limit.  Exiting.")
                    time_exit = True
                    break

                table_count += 1
                TableUtil.vacuum(self.conn, table, halt_time, args.vacuum,args.enforcetime)

            time.sleep(args.pause_time)
            self.conn.close()

        if not time_exit:
            logger.info("All tables vacuumed.")
            logger.info("%d tables in %d databases" % (table_count, db_count))
        else:
            logger.info("Vacuuming halted due to timeout")
            logger.info("after vacuuming %d tables in %d databases" % (table_count, db_count,))

        logger.info("Flexible Freeze run complete")
        sys.exit(0)

    def execute(self):
        try:
            self.main()
        except KeyboardInterrupt:
            self.cleanup(self.conn)


if __name__ == "__main__":
    TableUtil().execute()
