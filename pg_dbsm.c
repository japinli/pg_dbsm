/*----------------------------------------------------------------------------
 *
 * pg_dbsm.c
 *    Monitor the databse size.
 *
 * Copyright (c) 2019, Japin Li <japinli@hotmail.com>.
 *
 *----------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"


PG_MODULE_MAGIC;

void            _PG_init(void);

/* GUC variables */
static char *dbsm_database;
static int   dbsm_naptime;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

void		dbsm_main(Datum) pg_attribute_noreturn();

static void initialize_dbsm_object(void);


void
_PG_init(void)
{
    BackgroundWorker    worker;

    if (!process_shared_preload_libraries_in_progress)
        return;

    DefineCustomStringVariable("dbsm.database",
                               "Database used for monitor database size",
                               "Database used to check the database size (default: postgres).",
                               &dbsm_database,
                               "postgres",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);
    DefineCustomIntVariable("dbsm.naptime",
                            "Duration between each check (in seconds).",
                            NULL,
                            &dbsm_naptime,
                            60 * 60 * 24,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    /* See: see: https://www.postgresql.org/docs/current/bgworker.html */
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "pg_dbsm");
    sprintf(worker.bgw_function_name, "dbsm_main");
    worker.bgw_notify_pid = 0;
    snprintf(worker.bgw_name, BGW_MAXLEN, "database size monitor");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_dbsm");
    worker.bgw_main_arg = Int32GetDatum(0);

    RegisterBackgroundWorker(&worker);
}

/*
 * Signal handler for SIGTERM
 *              Set a flag to let the main loop to terminate, and set our latch to wake
 *              it up.
 */
static void
dbsm_sigterm(SIGNAL_ARGS)
{
        int save_errno = errno;

        got_sigterm = true;
        SetLatch(MyLatch);

        errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *              Set a flag to tell the main loop to reread the config file, and set
 *              our latch to wake it up.
 */
static void
dbsm_sighup(SIGNAL_ARGS)
{
        int save_errno = errno;

        got_sighup = true;
        SetLatch(MyLatch);

        errno = save_errno;
}

void
dbsm_main()
{
    StringInfoData    buf;

    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, dbsm_sighup);
    pqsignal(SIGTERM, dbsm_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection(dbsm_database, NULL, 0);

    initialize_dbsm_object();

    initStringInfo(&buf);
    appendStringInfo(&buf,
                     "WITH m AS (SELECT DISTINCT ON (datname) datname, "
                     "datsize FROM dbsm ORDER BY datname, created DESC) "
                     "INSERT INTO dbsm SELECT d.datname, now(), "
                     "pg_database_size(d.datname) AS datsize, "
                     "CASE WHEN m.datsize = NULL THEN 0 "
                     "ELSE pg_database_size(d.datname) - m.datsize END AS incsize "
                     "FROM pg_database d LEFT JOIN m ON m.datname = d.datname;");

    /*
     * Main loop: do this until the SIGTERM handler tells us to terminate
     */
    while (!got_sigterm)
    {
        int    ret;

        /*
         * Background workers mustn't call usleep() or any direct equivalent:
         * instead, they may wait on their process latch, which sleeps as
         * necessary, but is awakened if postmaster dies.  That way the
         * background process goes away immediately in an emergency.
         */
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                         dbsm_naptime * 1000L,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        /*
         * In case of a SIGHUP, just reload the configuration.
         */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /*
         * Start a transaction on which we can run queries.  Note that each
         * StartTransactionCommand() call should be preceded by a
         * SetCurrentStatementStartTimestamp() call, which sets both the time
         * for the statement we're about the run, and also the transaction
         * start time.  Also, each other query sent to SPI should probably be
         * preceded by SetCurrentStatementStartTimestamp(), so that statement
         * start time is always up to date.
         *
         * The SPI_connect() call lets us run queries through the SPI manager,
         * and the PushActiveSnapshot() call creates an "active" snapshot
         * which is necessary for queries to have MVCC data to work on.
         *
         * The pgstat_report_activity() call makes our activity visible
         * through the pgstat views.
         */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, buf.data);

        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_INSERT)
            elog(FATAL, "execute: \"%s\" failed", buf.data);

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat(false);
        pgstat_report_activity(STATE_IDLE, NULL);
    }

    pfree(buf.data);
    proc_exit(0);
}

static void
initialize_dbsm_object(void)
{

    int             ret;
    StringInfoData  buf;

    initStringInfo(&buf);
    appendStringInfo(&buf,
                     "CREATE TABLE IF NOT EXISTS dbsm ( "
                     "datname NAME NOT NULL, created TimestampTz DEFAULT now(), "
                     "datsize int8 NOT NULL, "
                     "incsize int8);"
                     "CREATE INDEX IF NOT EXISTS idx_datname_created ON "
                     "dbsm USING BTREE(datname, created);");

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_activity(STATE_RUNNING, buf.data);

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        elog(FATAL, "execute: \"%s\" failed", buf.data);

    resetStringInfo(&buf);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);

    pfree(buf.data);
}
