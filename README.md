# pg_dbsm

`pg_dbsm` is a PostgreSQL extension that monitor the database size. 

It use the PostgreSQL background worker to collect database information
periodically, and store the information into `dbsm` table which is in
`postgres` database.
