EXTENSION = pg_dbsm        # the extensions name
DATA = pg_dbsm--0.0.1.sql  # script files to install
PGFILEDESC = "pg_dbsm - PostgreSQL database size monitor"
MODULES = pg_dbsm

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
