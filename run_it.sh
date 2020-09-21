#! /bin/bash

python -m triangulation assignee_76_16.csv
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_a_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_b_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_c_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_e_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_d_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_f_models.sql
sqlite3 ./triangulation/prdn_db.db < /triangulation/src/tmp/create_crosswalk.sql
