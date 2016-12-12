azm_db_merge usage instructions
===============================

Use azm_db_merge.py to merge (import) .azm files to a target database.

Please follow SETUP.txt to setup all requirements/dependencies first.

For a full list of options plesase use cmd:
python azm_db_merge.py --help


merge (import) 
--------------

Specify --azm_file <file.azm> to import the .azm's log database to a central Database.

This operation will CREATE (if requireD), ALTER (if new columns are detected)
and INSERT data from all tables in the 'azqdata.db' of the azm log file into
the target (central) database.

Note: A .azm file is simply a renamed zip file so you can open/extract with
any zip manager software to view the azqdata.db file with any SQLite3 
browser program on PC.

Note: For a list of all 'elements' (which form tables through binding to columns) of azm's azqdata.db please refer to:
https://docs.google.com/spreadsheets/d/1ddl-g_qyoMYLF8PMkjrYPrpXusdinTZxsWLQOzJ6xu8/

You need to specify the --target_db_type and its ODBC login settings too.
(for SQLite3 merges - specify all login, password, database as "" - not used).


Microsoft SQL Server import/merge and unmerge:
---------------------------------------------

The current azm_db_merge SQL Server implementation (through pyodbc + "SQL Server Natve Client 11.0" ODBC driver)
 has full support for all azm_db_merge features:
- auto table create
- if table already exists in server, auto add of coulmns found in .azm to server
- very fast import speed through bulk insert operations. (A 1 hour lte/wcdma drive takes about 10 seconds to import).
- prevent duplicate .azm imports.
- unmerge support.
- merge/unmerge transactions are atomic.

Merge (import all data from .azm log) example command:
python azm_db_merge.py --azm_file "358096071732800 16_11_2016 17.14.15.azm" --target_db_type mssql --server_user azqdblogin --server_password pass --server_database azqdb

Unmerge (delete all data in target database that came from .azm log) example command:
python azm_db_merge.py --unmerge --azm_file "358096071732800 16_11_2016 17.14.15.azm" --target_db_type mssql --server_user azqdblogin --server_password pass --server_database azqdb


sqlite3 import/merge .azm example
---------------------------------

In below example we import one azm into 'merged.db':
python azm_db_merge.py --target_db_type sqlite3 --azm_file mod.azm --server_user "" --server_password "" --server_database "" --target_sqlite3_file merged.db

Note: the sqlite3 merge option is very early and does not have any CREATE, ALTER checks and no --unmerge support too.
So currently the merging of second, third files into the 'merged.db' would work
 but be reported as fail because there are no 'create if not exist' checks yet.
 Also, for sqlite3 target dbs - merging of a second file must be from the same
 azq app version only because there are no coulmn diff checks yet.
 
 ---
 
'''
Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.
Author: Kasidit Yusuf <kasidit@azenqos.com>, <ykasidit@gmail.com>
'''

 
