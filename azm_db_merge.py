infostr = '''azm_db_merge version 1.0 Copyright (c) 2016 Freewill FX Co., Ltd. All rights reserved.'''
usagestr = '''
Merge (import) AZENQOS Android .azm
test log files that contain SQLite3 database files (azqdata.db) into a target
central database (Now MS-SQL and SQLite3 only, later/roadmap: PostgreSQL and MySQL).\n

Please read SETUP.txt and INSTRUCTIONS.txt for usage examples.

Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.

'''

import subprocess
from subprocess import call
import sys
import signal
import argparse
import importlib
import time
import debug_helpers
from debug_helpers import dprint
import zipfile
import os
import shutil
import uuid
import traceback
import fnmatch
import hashlib
from datetime import datetime
from datetime import timedelta

# global vars
g_target_db_types = ['postgresql','mssql','sqlite3']
g_check_and_dont_create_if_empty = False

def parse_cmd_args():
    parser = argparse.ArgumentParser(description=infostr, usage=usagestr,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--azm_file',help='''An AZENQOS Android .azm file (or directory that contains multiple .azm files)
    that contains the SQLite3 "azqdata.db" to merge/import. If you want azm_db_merge to try find from multiple full paths, using whichever is first present, separate the strings with a comma.
    (a .azm is actually a zip file)''', required=True)
    
    parser.add_argument('--unmerge',
                        action='store_true',
                        help="un-merge mode: remove all rows of this azm from target_db.",
                        default=False)
    
    parser.add_argument('--folder_mode_stop_on_first_failure',
                        action='store_true',
                        help="""If --azm_file supplied was a folder,
                        by default it would not stop in the first failed azm.
                        Set this to make it stop at the first .azm that failed to merge/import.
                        """,
                        default=False)
    
    parser.add_argument('--target_db_type', choices=g_target_db_types,
                        help="Target DBMS type ", required=True)

    parser.add_argument(
        '--pg_port', help='''Specify postgres port number''',
        type=int,
        default=5432,
        required=False
    )
    
    parser.add_argument('--target_sqlite3_file', 
                        help="Target sqlite3 file (to create) for merge", required=False)


    parser.add_argument('--docker_postgres_server_name',
                        help="Same as --pg_host option. If azm_db_merge is running in a 'Docker' container and postgres+postgis is in another - use this to specify the server 'name'. NOTE: The azm file/folder path must be in a shared folder (with same the path) between this container and the postgres container as we're using the COPY command that requires access to a 'local' file on the postgres server.",
                        required=False,
                        default=None)

    parser.add_argument('--pg_host',
                        help="Postgres host (default is localhost)",
                        required=False,
                        default='localhost')
    
    parser.add_argument('--server_user',
                        help="Target login: username.", required=True)
    
    parser.add_argument('--server_password',
                        help="Target login: password.", required=True)
    
    parser.add_argument('--server_database',
                        help="Target database name.", required=True)
    
    parser.add_argument('--check_and_dont_create_if_empty',
                        action='store_true',
                        help="Force check and omit table create if table is empty. This check, however, can make processing slower than default behavior.",
                        default=False)
    
    parser.add_argument('--sqlite3_executable',
                        help="Full path to sqlite3 executable.",
                        default="sqlite3")
    
    parser.add_argument('--mssql_odbc_driver',
                        help="Driver string for SQL Server",
                        default="{SQL Server Native Client 11.0}")

    parser.add_argument('--dump_to_file_mode',
                        action='store_true',
                        help="""Set this to force full dump of sqlite3 db to .sql file
                        first before reading and parsing.
                        (We found that this is a bit slower and taks some disk space).""",
                        default=False)

    parser.add_argument('--exclude_tables',
                        help="""List tables to exclude from merge - separated by commas. Default tables 'spatial_ref_sys,geometry_columns' are always ommitted.""",
                        default='')

    parser.add_argument('--only_tables',
                        help="""List tables to "import only these tables". Default tables 'logs,' are always imported.""",
                        default='')

    parser.add_argument('--import_geom_column_in_location_table_only',
                        action='store_true',
                        help="""Omit 'geom' geometry column in all tables except the 'location' table.
                        By default, all tables contain the 'geom' geometry column for convenient use with QGIS.
                        However, all other tables have 'pos_id' that can be used with 'log_hash' to join/match with the 'location' table manually
                        (or as a view) if user decides to avoid this redundant data.""",
                        default=False)

    parser.add_argument('--call_preprocess_func_in_module_before_import',
                        help="""Specify a python module (.py file) that has the function 'preprocess(dir_processing_azm)' to be called before importing the 'azqdata.db' file. If you have multiple modules/functions to preprocess - simply make and specify a module that calls all of them.""",
                        default=None)

    parser.add_argument('--dry',
                        help="""specify the string 'true' (without quotes) to skip the target database procedure - designed just for looping to call the "preprocess" func mode (without unzipping the azm) where real import is not required like using with legacy AzqGen.exe calls to import to legacy mysql db with legacy schemas.""",
                        default='',
                        required=False)

    parser.add_argument('--move_imported_azm_files_to_folder',
                        help='''If specified, succssfully imported azm files would get moved to that folder.''',
                        default=None,
                        required=False)
    

    parser.add_argument('--move_failed_import_azm_files_to_folder',
                        help='''If specified, failed-to-import azm files would get moved to that folder.''',
                        default=None,
                        required=False)

    parser.add_argument('--pg_schema',
                        help='''If specified, will create/use this schema for all tables.''',
                        default="public",
                        required=False)

    parser.add_argument('--daemon_mode_rerun_on_folder_after_seconds',
                        help='''If specified, azm_db_merge will block re-run on the same folder (specified with '--azm_file') again after the specified number of seconds.''',
                        default=None,
                        required=False)

    parser.add_argument('--add_imei_id_to_all_tables',
                        action='store_true',
                        help="""Add log device's IMEI to all rows in all tables.""",
                        default=False)


    parser.add_argument('--debug',
                        action='store_true',
                        help="""Set debug (verbose) mode printing.
                        """,
                        default=False)

    parser.add_argument('--keep_temp_dir',
                        action='store_true',
                        help="""Dont delete temp dirs (that holds csv dumps) at end for further manual analysis.
                        """,
                        default=False)

    parser.add_argument('--dump_parquet',
                        action='store_true',
                        help="""Use Pandas to dump each table to Parquet files.
                        """,
                        default=False)


    parser.add_argument('--get_schema_shasum_and_exit',
                        action='store_true',
                        help="""Compute the schema of the azqdata.db inside the specified azm and exit. Example:
python azm_db_merge.py --target_db_type sqlite3 --azm_file example_logs/358096071732800\ 2_1_2017\ 13.12.49.azm --server_user "" --server_password "" --server_database "" --target_sqlite3_file merged.db --get_schema_shasum_and_exit
                        """,
                        default=False)

    parser.add_argument('--pg10_partition_by_month',
                        action='store_true',
                        help="""For postgresql v10 only - when create tables - do declartive partitioning by month like '2017_06' etc""",
                        default=False)

    parser.add_argument('--pg10_partition_index_log_hash',
                        action='store_true',
                        help="""For postgresql v10 only - when creating partitions, set log_hash as the index of the table""",
                        default=False)
    
    args = vars(parser.parse_args())
    return args

def is_dump_schema_only_for_target_db_type(args):
   
    # now force bulk so always schema only
    return True
    
"""
def popen_sqlite3_dump(args):
    params = [
        args['sqlite3_executable'],
        args['file']        
        ]
    
    if (is_dump_schema_only_for_target_db_type(args)):
        params.append(".schema")
    else:
        params.append(".dump")
    
    print("popen_sqlite3_dump params: "+str(params))
    sub = subprocess.Popen(
        params,
        bufsize = -1, # -1 means default of OS. If no buf then it will block sqlite and very slow
        shell=False,
        stdout=subprocess.PIPE,
        #stderr=subprocess.STDOUT
        #stderr=sys.stdout.fileno()
        )
    dprint("subporcess popen done")
    return sub
"""

# Dump db to a text sql file
def dump_db_to_sql(dir_processing_azm):
    dumped_sql_fp = "{}_dump.sql".format(args['file'])
    cmd = [
        "{}".format(args['sqlite3_executable']),
        "{}".format(args['file']),
        ".out {}".format(dumped_sql_fp.replace("\\", "\\\\")),
        ".schema" if (is_dump_schema_only_for_target_db_type(args)) else ".dump"
    ]
     
    print "cmd: ",cmd
    ret = call(cmd, shell=False)
    print "conv ret: "+str(ret)
    if (ret != 0):
        print "dump db to {} file failed - ABORT".format(dumped_sql_fp)
        return None
    
    print "dump db to {} file success".format(dumped_sql_fp)
    return dumped_sql_fp

# global vars for handle_sql3_dump_line
g_is_in_insert = False
g_is_in_create = False
g_insert_buf = ""
g_create_buf = ""

# global module functions
g_connect_function = None
g_check_if_already_merged_function = None
g_create_function = None
g_commit_function = None
g_close_function = None

# g_insert_function = None


# parse multi-line statements info one for insert, parse create, commit commands and call related funcs of target db type module
def handle_sql3_dump_line(args, line):
    global g_is_in_insert
    global g_is_in_create
    global g_insert_buf
    global g_create_buf
    global g_insert_function
        
    if g_is_in_insert is True:
        g_insert_buf = g_insert_buf + line
        
        if line.strip().endswith(");"):
            
            handle_ret = g_insert_function(args, g_insert_buf.strip())
            
            g_is_in_insert = False       
            g_insert_buf = None
                        
            # dprint("multi line insert END:")            
            
            return handle_ret
        else:
            # dprint("multi line insert still not ending - continue")
            return True

    if g_is_in_create:
        g_create_buf += line.strip()
        if line.strip().endswith(");"):
            line = g_create_buf
            print "multi line create END"  # \ng_is_in_create final line:", line
        else:
            return True
        
    is_omit_table = False
   
    if line.startswith("CREATE TABLE ") or g_is_in_create:
        g_is_in_create = False
        
        # in case user is using already 'sqlite3 merged azqdata.db' there will be the CREATE TABLE IF NOT EXISTS lines which we created - restore it...
        line = line.replace("CREATE TABLE IF NOT EXISTS ","CREATE TABLE ",1)

        if not line.strip().endswith(");"):
            print("multi line create START")
            g_is_in_create = True
            g_create_buf = line.strip()
            return True

        
        table_name = line.split(" (")[0].replace("CREATE TABLE ","").replace("\"","")
        dprint("check table_name is_omit_table: "+table_name)
        is_omit_table = table_name in args['omit_tables_array']
        dprint("is this table in --omit_tables ? "+table_name+" = "+str(is_omit_table))

        if args['only_tables_on']:
            dprint("--only_tables on - check if we should exclude this table: "+table_name)
            is_omit_table = True
            if table_name in args['only_tables_array']:
                is_omit_table = False
            dprint("--only_tables on - exclude this table? "+table_name+" = "+str(is_omit_table))
        
        
    if (
            line.startswith("CREATE TABLE ") and
            not line.startswith("CREATE TABLE android_metadata") and
            not ("_layer_statistics" in line) and
            not is_omit_table
    ):

        # get table name:
        table_name = line.split(" ")[2].replace("\"", "")

        ''' put where in select at create csv instead - delete where would be a cmd and slower too
        if not args['unmerge']:
            #print "delete all rows with wrong modem timestamp before 48h of log_start_time and log_end_time for this table:", table_name
            
            sqlstr = "delete from {} where time < '{}' or time > '{}' or time is null;".format(table_name, args['log_data_min_time'], args['log_data_max_time'])
            cmd = [args['sqlite3_executable'],args['file'],sqlstr]
            #print "call cmd:", cmd
            try:
                outstr = subprocess.check_output(cmd)
                #print "delete from ret outstr:", outstr
            except Exception as se:
                print "WARNING: delete pre y2k rows from table failed exception:", se
        '''

        print("\nprocessing: create/alter/insert for table_name: "+table_name)
        
        print "processing create at handler module..." # always create - flag override                
        handle_ret = g_create_function(args, line)
        
        
    elif (line.startswith("COMMIT;")):        
        print("\nprocessing: commit")        
        handle_ret = g_commit_function(args, line)
        return handle_ret
    elif (line.startswith("INSERT INTO")):
        
        raise Exception("ABORT: currently bulk insert mode is used so only scheme should be dumped/read... found INSERT INTO - abort")
        
        table_name = line.split(" ")[2].replace("\"", "")
        if (table_name == "android_metadata"):
            return True #omit
        
        line_stripped = line.strip() 
        if line_stripped.endswith(");"):
            # dprint("single line insert")
            handle_ret = g_insert_function(args, line_stripped)            
            return handle_ret
        else:
            dprint("multi line insert START")
            g_is_in_insert = True
            g_insert_buf = line
            return True
    else:
        # dprint "omit line: "+line
        return True

    return False


# unzip azm file to a tmp processing folder
def unzip_azm_to_tmp_folder(args):         
    
    dprint("unzip_azm_to_tmp_folder 0")
    print "args['azm_file']: "+args['azm_file']
    azm_fp = os.path.abspath(args['azm_file'])
    print "azm_fp: "+azm_fp
    
    if os.path.isfile(azm_fp):
        pass
    else:
        raise Exception("INVALID: - azm file does not exist at given path: "+str(azm_fp)+" - ABORT")        
    
    dir_azm_unpack = os.path.dirname(azm_fp)
    print "dir_azm_unpack: "+dir_azm_unpack
    azm_name_no_ext = os.path.splitext(os.path.basename(azm_fp))[0]
    print "azm_name_no_ext: "+azm_name_no_ext
    if 'TMP_GEN_PATH' in os.environ:
        dir_azm_unpack = os.environ['TMP_GEN_PATH']
        print "dir_azm_unpack using TMP_GEN_PATH:", dir_azm_unpack
    dir_processing_azm = os.path.join(dir_azm_unpack, "tmp_azm_db_merge_"+str(uuid.uuid4())+"_"+azm_name_no_ext.replace(" ","-")) # replace 'space' in azm file name
    args['dir_processing_azm'] = dir_processing_azm
    
    dprint("unzip_azm_to_tmp_folder 1")
    
    # try clear tmp processing folder just in case it exists from manual unzip or previous failed imports
    try:
        shutil.rmtree(dir_processing_azm)        
    except Exception as e:
        estr = str(e)
        if ("cannot find the path specified" in estr or "No such file or" in estr):
            pass
        else:
            print("rmtree dir_processing_azm: "+str(e))
            raise e
    
    dprint("unzip_azm_to_tmp_folder 2")
    
    os.mkdir(dir_processing_azm)
    
    dprint("unzip_azm_to_tmp_folder 3")

    
    try:
        azm = zipfile.ZipFile(args['azm_file'],'r')
        azm.extract("azqdata.db", dir_processing_azm)

        '''
        try:
            # handle malformed db cases
            
            import pandas as pd
            import sqlite3
            
            dbfile = os.path.join(dir_processing_azm, "azqdata.db")
            dbcon = sqlite3.connect(dbfile)
            integ_check_df = pd.read_sql("PRAGMA integrity_check;", dbcon)
            try:
                dbcon.close()  # we dont use dbcon in further azm_db_merge code, and db file can be removed if integ not ok - avoid file locks
            except:
                pass
            print "azm_db_merge: sqlite db integ_check_df first row:", integ_check_df.iloc[0]
            if integ_check_df.iloc[0].integrity_check == "ok":
                print "azm_db_merge: sqlite3 db integrity check ok"
            else:
                print "azm_db_merge: sqlite3 db integrity check failed - try recover..."
                dump_ret = subprocess.call("sqlite3 '{}' .dump > '{}.txt'".format(dbfile, dbfile),shell=True)
                print "azm_db_merge: dump_ret:", dump_ret
                if dump_ret != 0:
                    print "WARNING: azm_db_merge: recov corrupt sqlite db file - failed to dump sqlite db file"
                else:
                    os.remove(dbfile)
                    import_ret = subprocess.call("sqlite3 '{}' < '{}.txt'".format(dbfile, dbfile), shell=True)
                    print "azm_db_merge: recov corrupt db file import ret:", import_ret                    
        except:
            type_, value_, traceback_ = sys.exc_info()
            exstr = traceback.format_exception(type_, value_, traceback_)
            print "WARNING: check malformed db exception:", exstr
        '''
        
        if args['get_schema_shasum_and_exit']:
            print "get_schema_shasum_and_exit start"
            sha1 = hashlib.sha1()
            #print "get_schema_shasum_and_exit 1"
            dbfile = os.path.join(dir_processing_azm,"azqdata.db")
            #print "get_schema_shasum_and_exit 2"
            cmd = [args['sqlite3_executable'],dbfile,".schema"]
            print "call cmd:", cmd
            schema = subprocess.check_output(cmd)
            #print "get_schema_shasum_and_exit 3"
            sha1.update(schema)
            #print "get_schema_shasum_and_exit 4"
            print str(sha1.hexdigest())+" is the sha1 for the schema of azqdata.db inside azm: "+args['azm_file']
            print "get_schema_shasum_and_exit done"
            azm.close()
            cleanup_tmp_dir(dir_processing_azm)
            exit(0)
        azm.close()
    except Exception as e:
        try:
            cleanup_tmp_dir(dir_processing_azm)
        except:
            pass
        raise Exception("Invalid azm_file: azm file does not contain azqdata.db database - exception: "+str(e))
        
    
    dprint("unzip_azm_to_tmp_folder 4")
    
    args['file'] = os.path.join(dir_processing_azm, "azqdata.db")
    return dir_processing_azm


def cleanup_tmp_dir(dir_processing_azm):
    # clear tmp processing folder
    attempts = range(5) # 0 to 4 
    imax = len(attempts)
    print "cleanup_tmp_dir: ",dir_processing_azm
    if dir_processing_azm != None and os.path.exists(dir_processing_azm) and os.path.isdir(dir_processing_azm):
        pass
    else:
        return    
    for i in attempts:
        try:
            # print("cleaning up tmp dir...")        
            shutil.rmtree(dir_processing_azm)
            break
            # print("cleanup tmp_processing_ dir done.")
            
        except Exception as e:
            print("warning: attempt %d/%d - failed to delete tmp dir: %s - dir_processing_azm: %s" % (i, imax, e,dir_processing_azm))
            time.sleep(0.01) # sleep 10 millis
            pass


def check_azm_azq_app_version(args):
    # check version of AZENQOS app that produced the .azm file - must be at least 3.0.562    
    MIN_APP_V0 = 3
    MIN_APP_V1 = 0
    MIN_APP_V2 = 587
    sqlstr = "select log_app_version from logs" # there is always only 1 ver of AZENQOS app for 1 azm - and normally 1 row of logs per azm too - but limit just in-case to be future-proof 
    cmd = [args['sqlite3_executable'],args['file'],sqlstr]
    print "call cmd:", cmd
    outstr = subprocess.check_output(cmd).strip()
    try:
        args["azm_apk_version"] = "0.0.0"
        outstr = outstr.replace("v","") # replace 'v' prefix - like "v3.0.562" outstr
        print "azm app version outstr:", outstr
        parts = outstr.split(".")
        v0 = int(parts[0]) * 1000 * 1000
        v1 = int(parts[1]) * 1000
        v2 = int(parts[2])
        args["azm_apk_version"] = v0 + v1 + v2
        if (args["azm_apk_version"] >= MIN_APP_V0*1000*1000 + MIN_APP_V1*1000 + MIN_APP_V2):
            pass
        else:
            print "WARNING: azm too old - azm file must be from AZENQOS apps with versions {}.{}.{} or newer.".format(MIN_APP_V0,MIN_APP_V1,MIN_APP_V2)
    except:
        type_, value_, traceback_ = sys.exc_info()
        exstr = traceback.format_exception(type_, value_, traceback_)
        print "WARNING: check azm app version exception:", exstr


def mv_azm_to_target_folder(args):
    mv_target_folder = args['move_imported_azm_files_to_folder']

    if not mv_target_folder is None:
        if not os.path.exists(mv_target_folder):
            os.makedirs(mv_target_folder)        
        azm_fp = os.path.abspath(args['azm_file'])
        target_fp = os.path.join(mv_target_folder,os.path.basename(azm_fp))
        try:
            os.remove(target_fp)
            os.remove(target_fp+"_output.txt")
        except:
            pass
        print "move_imported_azm_files_to_folder: mv {} to {}".format(azm_fp,target_fp)
        os.rename(azm_fp, target_fp)
        try:
            os.rename(azm_fp+"_output.txt", target_fp+"_output.txt")
        except:
            pass

    
def process_azm_file(args):
    proc_start_time = time.time()
    ret = -9
    use_popen_mode = True
    sql_dump_file = None
            
    
    try:
        dir_processing_azm = None

        dry_str = args['dry']
        dry_str = dry_str.strip().lower()
        dry_mode = (dry_str == "true")
        print "dry_mode setting: ",dry_mode

        
        if dry_mode:
            print "dry_mode - dont unzip azm for azqdata.db - let preprocess func handle itself"
        else:
            print "normal import mode"
            dir_processing_azm = unzip_azm_to_tmp_folder(args)
            args['dir_processing_azm'] = dir_processing_azm

        preprocess_module = args['call_preprocess_func_in_module_before_import']
        
        if not preprocess_module is None:
            preprocess_module = preprocess_module.replace(".py","",1)
            print "get preprocess module: ", preprocess_module
            importlib.import_module(preprocess_module)
            mod = sys.modules[preprocess_module]
            preprocess = getattr(mod, 'preprocess')
            print "exec preprocess module > preprocess func"
            preprocess(dir_processing_azm,args['azm_file'])

        if dry_mode:
            print "dry_mode - end here"
            mv_azm_to_target_folder(args)
            return 0
            
        check_azm_azq_app_version(args)
        
        g_check_and_dont_create_if_empty = args['check_and_dont_create_if_empty']
        use_popen_mode = not args['dump_to_file_mode']
        
        if args['target_db_type'] == "sqlite3":
            if args['target_sqlite3_file'] is None:
                raise Exception("INVALID: sqlite3 merge mode requires --target_sqlite3_file option to be specified - ABORT")            
            else:
                use_popen_mode = False # dump to .sql file for .read

        print "NOTE: now we delete pre y2k rows and if it was popen then the delete would error as 'database is locked' so always dump schema to sql - so force set use_popen_mode = False"
        use_popen_mode = False

        if (use_popen_mode):
            print "using live in-memory pipe of sqlite3 dump output parse mode"
        else:
            print "using full dump of sqlite3 to file mode"
        
        dump_process = None
        dumped_sql_fp = None

        
        if (use_popen_mode):
            print("starting sqlite3 subporcess...")
            dump_process = popen_sqlite3_dump(args)
            if dump_process is None:
                raise Exception("FATAL: dump_process is None in popen_mode - ABORT")
        else:
            print("starting sqlite3 to dump db to .sql file...")
            dumped_sql_fp = dump_db_to_sql(dir_processing_azm)
            if dumped_sql_fp is None:
                raise Exception("FATAL: dumped_sql_fp is None in non popen_mode - ABORT")
        
        
        # sqlite3 merge is simple run .read on args['dumped_sql_fp']
        if args['target_db_type'] == "sqlite3":
            is_target_exists = os.path.isfile(args['target_sqlite3_file'])
            print "sqlite3 - import to {} from {}".format(args['target_sqlite3_file'], dumped_sql_fp)

            dumped_sql_fp_adj = dumped_sql_fp + "_adj.sql"
            of = open(dumped_sql_fp,"r")
            nf = open(dumped_sql_fp_adj,"wb") # wb required for windows so that \n is 0x0A - otherwise \n will be 0x0D 0x0A and doest go with our fmt file and only 1 row will be inserted per table csv in bulk inserts... 

            while True:
                ofl = of.readline()
                    
                if ofl == "":
                    break

                ofl = ofl.replace("CREATE TABLE android_metadata (locale TEXT);","",1)
                ofl = ofl.replace('CREATE TABLE "','CREATE TABLE IF NOT EXISTS "',1)

                if ofl.startswith('INSERT INTO "android_metadata"'):
                    ofl = ""
                
                if is_target_exists:
                    # dont insert or create qgis tables
                    if ofl.startswith("CREATE TABLE geometry_columns") or ofl.startswith("CREATE TABLE spatial_ref_sys") or ofl.startswith('INSERT INTO "spatial_ref_sys"') or ofl.startswith('INSERT INTO "geometry_columns"'):
                        ofl = ""

                nf.write(ofl)
                #nf.write('\n')
            nf.close()
            of.close()

            
            cmd = [
                args['sqlite3_executable'],
                args['target_sqlite3_file'],
                ".read {}".format(dumped_sql_fp_adj.replace("\\", "\\\\"))
                ]
            print "cmd: ",cmd
            ret = call(cmd, shell=False)
            print "import ret: "+str(ret)
            if (ret == 0):
                print( "\n=== SUCCESS - import completed in %s seconds" % (time.time() - proc_start_time) )
                if debug_helpers.debug == 1 or args['keep_temp_dir']:
                    print "debug mode keep_tmp_dir:", dir_processing_azm
                else:
                    cleanup_tmp_dir(dir_processing_azm)
                return 0
            else:
                if debug_helpers.debug == 1 or args['keep_temp_dir']:
                    print "debug mode keep_tmp_dir:", dir_processing_azm
                else:
                    cleanup_tmp_dir(dir_processing_azm)

                raise Exception("\n=== FAILED - ret %d - operation completed in %s seconds" % (ret, time.time() - proc_start_time))
                
            raise Exception("FATAL: sqlite3 mode merge process failed - invalid state")
            
        # now we use bulk insert done at create/commit funcs instead g_insert_function = getattr(mod, 'handle_sqlite3_dump_insert')
            
        print "### connecting to dbms..."    
        ret = g_connect_function(args)
        
        if ret == False:
            raise Exception("FATAL: connect_function failed")
            
        
        if (args['unmerge']):
            print "### unmerge mode"
            # unmerge mode would be handled by same check_if_already_merged_function below - the 'unmerge' flag is in args

        # check if this azm is already imported/merged in target db (and exit of already imported)
        # get log_hash
        sqlstr = "select log_hash from logs limit 1"
        cmd = [args['sqlite3_executable'],args['file'],sqlstr]
        print "call cmd:", cmd
        outstr = subprocess.check_output(cmd)
        log_hash = outstr.strip()
        args['log_hash'] = long(log_hash)

        if log_hash == 0:
            raise Exception("FATAL: invalid log_hash == 0 case")


        args['log_start_time_str'] = get_sql_result(
            "select log_start_time from logs limit 1",
            args
        )
        args['log_end_time_str'] = get_sql_result(
            "select log_end_time from logs limit 1",
            args
        )
        #print "args['log_end_time_str']:", args['log_end_time_str']

        
        args['log_start_time'] = get_sql_result(
            "select strftime('%s', log_start_time) from logs limit 1",
            args
        )
        print "parse log_start_time:", args['log_start_time']
        args['log_start_time'] = datetime.fromtimestamp(long(args['log_start_time']))
        print "args['log_start_time']:", args['log_start_time']
        print "args['log_start_time_str']:", args['log_start_time_str']

        args['log_end_time'] = get_sql_result(
            "select strftime('%s', log_end_time) from logs limit 1",
            args
        )

        # some rare cases older apks log_end_time somehow didnt get into db
        if not args['log_end_time']:
            args['log_end_time'] = get_sql_result(
                "select strftime('%s', max(time)) from android_info_1sec",
                args
            )            
        
        print "parse log_end_time:", args['log_end_time']
        args['log_end_time'] = datetime.fromtimestamp(long(args['log_end_time']))
        print "args['log_end_time']:", args['log_end_time']
        print "args['log_end_time_str']:", args['log_end_time_str']

        args['log_data_min_time'] = args['log_start_time'] - timedelta(hours=48)
        print "args['log_data_min_time']:", args['log_data_min_time']
        
        args['log_data_max_time'] = args['log_end_time'] + timedelta(hours=48)
        print "args['log_data_max_time']:", args['log_data_max_time']


        if log_hash == 0:
            raise Exception("FATAL: invalid log_hash == 0 case")

        g_check_if_already_merged_function(args, log_hash)
                
        ''' now we're connected and ready to import, open dumped file and hadle CREATE/INSERT
        operations for current target_type (DBMS type)'''
        
        if (use_popen_mode == False):
            sql_dump_file = open(dumped_sql_fp, 'rb')
        
        # output for try manual import mode
        # args['out_sql_dump_file'] = open("out_for_dbtype_{}.sql".format(args['file']), 'w')
        
        dprint("entering main loop")
        
        n_lines_parsed = 0
        while(True):
            if (use_popen_mode):
                line = dump_process.stdout.readline()        
            else:
                line = sql_dump_file.readline()
            dprint("read line: "+line)
            # when EOF is reached, we'd get an empty string
            if (line == ""):
                print "\nreached end of file/output"
                break
            else:
                n_lines_parsed = n_lines_parsed + 1
                handle_sql3_dump_line(args, line)
        
        
        # finally call commit again in case the file didn't have a 'commit' line at the end
        print "### calling handler's commit func as we've reached the end..."
        
        handle_ret = g_commit_function(args, line)
        
        # call close() for that dbms handler   
    
        operation = "merge/import"
        if (args['unmerge']):
            operation = "unmerge/delete"
            
        if (n_lines_parsed != 0):
            print( "\n=== SUCCESS - %s completed in %s seconds - tatal n_lines_parsed %d (not including bulk-inserted-table-content-lines)" % (operation, time.time() - proc_start_time, n_lines_parsed) )
            ret =  0
            mv_azm_to_target_folder(args)
        else:            
            raise Exception("\n=== FAILED - %s - no lines parsed - tatal n_lines_parsed %d operation completed in %s seconds ===" % (operation, n_lines_parsed, time.time() - proc_start_time))
        
    
    except Exception as e:
        type_, value_, traceback_ = sys.exc_info()
        exstr = traceback.format_exception(type_, value_, traceback_)

        mv_target_folder = args['move_failed_import_azm_files_to_folder']            
        if not mv_target_folder is None and not os.path.exists(mv_target_folder):
            os.makedirs(mv_target_folder)            
        if not mv_target_folder is None:
            azm_fp = os.path.abspath(args['azm_file'])
            target_fp = os.path.join(mv_target_folder,os.path.basename(azm_fp))
            try:
                os.remove(target_fp)
                os.remove(target_fp+"_output.txt")
            except Exception as x:
                pass
            
            print "move the failed_import_azm_files_to_folder: mv {} to {}".format(azm_fp,target_fp)
            try:
                os.rename(azm_fp, target_fp)
                try:
                    os.rename(azm_fp+"_output.txt", target_fp+"_output.txt")
                except:
                    pass
            except Exception as x:
                print "WARNING: move_failed_import_azm_files_to_folder failed"
                pass

    
        print "re-raise exception e - ",exstr
        raise e
    
    finally:
        print "cleanup start..."
        if (use_popen_mode):
            # clean-up dump process
            try:
                dump_process.kill()
                dump_process.terminate()
            except:
                pass
        else:
            try:
                sql_dump_file.close()
            except:
                pass
        try:
            g_close_function(args)
        except:
            pass
        
        if debug_helpers.debug == 1 or args['keep_temp_dir']:
            print "debug mode keep_tmp_dir:", dir_processing_azm
            pass # keep files for analysis of exceptions in debug mode
        else:
            print "cleanup_tmp_dir..."
            cleanup_tmp_dir(dir_processing_azm)
    
    return ret


def sigterm_handler(_signo, _stack_frame):
    print "azm_db_merge.py: received SIGTERM - exit(0) now..."
    sys.exit(0)
    return


def get_sql_result(sqlstr, args):
    cmd = [args['sqlite3_executable'],args['file'],sqlstr]
    print "get_sql_result cmd:", cmd
    outstr = subprocess.check_output(cmd)
    result = outstr.strip()
    return result


if __name__ == '__main__':

    #################### Program START

    print infostr

    signal.signal(signal.SIGTERM, sigterm_handler)

    args = parse_cmd_args()
    # must be localhost only because now we're using BULK INSERT (or COPY) commands

    if args['docker_postgres_server_name'] is not None:
        args['pg_host'] = args['docker_postgres_server_name']

    if (args['unmerge']):
        print "starting with --unmerge mode"

    print "checking --sqlite3_executable: ",args['sqlite3_executable']
    try:
        cmd = [
            args['sqlite3_executable'],
            "--version"
        ]
        ret = call(cmd, shell=False)
        if ret == 0:
            print "sqlite3_executable working - OK"
        else:
            raise Exception("Secified (or default) --sqlite3_executable not working - ABORT")
    except Exception as e:
        estr = str(e)
        print "error - sqlite3 check exception estr: ",estr
        if "The system cannot find the file specified" in estr:
            print "windows run: can't call specified sqlite3_executable - trying use 'where' to find the default 'sqlite3' executable."
            outstr = subprocess.check_output(
                ["cmd.exe",
                 "/c",
                 "where",
                 "sqlite3"
                ]
            )
            print "where returned: ",outstr.strip()
            print "blindly using where return val as sqlite3 path..."
            args['sqlite3_executable'] = outstr.strip()
            cmd = [
                args['sqlite3_executable'],
                "--version"
            ]
            ret = call(cmd, shell=False)
            if ret == 0:
                print "sqlite3_executable working - OK"
            else:
                raise Exception("Secified (or default) --sqlite3_executable not working - ABORT")
        else:
            args['sqlite3_executable'] = "./sqlite3"
            cmd = [
                args['sqlite3_executable'],
                "--version"
            ]
            ret = call(cmd, shell=False)
            if ret == 0:
                print "sqlite3_executable working - OK"
            else:
                raise Exception("Failed to find sqlite3 - please install sqlite3 and make sure it is in the path first. exception:"+str(e))


    omit_tables = "spatial_ref_sys,geometry_columns,"+args['exclude_tables']
    omit_tables_array = omit_tables.split(",")
    args['omit_tables_array'] = omit_tables_array

    only_tables = "logs,"+args['only_tables']
    only_tables_array = only_tables.split(",")
    args['only_tables_array'] = only_tables_array

    if only_tables == "logs,": # logs is default table - nothing added
        args['only_tables_on'] = False
    else:
        args['only_tables_on'] = True

    if not args['move_imported_azm_files_to_folder'] is None:
        try:
            os.mkdir(args['move_imported_azm_files_to_folder'])
        except:
            pass # ok - folder likely already exists...
        if os.path.isdir(args['move_imported_azm_files_to_folder']):
            pass
        else:
            raise Exception("ABORT: Can't create or access folder specified by --move_imported_azm_files_to_folder: "+str(args['move_imported_azm_files_to_folder']))

    mod_name = args['target_db_type']
    if args['debug']:
        print "set_debug 1"
        debug_helpers.set_debug(1)
    else:
        print "set_debug 0"
        debug_helpers.set_debug(0)

    #if  mod_name in ['postgresql','mssql']:
    mod_name = 'gen_sql'
    mod_name = mod_name + "_handler"
    print "### get module: ", mod_name
    importlib.import_module(mod_name)
    mod = sys.modules[mod_name]
    #print "module dir: "+str(dir(mod))

    g_connect_function = getattr(mod, 'connect')
    g_check_if_already_merged_function = getattr(mod, 'check_if_already_merged')
    g_create_function = getattr(mod, 'create')
    g_commit_function = getattr(mod, 'commit')
    g_close_function = getattr(mod, 'close')

    if "," in args['azm_file']:
        print "found comman in args['azm_file'] - split and use whichever is first present in the list"
        csv = args['azm_file']
        found = False
        for fp in csv.split(","):
            if os.path.isfile(fp):
                args['azm_file'] = fp
                print "using valid azm_file file path:", args['azm_file']
                found = True
                break
        if not found:
            raise Exception("Failed to find any valid existing azm file in supplied comma separated --azm_file option:"+str(args['azm_file']))   

    azm_file_is_folder = os.path.isdir(args['azm_file'])

    folder_daemon = not args['daemon_mode_rerun_on_folder_after_seconds'] is None
    folder_daemon_wait_seconds = 60
    if folder_daemon:
        if not azm_file_is_folder:
            raise Exception("ABORT: --daemon_mode_rerun_on_folder_after_seconds specified but --azm_file is not a folder.")
        folder_daemon_wait_seconds = int(args['daemon_mode_rerun_on_folder_after_seconds'])
        print "folder_daemon_wait_seconds: ",folder_daemon_wait_seconds
        if folder_daemon_wait_seconds <= 0:
            raise Exception("ABORT: --daemon_mode_rerun_on_folder_after_seconds option must be greater than 0.")
    ori_args = args

    if args['add_imei_id_to_all_tables']:
        # get imei
        imei = None
        col = "IMEI"
        table = "log_info"
        where = "where {} != ''".format(col) # not null and not empty
        sqlstr = "select {} from {} {} order by seqid desc limit 1;".format(col, table, where)
        cmd = [args['sqlite3_executable'],args['file'],sqlstr]
        print "call cmd:", cmd
        imei = subprocess.check_output(cmd).strip()
        args['imei'] = imei

    while(True):

        process_start_time = time.time()

        args = ori_args.copy() # args gets modified by each run - especially ['azm_file'] gets changed - so we want to use a copy of the original_args here (otherwise args would get modified and we won't be able to restore to the original for daemon_mon

        azm_files = []
        # check if supplied 'azm_file' is a folder - then iterate over all azms in that folder
        if azm_file_is_folder:
            dir = args['azm_file']
            print "supplied --azm_file: ",dir," is a directory - get a list of .azm files to process:"
            matches = []
            # recurse as below instead azm_files = glob.glob(os.path.join(dir,"*.azm"))
            # http://stackoverflow.com/questions/2186525/use-a-glob-to-find-files-recursively-in-python
            for root, dirnames, filenames in os.walk(dir):
                for filename in fnmatch.filter(filenames, '*.azm'):
                    matches.append(os.path.join(root, filename))
            azm_files = matches
        else:
            azm_files = [args['azm_file']]

        nazm = len(azm_files)
        print "n_azm_files to process: {}".format(nazm)
        print "list of azm files to process: "+str(azm_files)
        iazm = 0
        ifailed = 0
        ret = -1
        had_errors = False

        for azm in azm_files:
            iazm = iazm + 1
            args['azm_file'] = azm
            print "## START process azm {}/{}: '{}'".format(iazm, nazm, azm)
            try: 
                ret = process_azm_file(args)
                if (ret != 0):
                    raise Exception("ABORT: process_azm_file failed with ret code: "+str(ret))        
                print "## DONE process azm {}/{}: '{}' retcode {}".format(iazm, nazm, azm, ret)        
            except Exception as e:
                ifailed = ifailed + 1
                had_errors = True
                type_, value_, traceback_ = sys.exc_info()
                exstr = traceback.format_exception(type_, value_, traceback_)
                print "## FAILED: process azm {} failed with below exception:\n(start of exception)\n{}\n{}(end of exception)".format(azm,str(e),exstr)
                if (args['folder_mode_stop_on_first_failure']):
                    print "--folder_mode_stop_on_first_failure specified - exit now."
                    exit(-9)

        if (had_errors == False):
            print "SUCCESS - operation completed successfully for all azm files (tatal: %d) - in %.03f seconds." % (iazm,  time.time() - process_start_time)
        else:
            print "COMPLETED WITH ERRORS - operation completed but had encountered errors (tatal: %d, failed: %d) - in %.03f seconds - (use --folder_mode_stop_on_first_failure to stop on first failed azm file)." % (iazm,ifailed, time.time() - process_start_time)
        if not folder_daemon:
            print "exit code:",str(ret)
            exit(ret)
        else:
            print "*** folder_daemon mode: wait seconds: ",folder_daemon_wait_seconds
            for i in range(0,folder_daemon_wait_seconds):
                print "** folder_daemon mode: waiting ",i,"/",folder_daemon_wait_seconds," seconds"
                time.sleep(1)
