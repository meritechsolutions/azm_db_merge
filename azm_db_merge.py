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
import glob
import traceback
import fnmatch


# global vars
g_target_db_types = ['postgresql','mssql','sqlite3']
g_check_and_dont_create_if_empty = False

def parse_cmd_args():
    parser = argparse.ArgumentParser(description=infostr, usage=usagestr,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--azm_file',help='''An AZENQOS Android .azm file (or directory that contains multiple .azm files)
    that contains the SQLite3 "azqdata.db" to merge/import.
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
    
    parser.add_argument('--target_sqlite3_file', 
                        help="Target sqlite3 file (to create) for merge", required=False)

    """ must be localhost only because now we're using BULK INSERT (or COPY) commands
    parser.add_argument('--server_url',
                        help="Target DBMS Server URL (domain or ip).",
                        required=False, default="localhost")
    """

    parser.add_argument('--docker_postgres_server_name',
                        help="If azm_db_merge is running in a 'Docker' container and postgres+postgis is in another - use this to specify the server 'name'. NOTE: The azm file/folder path must be in a shared folder (with same the path) between this container and the postgres container as we're using the COPY command that requires access to a 'local' file on the postgres server.",
                        required=False,
                        default=None)

    
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
                        help="""Specify a pyhon module (.py file) that has the function 'preprocess(dir_processing_azm)' to be called before importing the 'azqdata.db' file. If you have multiple modules/functions to preprocess - simply make and specify a module that calls all of them.""",
                        default=None)

    parser.add_argument('--move_imported_azm_files_to_folder',
                        help='''If specified, succssfully imported azm files would get moved to that folder.''',
                        default=None,
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


    
    args = vars(parser.parse_args())
    return args

def is_dump_schema_only_for_target_db_type(args):
    
    # now force bulk so always schema only
    return True
    
    if (args['unmerge']):
        return True
    
    if (args['target_db_type'] == 'mssql' and not args['mssql_local_bulk_insert_mode_disable'] == True):
        return True
    
    return False


def popen_sqlite3_dump(args):
    params = [
        args['sqlite3_executable'],
        args['file']        
        ]
    
    if (is_dump_schema_only_for_target_db_type(args)):
        params.append(".schema")
    else:
        params.append(".dump")
    
    dprint("popen_sqlite3_dump params: "+str(params))
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

# Dump db to a text sql file
def dump_db_to_sql(dir_processing_azm):
    dumped_sql_fp = "{}_dump.sql".format(args['file'])
    cmd = [
        "{}".format(args['sqlite3_executable']),
        "{}".format(args['file']),
        ".out {}".format(dumped_sql_fp.replace("\\", "\\\\")),
        ".dump"
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
g_insert_buf = ""

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
    global g_insert_buf
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
        
    is_omit_table = False
   
    if line.startswith("CREATE TABLE "):

        # in case user is using already 'sqlite3 merged azqdata.db' there will be the CREATE TABLE IF NOT EXISTS lines which we created - restore it...
        line = line.replace("CREATE TABLE IF NOT EXISTS ","CREATE TABLE ",1)
        
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
            not is_omit_table
    ):

        # get table name:
        table_name = line.split(" ")[2].replace("\"", "")
        
        print("\nprocessing: create/alter/insert for table_name: "+table_name)
        
        # check if create is required for this table (omit if empty)
        create = True        
        if (not g_check_and_dont_create_if_empty):
            pass # print "create_empty_tables specified in option - do create" # always create - flag override
        else:
            # checking can make final processing slower...
            print("checking if table is empty ...")
            sqlstr = "SELECT 1 FROM {} LIMIT 1".format(table_name)
            cmd = [args['sqlite3_executable'],args['file'],sqlstr]
            outstr = subprocess.check_output(cmd)
            # print "check has_rows out: "+outstr
            has_rows = (outstr.strip() == "1")
            # print "check has_rows ret: " + str(has_rows)
            if (has_rows):
                print "table is not empty - do create"
                create = True
            else:
                print "table is empty - omit create"
                create = False            
        
        if create:
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
            # dprint("multi line insert START")
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
    
    dir_azm = os.path.dirname(azm_fp)
    print "dir_azm: "+dir_azm
    azm_name_no_ext = os.path.splitext(os.path.basename(azm_fp))[0]
    print "azm_name_no_ext: "+azm_name_no_ext
    dir_processing_azm = os.path.join(dir_azm, "tmp_process_"+azm_name_no_ext.replace(" ","-")) # replace 'space' in azm file name 
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
        azm.close()
    except:
        os.rmdir(dir_processing_azm) # cleanup
        raise Exception("Invalid azm_file: azm file does not contain azqdata.db database.")
        
    
    dprint("unzip_azm_to_tmp_folder 4")
    
    args['file'] = os.path.join(dir_processing_azm, "azqdata.db")
    return dir_processing_azm


def cleanup_tmp_dir(dir_processing_azm):
    # clear tmp processing folder
    attempts = range(5) # 0 to 4 
    imax = len(attempts)
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
    outstr = subprocess.check_output(cmd).strip()
    outstr = outstr.replace("v","") # replace 'v' prefix - like "v3.0.562" outstr
    parts = outstr.split(".")
    v0 = int(parts[0])
    v1 = int(parts[1])
    v2 = int(parts[2])
    if (v0 >= MIN_APP_V0 and v1 >= MIN_APP_V1 and v2 >= MIN_APP_V2):
        pass
    else:
        raise Exception("Invalid azm_file: the azm file must be from AZENQOS apps with versions {}.{}.{} or newer.".format(MIN_APP_V0,MIN_APP_V1,MIN_APP_V2))
        
def process_azm_file(args):
    proc_start_time = time.time()
    ret = -9
    use_popen_mode = True
    sql_dump_file = None
            
    
    try:
        dir_processing_azm = None
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
            preprocess(dir_processing_azm)
        
        check_azm_azq_app_version(args)
        
        g_check_and_dont_create_if_empty = args['check_and_dont_create_if_empty']
        use_popen_mode = not args['dump_to_file_mode']
        
        if args['target_db_type'] == "sqlite3":
            if args['target_sqlite3_file'] is None:
                raise Exception("INVALID: sqlite3 merge mode requires --target_sqlite3_file option to be specified - ABORT")            
            else:
                use_popen_mode = False # dump to .sql file for .read 
            
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
                if debug_helpers.debug == 1:
                    print "debug mode keep_tmp_dir..."
                else:
                    cleanup_tmp_dir(dir_processing_azm)
                return 0
            else:
                if debug_helpers.debug == 1:
                    print "debug mode keep_tmp_dir..."
                else:
                    cleanup_tmp_dir(dir_processing_azm)

                raise Exception("\n=== FAILED - ret %d - operation completed in %s seconds" % (ret, time.time() - proc_start_time))
                
            raise Exception("FATAL: sqlite3 mode merge process failed - invalid state")
            
        # now we use bulk insert done at create/commit funcs instead g_insert_function = getattr(mod, 'handle_sqlite3_dump_insert')
            
        print "### connecting to dbms..."    
        ret = g_connect_function(args)
        
        if ret == False:
            raise Exception("FATAL: connect_function failed")
            
        # check if this azm is already imported/merged in target db (and exit of already imported)
        # get log_ori_file_name
        sqlstr = "select log_ori_file_name from logs limit 1"
        cmd = [args['sqlite3_executable'],args['file'],sqlstr]
        outstr = subprocess.check_output(cmd)
        log_ori_file_name = outstr.strip()
        if (not ".azm" in log_ori_file_name):
            raise Exception("FATAL: Failed to get log_ori_file_name from logs table of this azm's db - ABORT.")
        
        if (args['unmerge']):
            print "### unmerge mode"
            # unmerge mode would be handled by same check_if_already_merged_function below - the 'unmerge' flag is in args
        
        g_check_if_already_merged_function(args, log_ori_file_name)
        
        ''' now we're connected and ready to import, open dumped file and hadle CREATE/INSERT
        operations for current target_type (DBMS type)'''
        
        if (use_popen_mode == False):
            sql_dump_file = open(dumped_sql_fp, 'r')
        
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
            
            mv_target_folder = args['move_imported_azm_files_to_folder']
            
            if not os.path.exists(mv_target_folder):
                os.makedirs(mv_target_folder)

            if not mv_target_folder is None:
                azm_fp = os.path.abspath(args['azm_file'])
                target_fp = os.path.join(mv_target_folder,os.path.basename(azm_fp))
                try:
                    os.remove(target_fp)
                except:
                    pass
                print "move_imported_azm_files_to_folder: mv {} to {}".format(azm_fp,target_fp)
                os.rename(azm_fp, target_fp)
        else:            
            raise Exception("\n=== FAILED - %s - no lines parsed - tatal n_lines_parsed %d operation completed in %s seconds ===" % (operation, n_lines_parsed, time.time() - proc_start_time))
        
    
    except Exception as e:
        type_, value_, traceback_ = sys.exc_info()
        exstr = traceback.format_exception(type_, value_, traceback_)
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
        g_close_function(args)
        if debug_helpers.debug == 1:
            print "debug mode keep_tmp_dir..."
            pass # keep files for analysis of exceptions in debug mode
        else:
            print "cleanup_tmp_dir..."
            cleanup_tmp_dir(dir_processing_azm)
    
    return ret


def sigterm_handler(_signo, _stack_frame):
    print "azm_db_merge.py: received SIGTERM - exit(0) now..."
    sys.exit(0)
    return

#################### Program START

print infostr

signal.signal(signal.SIGTERM, sigterm_handler)

args = parse_cmd_args()
# must be localhost only because now we're using BULK INSERT (or COPY) commands
args['server_url'] = "localhost"

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
except WindowsError as e:
    estr = str(e)
    print "windowserror - sqlite3 check exception estr: ",estr
    if "The system cannot find the file specified" in estr:
        print "windows run: can't call specified sqlite3_executable - tring use 'where' to find the default 'sqlite3' executable."
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
        raise e
    

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
        exit(ret)
    else:
        print "*** folder_daemon mode: wait seconds: ",folder_daemon_wait_seconds
        for i in range(0,folder_daemon_wait_seconds):
            print "** folder_daemon mode: waiting ",i,"/",folder_daemon_wait_seconds," seconds"
            time.sleep(1)
