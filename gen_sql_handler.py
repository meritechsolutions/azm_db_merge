'''
module to handle merging (importing) of (azqdata.db from
azq .azm files) sqlite3 dump lines into a PostgreSQL and Microsoft SQL Server db.

Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.

'''

from debug_helpers import dprint
import azm_db_constants
from subprocess import call
import os
import sys
import traceback
import time
import datetime
from dateutil.relativedelta import relativedelta
import random
import glob
import pandas as pd
import numpy as np
import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq

PARQUET_COMPRESSION = 'snappy'
WKB_POINT_LAT_LON_BYTES_LEN = 25
# global vars
g_is_postgre = False
g_is_ms = False
g_prev_create_statement_column_names = None
g_prev_create_statement_table_name = None
g_bulk_insert_mode = True

g_unmerge_logs_row = None # would be set in --unmerge mode

g_cursor = None
g_conn = None
g_exec_buf = []

"""
now we already use 'autocommit = True' as recommended by MSDN doc
so set g_always_commit to False

old: sometimes imports
work fine without cursor.commit() but after a --unmerge task, imports dont work
anymore until we do commit() after each execute for all tables
"""

# TODO: set/use as global - from args from azm_db_merge - where .db is extracted from azm    
g_dir_processing_azm = None

pa_type_replace_dict = {
    "text": pa.string(),

    "bigint unique": pa.int64(),
    "bigint": pa.int64(),
    "biginteger": pa.int64(),    
    "int": pa.int32(),
    "integer": pa.int32(),
    "short": pa.int16(),

    "double": pa.float64(),
    "real": pa.float64(),
    "float": pa.float64(),    

    "geometry": pa.binary(),

    # because pyarrow is somehow not taking vals like this so use strings first: In CSV column #0: CSV conversion error to timestamp[ms]: invalid value '2018-07-24 09:59:48.218'
    "timestamp": pa.string(),
    "datetime": pa.string(),
}


KNOWN_COL_TYPES_LOWER_TO_PD_PARQUET_TYPE_DICT = {
    "timestamp": datetime,
    "time": datetime,
    "date": datetime,
    "datetime": datetime,
    "text": str,
    "geometry": str,
    "double": np.float64,
    "real": np.float64,
    "float": np.float64,
    "biginteger": np.float64, # EXCEPT special allowed cols like 'log_hash'
    "bigint": np.float64, # EXCEPT special allowed cols like 'log_hash' that will never be null - they will be np.int64 - but for generic numbers can be null so pd df needs it as float64
    "integer": np.float64, # for generic numbers can be null so pd df needs it as float64
    "int": np.float64,  # for generic numbers can be null so pd df needs it as float64
}

### below are functions required/used by azq_db_merge        
def get_module_path():
    return os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )


def connect(args):
    global g_bulk_insert_mode
    global g_dir_processing_azm
    global g_cursor, g_conn
    global g_exec_buf
    global g_is_ms, g_is_postgre

    if (args['target_db_type'] == 'postgresql'):
        print("PostgreSQL mode initializing...")
        g_is_postgre = True
        import psycopg2
        
        
    elif (args['target_db_type'] == 'mssql'):
        g_is_ms = True
        import pyodbc

    # cleanup old stuff just in case
    close(args)
    
    g_bulk_insert_mode = True # always bulk insert mode now
    
    g_dir_processing_azm = args['dir_processing_azm']

    if g_is_ms:
        print("Connecting... Target DBMS type: mssql")
        connect_str = args['mssql_conn_str']
        print("connect_str:", connect_str)
        mssql_conn_str_dict = {}
        for part in connect_str.split(";"):
            if "=" in part:
                ppart = part.split("=")
                mssql_conn_str_dict[ppart[0]] = ppart[1]
        print("conn_str_dict:", mssql_conn_str_dict)
        assert mssql_conn_str_dict
        args["mssql_conn_str_dict"] = mssql_conn_str_dict
        if not connect_str:
            raise Exception("invalid: mssql mode requres: --mssql_conn_str or mssql_conn_str file argument")
        #unsafe as users might see in logs print "using connect_str: "+connect_str
        """
        https://msdn.microsoft.com/en-us/library/ms131281.aspx
        ODBC applications should not use Transact-SQL transaction statements such as
        BEGIN TRANSACTION, COMMIT TRANSACTION, or ROLLBACK TRANSACTION because this
        can cause indeterminate behavior in the driver. An ODBC application should
        run in autocommit mode and not use any transaction management functions or
        statements, or run in manual-commit mode and use the ODBC SQLEndTran
        function to either commit or roll back transactions.

        https://mkleehammer.github.io/pyodbc/api.html >> 'autocommit' in our case set to false and buffer all atomic cmds into g_exec_buf for run once before commit
        """
        g_conn = pyodbc.connect(connect_str, autocommit = False)
        
    elif g_is_postgre:
        print("Connecting... Target DBMS type: PostgreSQL")
        # example: conn = psycopg2.connect("dbname=azqdb user=azqdb")
        connect_str = "dbname={} user={} password={} port={}".format(
                args['server_database'],
                args['server_user'],
            args['server_password'],
            args['pg_port']
                )
        print(connect_str)
        if args['pg_host'] != None:
            connect_str = "host="+args['pg_host']+" "+connect_str
        #unsafe as users might see in logs print "using connect_str: "+connect_str
        args['connect_str'] = connect_str
        g_conn = psycopg2.connect(connect_str)
    if (g_conn is None):
        print("psycopg2.connect returned None")
        return False
    
    print("connected")
    
    g_cursor = g_conn.cursor()

    # post connect steps for each dbms
    if g_is_postgre and not args['unmerge']:

       try_cre_postgis(schema="public") # create postgis at public schema first
        
       if args["pg_schema"] != "public":
            print("pg mode create pg_schema:", args["pg_schema"])
            try:
                with g_conn as c:
                    g_cursor.execute("create schema if not exists "+args["pg_schema"])
                    c.commit()
                    print("success: create schema "+args["pg_schema"]+ " success")
                    
            except Exception as e:
                estr = str(e)
                if 'already exists' in estr:
                    dprint("schema already exists")
                    pass
                else:
                    print(("FATAL: CREATE schema failed:"+args["pg_schema"]))
                    raise e
            # create postgis in public only - print "pg using schema start"
            # try_cre_postgis(schema=args["pg_schema"]) # inside new schema
                

    if g_is_ms:
        pass
        ''' somehow not working - let qgis detect itself for now...
        try:
            # set 'f_table_name' to unique so we can blindly insert table_name:geom (on create handlers) to it without checking (let mssql check)
            ret = g_cursor.execute("""
            CREATE TABLE [dbo].[geometry_columns](
            [f_table_catalog] [varchar](50) NULL,
            [f_table_schema] [varchar](50) NULL,
            [f_table_name] [varchar](100) NULL UNIQUE,
            [f_geometry_column] [varchar](50) NULL,
            [coord_dimension] [int] NULL,
            [srid] [int] NULL,
            [geometry_type] [varchar](50) NULL
            )
            """)
            print "created qgis table: geometry_columns"
        except Exception as e:
            pass
        
        try:
            # below execute would raise an exception if it is already created
            ret = g_cursor.execute("""
            CREATE TABLE spatial_ref_sys (srid INTEGER NOT NULL PRIMARY KEY,auth_name VARCHAR(256) NOT NULL,auth_srid INTEGER NOT NULL,ref_sys_name VARCHAR(256),proj4text VARCHAR(2048) NOT NULL);            
            """)
            print "created qgis table: spatial_ref_sys"
            # if control reaches here means the table didn't exist (table was just created and is empty) so insert wgs84 into it... 
            ret = g_cursor.execute("""
            INSERT INTO "spatial_ref_sys" VALUES(4326,'epsg',4326,'WGS 84','+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs');
            """)
            print "added wgs84 to qgis table: spatial_ref_sys"        
        except Exception as e:
            pass
        '''

    return True


def try_cre_postgis(schema="public"):
    global g_conn
    global g_cursor
    try:
        with g_conn as c:
            sql = "CREATE EXTENSION if not exists postgis SCHEMA {}".format(schema)
            print("try: CREATE EXTENSION postgis on schema:", schema, "sql:", sql)
            g_cursor.execute(sql)
            c.commit()
            print("success: CREATE EXTENSION postgis")
    except Exception as e:
        estr = str(e)
        if 'already exists' in estr:
            print("postgis already exists")
            pass
        else:
            print("FATAL: CREATE EXTENSION postgis - failed - please make sure postgis is correctly installed.")
            raise e


def check_if_already_merged(args, log_hash):    
    global g_unmerge_logs_row
    global g_cursor
    global g_exec_buf
    global g_is_ms, g_is_postgre

    if args["pg_schema"] != "public":
        g_cursor.execute("SET search_path = '{}','public';".format(args["pg_schema"]))
    
    try:
        print("checking if this log_hash has already been imported/merged: "+log_hash)
        sqlstr = "select \"log_hash\" from \"logs\" where \"log_hash\" = ?"
        if g_is_postgre:
            sqlstr = sqlstr.replace("?","%s")
        print(("check log cmd: "+sqlstr))

        row = None

        # use with for auto rollback() on g_conn on exception - otherwise we cant use the cursor again - would fail as: current transaction is aborted, commands ignored until end of transaction block        
        with g_conn:
            g_cursor.execute(sqlstr, [log_hash])
            row = g_cursor.fetchone()

        print(("after cmd check if exists row:", row))

        if (row is None):
            
            # azm never imported
            
            if (args['unmerge']):
                # unmerge mode - this azm is not in target db
                raise Exception("ABORT: This azm is already not present in target db's logs' table")               
            else:
                print("This log hasn't been imported into target db yet - ok to proceed")
                pass
                return True
            
        else:
            
            # azm already imported
                        
            if (args['unmerge']):
                
                #dprint("um0: row: "+str(row))

                if g_is_postgre or g_is_ms:
                    #dprint("upg 0")
                    # row is a tuple - make it a dict
                    #dprint("upg 01")
                    # now we only need 'log_hash' to unmerge and the used odbc cant parse geom too - cols = get_remote_columns(args,'logs')
                    cols = [['log_hash', 'bigint']]
                    #dprint("upg 1: cols: "+str(cols))
                    drow = {}
                    i = 0
                    for col in cols:
                        print(row[i])
                        drow[col[0]] = row[i]
                        i = i+1
                    row = drow
                
                #dprint("um1")
                    
                print("### unmerge mode - delete start for azm: log_hash {}".format(row['log_hash']))
                g_unmerge_logs_row = row
                
                sqlstr = "delete from \"logs\" where \"log_hash\" = '{}'".format(log_hash)
                
                g_exec_buf.append(sqlstr)
                print("delete from logs table added to g_exec_buf: ", sqlstr)
                
            else:
                raise Exception("ABORT: This log ({}) has already been imported/exists in target db (use --unmerge to remove first if you want to re-import).".format(log_hash))
                
                                             
    except Exception as e:
        estr = str(e)
        if ("Invalid object name 'logs'" in estr or '42S02' in estr
            or 'relation "logs" does not exist' in estr):
            print("looks like this is the first-time log import - no table named logs exists yet - ok...")
            if args['unmerge']:
                raise Exception("--unmerge mode called on an empty database: no related 'logs' table exist yet")
            # first time import - no table named logs exists yet
        
        else:
            type_, value_, traceback_ = sys.exc_info()
            exstr = str(traceback.format_exception(type_, value_, traceback_))
            print("re-raise exception e - ",exstr)
            raise e
        
    return False
        

def close(args):
    global g_cursor, g_conn
    global g_exec_buf    
    global g_prev_create_statement_column_names
    global g_prev_create_statement_table_name
    global g_bulk_insert_mode
    global g_unmerge_logs_row
    
    print("mssql_handler close() - cleanup()")
    
    g_prev_create_statement_column_names = None
    g_prev_create_statement_table_name = None
    g_bulk_insert_mode = True
    g_unmerge_logs_row = None
    
    del g_exec_buf[:]
        
    if g_cursor is not None:
        try:
            g_cursor.close()
            g_cursor = None        
        except Exception as e:
            print("warning: mssql cursor close failed: "+str(e))
        
    if g_conn is not None:    
        try:
            g_conn.close()
            g_conn = None
        except Exception as e:
            print("warning: mssql conn close failed: "+str(e))
            
    return True

    
def commit(args, line):
    global g_cursor, g_conn 
    global g_prev_create_statement_table_name
    global g_exec_buf
    
    g_prev_create_statement_table_name = None

    n = len(g_exec_buf)

    # make sure all create/alters are committed
    g_conn.commit()
    
    print("### total cmds to execute for operation: "+str(n))
    
    i = 0
    for buf in g_exec_buf:
        #print("buf:", buf)
        if isinstance(buf, tuple):
            # for COPY from stdin
            buf, dump_fp = buf
            if g_is_postgre:
                with open(dump_fp, "rb") as dump_fp_fo:
                    g_cursor.copy_expert(buf, dump_fp_fo)
            elif g_is_ms:
                table = buf.split('"')[1].strip()
                assert table
                mssql_conn_str_dict = args["mssql_conn_str_dict"]
                assert mssql_conn_str_dict
                cmd = f'''bcp {mssql_conn_str_dict["Database"]}.dbo.{table} in {dump_fp} -S "{mssql_conn_str_dict["Server"]}" -U "{mssql_conn_str_dict["UID"]}" -P "{mssql_conn_str_dict["PWD"]}" -t"{azm_db_constants.BULK_INSERT_COL_SEPARATOR_PARAM}" -r"{azm_db_constants.BULK_INSERT_LINE_SEPARATOR_PARAM}" -c'''
                print("mssql bcp cmd:\n", cmd)
                cmd_ret = os.system(cmd)
                assert 0 == cmd_ret
            else:
                raise Exception("invalid not pg not ms")
        else:
            try:
                
                if args['dump_parquet']:
                    #print("dump_parquet mode exec buf:", buf)
                    skip = True
                    if 'delete from "logs" where' in buf:
                        skip = False
                    if skip:
                        print("dump_parquet mode SKIP exec buf:", buf)
                        continue
                with g_conn:  # needed otherwise cursor would become invalid and unmerge would fail for no table cases handled below
                    g_cursor.execute(buf)
            except Exception as e:
                if "does not exist" in str(e) and args['unmerge']:
                    print("WARNING: unmerge exception: {} - but ok for --umnerge mode if exec delete and face - does not exist exception...".format(e))
                else:
                    raise e

        print("# done execute cmd {}/{}: {}".format(i, n, buf))
        i = i + 1
        
    print("### all cmds exec success - COMMIT now...")    
    g_conn.commit()
    print("### COMMIT success...")

    # do mc cp all parquet files to object store...
    if args['dump_parquet']:
        bucket_name = ""
        if "AZM_BUCKET_NAME_OVERRIDE" in os.environ and os.environ["AZM_BUCKET_NAME_OVERRIDE"]:
            bucket_name = os.environ["AZM_BUCKET_NAME_OVERRIDE"]
        else:
            subdomain = os.environ['WEB_DOMAIN_NAME'].split(".")[0]
            bucket_name = "azm-"+subdomain

        bucket_ym_folder_name = args['log_hash_ym_str'].replace("_", "-")
        if args['unmerge']:
            if 'WEB_DOMAIN_NAME' in os.environ and os.environ['WEB_DOMAIN_NAME'] == 'localhost':
                print('localhost mc rm old parquet files')
                # object listing would cost too much cpu and class a operations so skip this for parquet mode
                rmcmd = "mc find minio_logs/{}/{}/ --name '*_{}.parquet'".format(
                    bucket_name,
                    bucket_ym_folder_name,
                    args['log_hash']
                )
                rmcmd += " --exec 'mc rm {}'"
                print("mc rmcmd:", rmcmd)
                rmcmdret = os.system(rmcmd)
                if rmcmdret != 0:
                    raise Exception("Remove files from object store failed cmcmdret: {}".format(rmcmdret))
                try:
                    with g_conn:
                        update_sql = "update uploaded_logs set non_azm_object_size_bytes = null where log_hash = {};".format(args['log_hash'])
                        print("update_sql:", update_sql)
                        g_cursor.execute(update_sql)
                except:
                    type_, value_, traceback_ = sys.exc_info()
                    exstr = str(traceback.format_exception(type_, value_, traceback_))
                    print("WARNING: update uploaded_logs set non_azm_object_size_bytes to null failed exception:", exstr)

        else:
            cpcmd = "mc cp {}/*.parquet minio_logs/{}/{}/".format(
                g_dir_processing_azm,
                bucket_name,
                bucket_ym_folder_name,
            )
            print("mc cpcmd:", cpcmd)
            cpcmdret = os.system(cpcmd)
            if cpcmdret != 0:
                raise Exception("Copy files to object store failed cmcmdret: {}".format(cpcmdret))
            try:
                combined_pq_size = 0
                for fp in glob.glob('{}/*.parquet'.format(g_dir_processing_azm)):
                    fp_sz = os.path.getsize(fp)
                    combined_pq_size += fp_sz
                with g_conn:
                    update_sql = "update uploaded_logs set non_azm_object_size_bytes = {} where log_hash = {};".format(combined_pq_size, args['log_hash'])
                    print("update_sql:", update_sql)
                    g_cursor.execute(update_sql)
            except:
                type_, value_, traceback_ = sys.exc_info()
                exstr = str(traceback.format_exception(type_, value_, traceback_))
                print("WARNING: update uploaded_logs set non_azm_object_size_bytes to parquets size failed exception:", exstr)

    
    return True

def find_and_conv_spatialite_blob_to_wkb(csv_line):
    #print "fac csv_line:", csv_line
    spat_blob_offset = csv_line.find('0001E6100000')
    if spat_blob_offset == -1:
        return csv_line
    part = csv_line[spat_blob_offset:spat_blob_offset+120+1]
    #print "part[120]:", part[120]
    #dprint("csv_line spatialite_geom_part: "+part)

    spatialite_geom_contents = ""
    if (g_is_postgre and (part[120] == ',' or part[120] == '\n')) or (g_is_ms and part[120] == '\t'):
        spatialite_geom_contents = part[0:120]
    else:
        dprint("check of spatialite_geom_part - failed - abort")
        return csv_line
    
    #dprint("spatialite_geom_contents: len "+str(len(spatialite_geom_contents))+" val: "+spatialite_geom_contents)
    # convert spatialite geometry blob to wkb
    """

    Spatialite BLOB Format (Point)
    ------------------------------

    http://www.gaia-gis.it/gaia-sins/BLOB-Geometry.html
    example:
    0001E6100000DD30C0F46C2A594041432013008E2B40DD30C0F46C2A594041432013008E2B407C01000000DD30C0F46C2A594041432013008E2B40FE

    parse:
    spatialite header: 00 (str_off 0 str_len 2)
    endian: 01 little endian (str_off 2 str_len 2) (spec: if this GEOMETRY is BIG_ENDIAN ordered must contain a 0x00 byte value otherwise, if this GEOMETRY is LITTLE_ENDIAN ordered must contain a 0x01 byte value)
    SRID: E6 10 00 00 (str_off 4 str_len 8)
    MBR_MIN_X: DD 30 C0 F4 6C 2A 59 40 (str_off 12 str_len 16)
    MBR_MIN_Y: 41 43 20 13 00 8E 2B 40 (str_off 28 str_len 16)
    MBR_MAX_X: DD 30 C0 F4 6C 2A 59 40 (str_off 42 str_len 16)
    MBR_MAX_Y: 41 43 20 13 00 8E 2B 40 (str_off 58 str_len 16)
    MBR_END: 7C (str_off 76 str_len 2)
    CLASS_TYPE: 01 00 00 00 (str_off 78 str_len 8)
    POINT:
      X: DD 30 C0 F4 6C 2A 59 40 (str_off 86 str_len 16)
      Y: 41 43 20 13 00 8E 2B 40 (str_off 102 str_len 16)
    END: FE (str_off 118 str_len 2)

    ---

    WKB Format
    ----------

    See "3.3.2.6 Description of WKBGeometry Representations"
    in https://portal.opengeospatial.org/files/?artifact_id=829

    Point {
    double x;
    double y;
    };

    WKBPoint {
    byte byteOrder;
    uint32 wkbType; //class_type
    Point point;
    }

    Therefore, for "Point" we need from spatialite blob parts:
    endian, CLASS_TYPE, POINT
    
    
    
    """
    # spatialite blob point size is 60 bytes = 120 chars in hex - as in above example and starts with 00
    if len(spatialite_geom_contents) == 120 and spatialite_geom_contents[0] == '0' and spatialite_geom_contents[1] == '0':
        endian = spatialite_geom_contents[2:4] # 2 + len 2
        class_type = "<unset>"
        if g_is_postgre:
            """
            old code: class_type = spatialite_geom_contents[78:86] # 78 + 8

            change class_type to 'point' BITWISE_OR SRID flag as per https://trac.osgeo.org/postgis/browser/trunk/doc/ZMSgeoms.txt
           
            "
            wkbSRID = 0x20000000
            If the SRID flag is set it's value is encoded as a 4byte integer
        right after the type integer.
            "
    
            so our class is pont | wkbSRID = 0x20000001 (little endian 32: 01000020)
    
            then add srid "right after the type integer"
            our srid = 4326 = 0x10E6 (little endian 32: E6100000)
            
            therefore, class_type_point_with_srid_wgs84 little_endian is 01000020E6100000
    
            """
    
            class_type = "01000020E6100000"
        elif g_is_ms:
            class_type = ""
            
        point = spatialite_geom_contents[86:118] # 86 + 16 + 16
        wkb = ""
        if g_is_postgre:
            wkb = endian + class_type + point # example:  01 01000020e6100000 ae17f9ab76565340 59528b140ca03c40
        if g_is_ms:
            
            """
            https://msdn.microsoft.com/en-us/library/ee320529.aspx
            
            0xE6100000 01 0C 0000000000001440 0000000000002440
            This string is interpreted as shown in the following table.
            Binary value Description
            E6100000 SRID = 4326
            01 Version = 1
            0C Serialization Properties = V + P (geometry is valid, single point)
            0000000000001440 X = 5
            0000000000002440 Y = 10
            """
            wkb = "E6100000010C"+point        
        csv_line = csv_line.replace(spatialite_geom_contents,wkb,1)
    else:
        pass
        #dprint("not entering spatialite blob parse - len "+str(len(spatialite_geom_contents)))

    #dprint("find_and_conv_spatialite_blob_to_wkb ret: "+csv_line)
    return csv_line


def create(args, line):
    global g_cursor, g_conn
    global g_prev_create_statement_table_name
    global g_prev_create_statement_column_names
    global g_exec_buf
    global g_is_ms, g_is_postgre
    global g_unmerge_logs_row
        
    g_prev_create_statement_column_names = None

    if args["pg_schema"] != "public":
        g_cursor.execute("SET search_path = '{}','public';".format(args["pg_schema"]))

    line_adj = sql_adj_line(line)
    table_name = get_table_name(line_adj)
    schema_per_month_name = "per_month_{}".format(table_name)

    if table_name.startswith("spatialite_history"):
        return False  # omit these tables - import fails

    if table_name == "logs":
        uline = line.replace('"log_hash" BIGINT,','"log_hash" BIGINT UNIQUE,',1)
        print("'logs' table cre - make log_hash unique for this table: ", uline)
        line_adj = sql_adj_line(uline)
    if table_name == "wifi_scanned":
        wifi_scanned_MIN_APP_V0 = 3
        wifi_scanned_MIN_APP_V1 = 0
        wifi_scanned_MIN_APP_V2 = 742
        print("check azm apk ver for wifi_scanned table omit: ", args["azm_apk_version"])
        if args["azm_apk_version"] < wifi_scanned_MIN_APP_V0*1000*1000 + wifi_scanned_MIN_APP_V1*1000 + wifi_scanned_MIN_APP_V2:
            print("omit invalidly huge wifi_scanned table in older app vers requested by a customer - causes various db issues")
            return False

    if table_name == "lte_rrc_tmsi" and not args['dump_parquet']:
        print('skipping lte_rrc_tmsi table for legacy postgres mode')
        return False
        
    if args['import_geom_column_in_location_table_only'] and table_name != "location":
        line_adj = sql_adj_line(line.replace(',"geom" BLOB','',1))

    if (g_unmerge_logs_row is not None):
        print("### unmerge mode - delete all rows for this azm in table: "+table_name)
        
        """ now we use log_hash - no need to parse time
        # remove 3 traling 000 from microsecs str
        start_dt_str = str(g_unmerge_logs_row['log_start_time'])[:-3] 
        end_dt_str = str(g_unmerge_logs_row['log_end_time'])[:-3]
        """
        sqlstr = "delete from \""+table_name+"\" where \"log_hash\" = {}".format(g_unmerge_logs_row['log_hash'])
        g_exec_buf.append(sqlstr)
        
        return True
    
    g_prev_create_statement_table_name = table_name
    sqlstr = line_adj
        
    '''
    Now get local columns
    Example sqlstr:
    CREATE TABLE "browse" ("time" DATETIME,"time_ms" INT,"posid" INT,"seqid" INT,"netid" INT,  "Browse_All_Session_Throughput_Avg" real, "Data_Browse_Throughput" real, "Data_Browse_Throughput_Avg" real, "Data_Browse_Total_Loaded_Obj" smallint, "Data_Browse_Total_Page_Obj" smallint, "Data_Browse_Page_Load_Time" real, "Data_Browse_Page_Load_Time_Avg" real, "Data_Browse_Total_Sessions" smallint, "Data_Browse_Total_Success" smallint, "Data_Browse_Total_Fail_Page" smallint, "Data_Browse_Total_Fail_Obj" smallint, "Data_Browse_Total_Timeout" smallint, "Data_Browse_Exterior_Fail_Page" smallint, "Data_Browse_Exterior_Fail_Obj" smallint, "Browse_Throughput" real, "Browse_Throughput_max" real, "Browse_Throughput_min" real, "Browse_Duration" real, "Browse_Duration_max" real, "Browse_Duration_min" real);
    '''
    # get part inside parenthesis
    ls = line_adj.split('" (')
    #dprint("ls :" + str(ls))
    ls = ls[1].split(");")[0]
    # split by comma
    ls = ls.split(",")
    
    # parse column names and keep for insert commands
    local_column_dict = {}
    local_columns = []
    local_column_names = []    
    for lsp in ls:
        splitted = lsp.split('"')
        if len(splitted) < 3:
            raise Exception("failed to get col_name/col_type for lsp: {}".format(lsp))
        col_name = splitted[1]
        col_type = splitted[2].strip()                

        omit_col = False

        """
        import_geom_column_in_location_table_only feature already implemented at line_adj above
        if args['import_geom_column_in_location_table_only'] and col_name == "geom" and table_name != "location":
            omit_col = True
        """
            
        if omit_col == False:
            local_column_dict[col_name] = col_type
            local_columns.append([col_name, col_type])
            local_column_names.append(col_name)
    
    # args['prev_create_statement_column_names'] 
    g_prev_create_statement_column_names = str(local_column_names).replace("'","").replace("[","(").replace("]",")")
    
    remote_column_names = None

    if (not args['dump_parquet']) or (table_name == "logs"):
        try:
            #dprint("create sqlstr: "+sqlstr)

            if g_is_postgre:

                if args['pg10_partition_by_month']:
                    if table_name == "logs":
                        # dont partition logs table
                        pass
                    else:
                        # create target partition for this log + table
                        # ok - partition this table
                        sqlstr = sqlstr.replace(";","") +" PARTITION BY RANGE (time);"
                        try:
                            with g_conn:
                                g_cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}';".format(schema_per_month_name))
                                if bool(g_cursor.rowcount):
                                    print("schema_per_month_name already exists:", schema_per_month_name)
                                    pass
                                else:
                                    print("cre schema now because: NOT schema_per_month_name already exists:", schema_per_month_name)
                                    c_table_per_month_sql = "create schema {};".format(schema_per_month_name)
                                    ret = g_cursor.execute(c_table_per_month_sql)
                                    g_conn.commit()
                                    print("success: create per_month ["+c_table_per_month_sql+"] success")
                        except:
                            type_, value_, traceback_ = sys.exc_info()
                            exstr = str(traceback.format_exception(type_, value_, traceback_))
                            print("WARNING: create table_per_month schema failed - next insert/COPY commands would likely faile now - exstr:", exstr)


                #dprint("create sqlstr postgres mod: "+sqlstr)                
                # postgis automatically creates/maintains "geometry_columns" 'view'



            if g_is_ms:
                #dprint("create sqlstr mod mssql geom: "+sqlstr)
                pass

            if g_is_postgre:
                with g_conn:
                    #too slow and high cpu: g_cursor.execute("select * from information_schema.tables where table_schema=%s and table_name=%s", (args["pg_schema"],table_name,))
                    g_cursor.execute("""
                    SELECT FROM pg_catalog.pg_class c
                        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE  n.nspname = %s
                        AND    c.relname = %s
                        AND    c.relkind = 'r'""" , (args["pg_schema"],table_name,))
                    if bool(g_cursor.rowcount):
                        print("omit create already existing 'logs' table - raise exception to check columns instead")
                        raise Exception("table {} already exists - no need to create".format(table_name))
                    else:
                        print("table not exists")

            ret = None
            # use with for auto rollback() on g_conn on expected fails like already exists
            with g_conn:
                sqlstr = sqlstr.replace('" bigintEGER,', '" bigint,').replace('" bigintEGER)', '" bigint)').replace('" bigintEGER', '" bigint')
                print("exec:", sqlstr)
                ret = g_cursor.execute(sqlstr)
            # commit now otherwise COPY might not see partitions
            g_conn.commit()
            #dprint("create execute ret: "+str(ret))

            """ if control reaches here then the create is successful
            - table was not existing earlier - so remote cols must be the same
            """
            remote_column_names = local_column_names

        except Exception as e:
            emsg = str(e)
            dprint("create failed: " + emsg + "\n from sqlstr:\n" +
                   sqlstr+"\nori line:\n"+line)
            if ("There is already an object named" in emsg or
                " already exists" in emsg):
                if args['need_check_remote_cols']:
                    print(("args['need_check_remote_cols']", args['need_check_remote_cols'], "so must do alter check"))
                    print("""This table already exists -
                    checking if all local columns already exist in remote
                    - otherwise will add each missing cols to
                    remote table before inserting to it.""")

                    remote_columns = get_remote_columns(args, table_name)
                    remote_column_names = get_col_names(remote_columns)

                    if (len(remote_columns) == 0):
                        raise Exception("FATAL: failed to parse/list remote columns")



                    # now get local columns that are not in remote

                    local_columns_not_in_remote = []

                    for col in local_columns:
                        col_name = col[0]
                        col_type = col[1]                        

                        ####### quickfix: col_type override for unsigned int32 cols from sqlite (bindLong already) - conv to bigint in pg as pg doesnt have unsigned
                        if col_name in ["lte_volte_rtp_source_ssrc", "lte_volte_rtp_timestamp", "lte_m_tmsi", "lte_mmec"] :
                            # might need to psql to do first manually if log was already imported using older azm_db_merge:
                            # alter table all_logs.lte_volte_rtp_msg alter column lte_volte_rtp_source_ssrc type bigint;
                            # alter table all_logs.lte_volte_rtp_msg alter column lte_volte_rtp_timestamp type bigint;
                            print("col_name in list", col_name)

                            col_type = "bigint"                
                        #######################

                        is_already_in_table = col_name in remote_column_names
                        dprint("local_col_name: " + col_name +
                               " col_type: " + col_type +
                               " - is_already_in_table: "+str(is_already_in_table))
                        if (not is_already_in_table):
                            local_columns_not_in_remote.append(
                                ' "{}" {}'.format(col_name, col_type))
                        # TODO: handle if different type?

                    n_cols_to_add = len(local_columns_not_in_remote)

                    if (n_cols_to_add == 0):
                        pass
                        #dprint("n_cols_to_add == 0 - no need to alter table")
                    else:
                        print("n_cols_to_add: " + str(n_cols_to_add) + " - need to alter table - add cols:" + str(local_columns_not_in_remote) + "\nremote_cols:\n"+str(remote_columns))
                        # example: ALTER TABLE dbo.doc_exa ADD column_b VARCHAR(20) NULL, column_c INT NULL ; 
                        alter_str = "ALTER TABLE \"{}\" ".format(table_name)
                        alter_cols = ""                

                        for new_col in local_columns_not_in_remote:
                            # not first
                            prefix = ""
                            if (alter_cols != ""):
                                prefix = ", "
                            alter_cols = alter_cols + prefix + " ADD " + new_col

                        alter_str = alter_str + alter_cols + ";"

                        sqlstr = sql_adj_line(alter_str)
                        print("execute alter_str: " + sqlstr)
                        exec_creatept_or_alter_handle_concurrency(sqlstr)

                        # re-get remote cols
                        remote_columns = get_remote_columns(args, table_name)
                        remote_column_names = get_col_names(remote_columns)
                        print(("get_remote_columns after alter: "+str(remote_column_names)))
                else:
                    print(("args['need_check_remote_cols']", args['need_check_remote_cols'], "so no need to do alter check"))

            else:
                raise Exception("FATAL: create table error - : \nemsg:\n "+emsg+" \nsqlstr:\n"+sqlstr)

    local_col_name_to_type_dict = {}

    if g_bulk_insert_mode:

        if args['pg10_partition_by_month'] and not args['dump_parquet']:
            if table_name == "logs":
                # dont partition logs table
                pass
            else:                
                
                ##  check/create partitions for month for log_hash, prev month, after month                
                ori_log_hash_datetime = args['ori_log_hash_datetime']
                months_pt_check_list = [ori_log_hash_datetime+relativedelta(months=-1), ori_log_hash_datetime, ori_log_hash_datetime+relativedelta(months=+1)]
                
                for pre_post_month_log_hash_datetime in months_pt_check_list:
                    log_hash_ym_str = pre_post_month_log_hash_datetime.strftime('%Y_%m')
                    #print  "log_hash_datetime:", log_hash_datetime

                    ntn = "logs_{}".format(log_hash_ym_str) # simpler name because we got cases where schema's table name got truncated: activate_dedicated_eps_bearer_context_request_params_3170932708
                    pltn = "{}.{}".format(schema_per_month_name, ntn)
                    per_month_table_already_exists = False
                    with g_conn:
                        # too slow and high cpu check_sql = "select * from information_schema.tables where table_schema='{}' and table_name='{}'".format(schema_per_month_name, ntn)
                        check_sql = """SELECT FROM pg_catalog.pg_class c
                        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE  n.nspname = '{}'
                        AND    c.relname = '{}'
                        AND    c.relkind = 'r'""".format(schema_per_month_name, ntn)   
                        print("check_sql partition of table exists or not:", check_sql)
                        g_cursor.execute(check_sql)
                        if bool(g_cursor.rowcount):
                            per_month_table_already_exists = True

                    if per_month_table_already_exists:
                        print("omit create already existing per_month table:", pltn)
                        pass
                    else:
                        print("NOT omit create already existing per_month table:", pltn)
                        cre_target_pt_sql = "CREATE TABLE {} PARTITION OF {} FOR VALUES from ('{}-1') to ('{}-1');".format(
                            pltn,
                            table_name,
                            pre_post_month_log_hash_datetime.strftime("%Y-%m"),
                            (pre_post_month_log_hash_datetime+relativedelta(months=+1)).strftime("%Y-%m")
                        )
                        if args['pg10_partition_index_log_hash']:
                            cre_index_for_pt_sql = "CREATE INDEX ON {} (log_hash);".format(pltn)
                            cre_target_pt_sql += " "+cre_index_for_pt_sql
                            
                        print(("cre_target_pt_sql:", cre_target_pt_sql))                        
                        exec_creatept_or_alter_handle_concurrency(cre_target_pt_sql, allow_exstr_list=[" already exists"])

        ###### let sqlite3 dump contents of table into file
        
        table_dump_fp = os.path.join(g_dir_processing_azm, table_name + ".csv")
        table_dump_format_fp = os.path.join(g_dir_processing_azm, table_name + ".fmt")
        
        #print("table_dump_fp: "+table_dump_fp)
        #print("table_dump_format_fp: "+table_dump_format_fp)

        # create dump csv of that table            
        """ 
        example dump of logs table:
        sqlite3 azqdata.db -list -newline "|" -separator "," ".out c:\\azq\\azq_report_gen\\azm_db_merge\\logs.csv" "select * from logs"
        """

        # get col list, and hex(col) for blob coulumns

        i = 0
        col_select = ""
        first = True
        #dprint("local_columns: "+str(local_columns))
        for col in local_columns:
            col_name = col[0]
            col_type = col[1]
            local_col_name_to_type_dict[col_name] = col_type
            if first:
                first = False
            else:
                col_select = col_select + ","
                
            pre = " "
            post = ""
            if col_type == "geometry" or (g_is_postgre and col_type == "bytea") or (g_is_ms and col_type.startswith("varbinary")):
                pre = " nullif(hex("
                post = "),'')"
                if col_name == "geom":
                    pass
                    #geom_col_index = i

            ############## wrong data format fixes
            
            ### custom limit bsic len in case matched wrongly entered bsic to long str but pg takes max 5 char len for bsic
            if col_name == "modem_time":
                # handle invalid modem_time case: 159841018-03-10 07:24:42.191
                col_name = "strftime('%Y-%m-%d %H:%M:%f', modem_time) as modem_time"            
            elif col_name == "gsm_bsic":
                col_name = "substr(gsm_bsic, 0, 6) as gsm_bsic"  # limit to 5 char len (6 is last index excluding)
            elif col_name == "android_cellid_from_cellfile":
                col_name = "cast(android_cellid_from_cellfile as int) as android_cellid_from_cellfile"  # type cast required to remove non-int in cellfile data
            elif col_name.endswith("duration") or col_name.endswith("time"):                
                # many _duration cols in detected_radion_voice_call_session and in pp_ tables have wrong types or even has right type but values came as "" so would be ,"" in csv which postgres and pyarrow wont allow for double/float/numeric cols - check by col_name only is faster than nullif() on all numericols - as most cases are these _duration cols only
                col_name = "nullif({},'') as {}".format(col_name, col_name)
            elif table_name == "nr_cell_meas":
                # special table handling
                if "int" in col_type.lower():
                    print("nr_cell_meas cast to int:  col_name {} col_type {}".format(col_name, col_type))
                    pre = "cast("
                    post = " as int)"
                elif "double" in col_type.lower():
                    print("nr_cell_meas cast to double:  col_name {} col_type {}".format(col_name, col_type))
                    pre = "cast("
                    post = " as double)"
            
            col_select = col_select + pre + col_name + post            
            i = i + 1
        
        dprint("col_select: "+col_select)
                
        if g_is_ms:
            ret = call(
                [
                args['sqlite3_executable'],
                args['file'],
                "-ascii",
                "-list",            
                '-separator', azm_db_constants.BULK_INSERT_COL_SEPARATOR_VALUE,
                '-newline', azm_db_constants.BULK_INSERT_LINE_SEPARATOR_VALUE,
                '.out ' + '"' +table_dump_fp.replace("\\","\\\\") + '"', # double backslash because it needs to go inside sqlite3 cmd parsing again      
                'select '+col_select+' from '+ table_name + ' where time is not null'
                ], shell = False
            )

        if g_is_postgre:
            select_sqlstr = 'select '+col_select+' from '+ table_name

            # filter all tables but not the main logs table
            if table_name != "logs":
                pass
                select_sqlstr += " where time >= '{}' and time <= '{}'".format(args['log_data_min_time'], args['log_data_max_time'])
                
            #print "select_sqlstr:", select_sqlstr
            dump_cmd = [
                args['sqlite3_executable'],
                args['file'],
                "-ascii",
                "-csv",
                '-separator',',',
                '-newline', '\n',
                '.out ' + '"' +table_dump_fp.replace("\\","\\\\") + '"',  # double backslash because it needs to go inside sqlite3 cmd parsing again
                select_sqlstr
                ]
            #dprint("dump_cmd:", dump_cmd)

            # if parquet dump mode do only logs table dump to track already imported
            start_time = datetime.datetime.now()
            if True:#(not args['dump_parquet']) or parquet_arrow_mode or table_name == "logs":
                ret = call(
                    dump_cmd,
                    shell=False
                )
            #print("dump_cmd:", dump_cmd)
            #print "dump_cmd ret:", ret
            append_table_operation_stats(args, table_name, "dump_csv duration:", (datetime.datetime.now() - start_time).total_seconds())

        table_dump_fp_ori = table_dump_fp
        pqfp = table_dump_fp_ori.replace(".csv","_{}.parquet".format(args['log_hash']))
        table_dump_fp_adj = table_dump_fp + "_adj.csv"        

        geom_format_in_csv_is_wkb = False
        # in parquet mode we are modifying geom anyway so assume geom is spatialite format instead of wkb
        
        if (not args['dump_parquet']) or (table_name == "logs"):           
            geom_format_in_csv_is_wkb = True            
            start_time = datetime.datetime.now()
            with open(table_dump_fp,"rb") as of:
                with open(table_dump_fp_adj,"w") as nf:  # wb required for windows so that \n is 0x0A - otherwise \n will be 0x0D 0x0A and doest go with our fmt file and only 1 row will be inserted per table csv in bulk inserts...
                    while True:
                        ofl = of.readline().decode()

                        ''' this causes python test_browse_performance_timing.py to fail as its json got changed
                        if g_is_postgre:
                            ofl = ofl.replace(',""',',')  # keep this legacy code for postgres mode code jus to be sure, although we already did nullif checks during sqlite csv dunp...
                        '''

                        """ no need to check this, only old stale thread versions would have these cases and will have other cases too so let it crash in all those cases
                        if ofl.strip() == all_cols_null_line:
                            continue
                        """
                        if g_is_postgre:
                            ofl = ofl.replace(',NaT',',')

                        ofl = find_and_conv_spatialite_blob_to_wkb(ofl)

                        if ofl == "":
                            break

                        nf.write(ofl)

            table_dump_fp = table_dump_fp_adj
            append_table_operation_stats(args, table_name, """find_and_conv_spatialite_blob_to_wkb, replace ,"" with , total file duration:""", (datetime.datetime.now() - start_time).total_seconds())



        #dprint("dump table: "+table_name+" for bulk insert ret: "+str(ret))
        
        if (ret != 0):
            print("WARNING: dump table: "+table_name+" for bulk insert failed - likely sqlite db file error like: database disk image is malformed. In many cases, data is still correct/complete so continue.")
            
            
        if (os.stat(table_dump_fp).st_size == 0):
            print("this table is empty...")
            return True
        
        # if control reaches here then the table is not empty
                
        ################## read csv to arrow, set types, dump to parqet - return True, but if log_table dont return - let it enter pg too...
        # yes, arrow read from csv, convert to pd to mod datetime col and add lat lon is faster than pd.read_sql() and converting fields and to parquet
        if args['dump_parquet']:
            #print "local_column_names:", local_column_names            
            pa_column_types = local_column_dict.copy()
            for col in list(pa_column_types.keys()):
                sqlite_col_type = pa_column_types[col].lower()
                if sqlite_col_type in list(pa_type_replace_dict.keys()):
                    pa_column_types[col] = pa_type_replace_dict[sqlite_col_type]
                elif sqlite_col_type.startswith("varchar"):
                    pa_column_types[col] = "string"

                # special cases
                if is_datetime_col(col):
                    # because pyarrow is somehow not taking vals like this so use strings first: In CSV column #0: CSV conversion error to timestamp[ms]: invalid value '2018-07-24 09:59:48.218'
                    pa_column_types[col] = pa.string()
                elif col.endswith("duration"):
                    pa_column_types[col] = pa.float64()
                elif col.endswith("session_master_session_id"):
                    pa_column_types[col] = pa.string()  # some old db invalid type cases
                elif pa_column_types[col] == "test":
                    pa_column_types[col] = pa.string()
                elif col == "exynos_basic_info_nr_cellid":
                    pa_column_types[col] = pa.uint64()
                elif col in ["lte_m_tmsi", "lte_mmec"]:
                    pa_column_types[col] = pa.int64()
                    
            
            # adj types for pa


            start_time = datetime.datetime.now()
            print("read csv into pa:", table_dump_fp)
            #print("pa_column_types:", pa_column_types)
            #print("local_column_names:", local_column_names)
            padf = csv.read_csv(
                table_dump_fp,
                read_options=csv.ReadOptions(
                    column_names=local_column_names,
                    autogenerate_column_names=False,
                    block_size=10*1024*1024,
                ),
                parse_options=csv.ParseOptions(
                    newlines_in_values=True
                ),
                convert_options=csv.ConvertOptions(
                    column_types=pa_column_types,
                    null_values=[""],
                    strings_can_be_null=True,
                )
                
            )
            append_table_operation_stats(args, table_name, "padf read_csv duration:", (datetime.datetime.now() - start_time).total_seconds())
            
            start_time = datetime.datetime.now()
            
            cur_schema = padf.schema
            field_indexes_need_pd_datetime = []
            fields_need_pd_datetime = []
            field_index_to_drop = []
            has_geom_field = False            
            geom_field_index = None
            field_index = -1
            signalling_symbol_column_index = None
            for field in cur_schema:
                field_index += 1
                if field.name == "time_ms":
                    field_index_to_drop.append(field_index)
                    continue

                if table_name == "signalling" and field.name == "symbol":
                    signalling_symbol_column_index = field_index

                # check if has geom
                if field.name == "geom":
                    has_geom_field = True
                    geom_field_index = field_index

                # change type of field in new schema to timestamp if required
                if is_datetime_col(field.name):
                    fields_need_pd_datetime.append(pa.field(field.name, pa.timestamp('ns')))
                    field_indexes_need_pd_datetime.append(field_index)

            ##### special mods for each table
            if table_name == "signalling":
                # create int column 'direction' for faster queries instead of the string 'symbol' column
                assert signalling_symbol_column_index is not None
                symbol_sr = padf.column(signalling_symbol_column_index).to_pandas().astype(str, copy=False)
                direction_sr = pd.Series(np.zeros(len(symbol_sr), dtype=np.uint8))
                uplink_mask = symbol_sr == "send"
                direction_sr.loc[uplink_mask] = 1
                #print "direction_sr.dtype", direction_sr.dtype
                padf = padf.append_column(
                    # org.apache.spark.sql.AnalysisException: Parquet type not supported: INT32 (UINT_8);
                    # org.apache.spark.sql.AnalysisException: Parquet type not supported: INT32 (UINT_16);
                    # so had to use uint32
                    pa.field("direction", pa.uint32()),
                    pa.Array.from_pandas(direction_sr.astype(np.uint32))
                )
                
                #print "symbol_sr:", symbol_sr
                #print "direction_sr:", direction_sr


            # conv datetime fields with pandas then assign back to padf - do this before adding lat lon as index would change...
            for i in range(len(fields_need_pd_datetime)):                
                index = field_indexes_need_pd_datetime[i]
                field = fields_need_pd_datetime[i]
                print("converting field index {} name {} to datetime...".format(index, field))
                # convert
                converted_sr = pd.to_datetime(padf.column(index).to_pandas(), errors='coerce')
                #print "converted_sr head:", converted_sr.head()
                # assign it back
                # print "padf.schema:\n", padf.schema
                padf = padf.set_column(index, field, pa.Array.from_pandas(converted_sr))


            if has_geom_field:                
                # use pandas to decode geom from hex to binary, then extract lat, lon from wkb
                geom_sr = padf.column(geom_field_index).to_pandas()
                geom_sr_null_mask = pd.isnull(geom_sr)
                geom_sr = geom_sr.str.decode('ascii')
                geom_sr = geom_sr.fillna("")
                #print("ori geom_sr:", geom_sr)
                
                if not geom_format_in_csv_is_wkb:
                    print("geom in csv is in spatialite format - convert to wkb first...")
                    spatialite_geom_sr = geom_sr
                    class_type = "01000020E6100000"
                    endian = "01"  # spatialite_geom_sr.str.slice(start=2, stop=4)
                    point =  spatialite_geom_sr.str.slice(start=86, stop=118)   # 86 + 16 + 16                    
                    geom_sr = endian + class_type + point  # wkb
                    
                
                geom_sr = geom_sr.str.decode("hex")
                geom_sr[geom_sr_null_mask] = None
                #print('wkb geom_sr.head():', geom_sr.head())
                lon_sr = geom_sr.apply(lambda x: None if (pd.isnull(x) or len(x) != WKB_POINT_LAT_LON_BYTES_LEN) else np.frombuffer(x[9:9+8], dtype=np.float64)).astype(np.float64)  # X                                
                lat_sr = geom_sr.apply(lambda x: None if (pd.isnull(x) or len(x) != WKB_POINT_LAT_LON_BYTES_LEN) else np.frombuffer(x[9+8:9+8+8], dtype=np.float64)).astype(np.float64)  # Y
                #print('lon_sr', lon_sr.head())
                #print('lat_sr', lat_sr.head())
                
                ##### assign all three back to padf
                ## replace geom with newly converted to binary geom_sr
                geom_sr_len = len(geom_sr)
                pa_array = None
                if pd.isnull(geom_sr).all():
                    print("geom_sr null all case")
                    pa_array = pa.array(geom_sr.values.tolist()+[b'']).slice(0, geom_sr_len)  # convert tolist() and add [""] then slice() back to ori len required to avoid pyarrow.lib.ArrowInvalid: Field type did not match data type - see azq_report_gen/test_spark_wkb_exception.py
                else:
                    print("not geom_sr null all case")
                    pa_array = pa.array(geom_sr)
                assert pa_array is not None
                padf = padf.set_column(geom_field_index, pa.field("geom", "binary"), pa_array)

                ## insert lat, lon
                padf = padf.add_column(geom_field_index+1, pa.field("lat", pa.float64()), pa.Array.from_pandas(lat_sr))
                padf = padf.add_column(geom_field_index+2, pa.field("lon", pa.float64()), pa.Array.from_pandas(lon_sr))

            # finally drop 'time_ms' legacy column used long ago in mysql where it didnt have milliseconds - not used anymore
            for drop_index in field_index_to_drop:
                padf = padf.remove_column(drop_index)            
                
            #print "padf.schema:\n", padf.schema
            append_table_operation_stats(args, table_name, "padf processing and conversion with pd duration:", (datetime.datetime.now() - start_time).total_seconds())
            
            print("padf len:", len(padf))

            start_time = datetime.datetime.now()

            # use snappy and use_dictionary - https://wesmckinney.com/blog/python-parquet-multithreading/
            pq.write_table(padf, pqfp, flavor='spark', compression=PARQUET_COMPRESSION, use_dictionary=True)
            
            assert os.path.isfile(pqfp)
            append_table_operation_stats(args, table_name, "pq.write_table duration:", (datetime.datetime.now() - start_time).total_seconds())
            print("wrote pqfp:", pqfp)
            
            # if log_table dont return - let it enter pg too...
            if table_name == "logs":
                pass  # import logs table to pg too
            else:
                return True


        if args['target_db_type'] == 'mssql':
            # create fmt format file for that table
            """        
            generate format file:
            https://msdn.microsoft.com/en-us/library/ms178129.aspx

            format file contents:
            https://msdn.microsoft.com/en-us/library/ms191479(v=sql.110).aspx                
            """

            n_local_cols = len(local_column_names)

            fmt = open(table_dump_format_fp,"w")
            fmt.write("11.0\n") # ver - 11.0 = SQL Server 2012
            fmt.write(str(n_local_cols)+"\n") # n cols

            host_field_order = 0 # dyn gen - first inc wil get it to 1
            host_file_data_type = "SQLCHAR"
            prefix_length = 0
            host_file_data_length = 0 # When a delimited text file having a prefix length of 0 and a terminator is imported, the field-length value is ignored, because the storage space used by the field equals the length of the data plus the terminator
            terminator = None # dyn gen
            server_col_order = None # dyn gen
            server_col_name = None # dyn gen
            col_coalition = ""

            for col in local_column_names:
                host_field_order = host_field_order + 1
                if (n_local_cols == host_field_order): #last
                    terminator = azm_db_constants.BULK_INSERT_LINE_SEPARATOR_PARAM
                else:
                    terminator = azm_db_constants.BULK_INSERT_COL_SEPARATOR_PARAM
                if not table_name.startswith("wifi_scanned"):
                    #dprint("remote_column_names: "+str(remote_column_names))
                    pass
                #dprint("col: "+str(col))
                server_col_order = remote_column_names.index(col) + 1 # not 0 based
                server_col_name = col # always same col name
                fmt.write(
                        '{}\t{}\t{}\t{}\t"{}"\t{}\t"{}"\t"{}"\n'.format(
                            host_field_order,
                            host_file_data_type,
                            prefix_length,
                            host_file_data_length,
                            terminator,
                            server_col_order,
                            server_col_name,
                            col_coalition
                            )
                        )
            fmt.flush()
            fmt.close()
        
        # both dump csv and format fmt files are ready        
        # execute bulk insert sql now

        if g_is_ms:
            sqlstr = "bulk insert \"{}\" from '{}' with ( formatfile = '{}' );".format(
                table_name,
                table_dump_fp,
                table_dump_format_fp
            )

        if g_is_postgre:
            colnames = ""
            first = True
            for col in local_column_names:
                if not first:
                    colnames = colnames + ","
                if first:
                    first = False
                colnames = colnames + '"' + col + '"'
                
            sqlstr = "copy \"{}\" ({}) from STDIN with (format csv, NULL '')".format(
                table_name,
                colnames               
            )
        
        #dprint("START bulk insert sqlstr: "+sqlstr)
        g_exec_buf.append((sqlstr, table_dump_fp))
        # print("DONE bulk insert - nrows inserted: "+str(ret.rowcount))
    
    return True


### below are functions not used by azq_db_merge


def sql_adj_line(line):

    global g_is_postgre
    
    sqlstr = line
    #sqlstr = sqlstr.replace('`', '"')
    sqlstr = sqlstr.replace("\" Double", "\" float")
    sqlstr = sqlstr.replace("\" double", "\" float")
    sqlstr = sqlstr.replace("\" DOUBLE", "\" float")
    sqlstr = sqlstr.replace("\" FLOAT", "\" float")

    sqlstr = sqlstr.replace("\" smallint", "\" bigint")
    sqlstr = sqlstr.replace("\" INT", "\" bigint")

    sqlstr = sqlstr.replace('"geom" BLOB','"geom" geometry',1)

    # sqlite pandas regen db uses lowercase
    sqlstr = sqlstr.replace('"geom" blob','"geom" geometry',1)

    if g_is_postgre:
        sqlstr = sqlstr.replace("\" DATETIME", "\" timestamp")
        sqlstr = sqlstr.replace("\" datetime", "\" timestamp")
        sqlstr = sqlstr.replace("\" BLOB", "\" bytea")
        sqlstr = sqlstr.replace("\" blob", "\" bytea")
        sqlstr = sqlstr.replace('" string', '" text')
        
    if g_is_ms:
        sqlstr = sqlstr.replace("\" BLOB", "\" varbinary(MAX)")
        
    # default empty fields to text type
    # sqlstr = sqlstr.replace("\" ,", "\" text,")
    # sqlstr = sqlstr.replace("\" );", "\" text);")

    return sqlstr


def get_table_name(line_adj):
    return line_adj.split(" ")[2].replace("\"", "")


def get_col_names(cols):
    ret = []
    for col in cols:
        ret.append(col[0])
    return ret


def get_remote_columns(args, table_name):
    global g_cursor
    global g_is_ms, g_is_postgre
    
    #dprint("table_name: "+table_name)
    sqlstr = ""
    if g_is_ms:
        sqlstr = "sp_columns @table_name=\"{}\"".format(table_name)
    if g_is_postgre:
        sqlstr = "select * from \"{}\" where false".format(table_name)
        
    #dprint("check table columns sqlstr: "+sqlstr)
    g_cursor.execute(sqlstr)
    #dprint("query execute ret: "+str(ret))
    rows = g_cursor.fetchall()

    '''
    Now get remote column list for this table...

    '''
    remote_columns = []

    if g_is_postgre:
        colnames = [desc[0] for desc in g_cursor.description]
        for col in colnames:
            remote_columns.append([col,""])
        return remote_columns

    if g_is_ms:
        # MS SQL
        for row in rows:
            '''
            MSSQL Column str return example:
            row n: ('master', 'dbo', 'events', 'log_hash', -5, 'bigint', 19, 8, 0, 10, 1, None, None, -5, None, None, 1, 'YES', 108)

            Result:
            col_name: locale
            col_type: text
            '''
            rs = str(row)
            #print("row n: " + rs)
            splitted = rs.split(",")
            #print("splitted:", splitted)
            col_name = splitted[3].replace("'","").strip()
            #print("col_name: "+col_name)
            col_type = splitted[5].replace("'","").strip()
            #dprint("col_type: "+col_type)
            remote_columns.append([col_name,col_type])

        return remote_columns


def exec_creatept_or_alter_handle_concurrency(sqlstr, raise_exception_if_fail=True, allow_exstr_list=[]):
    global g_conn
    global g_cursor

    print(("exec_creatept_or_alter_handle_concurrency START sqlstr: {}".format(sqlstr)))
    
    ret = False
    prev_exstr = ""

    exec_creatept_or_alter_handle_concurrency_max_retries = 2
    
    for retry in range(exec_creatept_or_alter_handle_concurrency_max_retries):
        try:
            
            # use with for auto rollback() on g_conn on expected fails like already exists
            with g_conn as con:
                print(("exec_creatept_or_alter_handle_concurrency retry {} sqlstr: {}".format(retry, sqlstr)))
                execret = g_cursor.execute(sqlstr)
                print(("exec_creatept_or_alter_handle_concurrency retry {} sqlstr: {} execret: {}".format(retry, sqlstr, execret)))

                # commit now otherwise upcoming COPY commands might not see partitions
                con.commit()
                print("exec_creatept_or_alter_handle_concurrency commit done")
            ret = True
            break
        except:
            type_, value_, traceback_ = sys.exc_info()
            exstr = str(traceback.format_exception(type_, value_, traceback_))

            for allow_case in allow_exstr_list:
                if allow_case in exstr:
                    print("exec_creatept_or_alter_handle_concurrency got exception but matches allow_exstr_list allow_case: {} - so treat as success".format(allow_case))
                    ret = True
                    break
            if ret == True:
                break
            
            prev_exstr = "WARNING: exec_creatept_or_alter_handle_concurrency retry {} exception: {}".format(retry, exstr)
            print(prev_exstr)
            sleep_dur = random.random() + 0.5
            time.sleep(sleep_dur)
            
    print(("exec_creatept_or_alter_handle_concurrency DONE sqlstr: {} - ret {}".format(sqlstr, ret)))

    if ret is False and raise_exception_if_fail:
        raise Exception("exec_creatept_or_alter_handle_concurrency FAILED after max retries: {} prev_exstr: {}".format(exec_creatept_or_alter_handle_concurrency_max_retries, prev_exstr))
    
    return ret


def is_datetime_col(col):
    return col.endswith("time") and (not col.endswith("trip_time")) and (not col.endswith("_interruption_time"))


def is_numeric_col_type(col_type):
    cl = col_type.lower()
    if cl in ("int", "integer", "bigint", "biginteger", "real", "double", "float"):
        return True
    return False


def append_table_operation_stats(args, table, operation, duration):
    print("operation_stats: {}:{}:{} seconds".format(table, operation, duration))
    od = args["table_operation_stats"]
    od["table"].append(table)
    od["operation"].append(operation)
    od["duration"].append(duration)
