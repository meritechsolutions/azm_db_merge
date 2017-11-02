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

### below are functions required/used by azq_db_merge        

def connect(args):
    global g_bulk_insert_mode
    global g_dir_processing_azm
    global g_cursor, g_conn
    global g_exec_buf
    global g_is_ms, g_is_postgre

    if (args['target_db_type'] == 'postgresql'):
        print "PostgreSQL mode initializing..."
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
        print "Connecting... Target DBMS type: mssql"
        dprint("connect args: {} {} {} {}".format(args['server_url'],
                                                  args['server_user'],
                                                  args['server_password'],
                                                  args['server_database']
                                                  )
               )
        driver = args['mssql_odbc_driver']
        connect_str = 'DRIVER={};SERVER={};DATABASE={};UID={};PWD={}'.format(
                driver,
                args['server_url'],
                args['server_database'],
                args['server_user'],
                args['server_password'])
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
        print "Connecting... Target DBMS type: PostgreSQL"
        # example: conn = psycopg2.connect("dbname=azqdb user=azqdb")
        connect_str = "dbname={} user={} password={}".format(
                args['server_database'],
                args['server_user'],
            args['server_password']
                )
        if args['docker_postgres_server_name'] != None:
            connect_str = "host="+args['docker_postgres_server_name']+" "+connect_str
        #unsafe as users might see in logs print "using connect_str: "+connect_str
        args['connect_str'] = connect_str
        g_conn = psycopg2.connect(connect_str)
    if (g_conn is None):
        print "psycopg2.connect returned None"
        return False
    
    print "connected"
    
    g_cursor = g_conn.cursor()

    # post connect steps for each dbms
    if g_is_postgre and not args['unmerge']:

       try_cre_postgis(schema="public") # create postgis at public schema first
        
       if args["pg_schema"] != "public":
            print "pg mode create pg_schema:", args["pg_schema"]
            try:
                with g_conn as c:
                    ret = g_cursor.execute("create schema "+args["pg_schema"])
                    c.commit()
                    print "success: create schema "+args["pg_schema"]+ " success"
                    
            except Exception as e:
                estr = str(e)
                if 'already exists' in estr:
                    dprint("schema already exists")
                    pass
                else:
                    print("FATAL: CREATE schema failed:"+args["pg_schema"])
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
            sql = "CREATE EXTENSION postgis SCHEMA {}".format(schema)
            print "try: CREATE EXTENSION postgis on schema:", schema, "sql:", sql
            ret = g_cursor.execute(sql)
            c.commit()
            print "success: CREATE EXTENSION postgis"
    except Exception as e:
        estr = str(e)
        if 'extension "postgis" already exists' in estr:
            print("postgis already exists")
            pass
        else:
            print("FATAL: CREATE EXTENSION postgis - failed - please make sure postgis is correctly installed.")
            raise e


def check_if_already_merged(args, log_ori_file_name):    
    global g_unmerge_logs_row
    global g_cursor
    global g_exec_buf
    global g_is_ms, g_is_postgre

    if args["pg_schema"] != "public":
        g_cursor.execute("SET search_path = '{}','public';".format(args["pg_schema"]))
    
    try:
        print "checking if this log has already been imported/merged: "+log_ori_file_name
        # select log_ori_file_name from logs where log_ori_file_name like '358096071732800 16_11_2016 17.14.15.azm'
        sqlstr = "select \"log_hash\" from \"logs\" where \"log_ori_file_name\" like ?"
        if g_is_postgre:
            sqlstr = sqlstr.replace("?","%s")
        print("check log cmd: "+sqlstr)

        row = None

        # use with for auto rollback() on g_conn on exception - otherwise we cant use the cursor again - would fail as: current transaction is aborted, commands ignored until end of transaction block
        ret = None
        with g_conn as c:
            ret = g_cursor.execute(sqlstr, [log_ori_file_name])
            row = g_cursor.fetchone()

        print("after cmd check if exists row:", row)

        if (row is None):
            
            # azm never imported
            
            if (args['unmerge']):
                # unmerge mode - this azm is not in target db
                raise Exception("ABORT: This azm is already not present in target db's logs' table")               
            else:
                print "This log hasn't been imported into target db yet - ok to proceed"
                pass
                return True
            
        else:
            
            # azm already imported
                        
            if (args['unmerge']):
                
                dprint("um0: row: "+str(row))

                if g_is_postgre or g_is_ms:
                    dprint("upg 0")
                    # row is a tuple - make it a dict
                    dprint("upg 01")
                    # now we only need 'log_hash' to unmerge and the used odbc cant parse geom too - cols = get_remote_columns(args,'logs')
                    cols = [['log_hash', 'bigint']]
                    dprint("upg 1: cols: "+str(cols))
                    drow = {}
                    i = 0
                    for col in cols:
                        print row[i]
                        drow[col[0]] = row[i]
                        i = i+1
                    row = drow
                
                dprint("um1")
                    
                print "### unmerge mode - delete start for azm: log_hash {}".format(row['log_hash'])
                g_unmerge_logs_row = row
                
                sqlstr = "delete from \"logs\" where \"log_ori_file_name\" like '{}'".format(log_ori_file_name)
                
                g_exec_buf.append(sqlstr)
                print "delete from logs table added to g_exec_buf..."
                
            else:
                raise Exception("ABORT: This log ({}) has already been imported/exists in target db (use --unmerge to remove first if you want to re-import).".format(log_ori_file_name))
                
                                             
    except Exception as e:
        estr = str(e)
        if ("Invalid object name 'logs'" in estr or '42S02' in estr
            or 'relation "logs" does not exist' in estr):
            print "looks like this is the first-time log import - no table named logs exists yet - ok..."
            if args['unmerge']:
                raise Exception("--unmerge mode called on an empty database: no related 'logs' table exist yet")
            # first time import - no table named logs exists yet
        
        else:
            type_, value_, traceback_ = sys.exc_info()
            exstr = traceback.format_exception(type_, value_, traceback_)
            print "re-raise exception e - ",exstr
            raise e
        
    return False
        

def close(args):
    global g_cursor, g_conn
    global g_exec_buf    
    global g_prev_create_statement_column_names
    global g_prev_create_statement_table_name
    global g_bulk_insert_mode
    global g_unmerge_logs_row
    
    print "mssql_handler close() - cleanup()"
    
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
            print "warning: mssql cursor close failed: "+str(e)
        
    if g_conn is not None:    
        try:
            g_conn.close()
            g_conn = None
        except Exception as e:
            print "warning: mssql conn close failed: "+str(e)
            
    return True

    
def commit(args, line):
    global g_cursor, g_conn 
    global g_prev_create_statement_table_name
    global g_exec_buf
    
    g_prev_create_statement_table_name = None

    n = len(g_exec_buf)

    # make sure all create/alters are committed
    g_conn.commit()
    
    print "### total cmds to execute for operation: "+str(n)
    
    i = 0
    for buf in g_exec_buf:
        i = i + 1
        print "# execute cmd {}/{}: {}".format(i, n, buf)
        g_cursor.execute(buf)
        
    print("### all cmds exec success - COMMIT now...")    
    g_conn.commit()
    print("### COMMIT success...")
    
    return True

def find_and_conv_spatialite_blob_to_wkb(csv_line):
    spat_blob_offset = csv_line.find('0001E6100000')
    if spat_blob_offset == -1:
        dprint
        return csv_line
    part = csv_line[spat_blob_offset:spat_blob_offset+120+1]
    dprint("csv_line spatialite_geom_part: "+part)

    spatialite_geom_contents = ""
    if (g_is_postgre and part[120] == ',') or (g_is_ms and part[120] == '\t'):
        spatialite_geom_contents = part[0:120]
    else:
        dprint("check of spatialite_geom_part - failed - abort")
        return csv_line
    
    dprint("spatialite_geom_contents: len "+str(len(spatialite_geom_contents))+" val: "+spatialite_geom_contents)
    # convert spatialite geometry blob to wkb
    """

    Spatialite BLOB Format (Point)
    ------------------------------

    http://www.gaia-gis.it/gaia-sins/BLOB-Geometry.html
    example:
    0001E6100000DD30C0F46C2A594041432013008E2B40DD30C0F46C2A594041432013008E2B407C01000000DD30C0F46C2A594041432013008E2B40FE

    parse:
    spatialite header: 00 (str_off 0 str_len 2)
    endian: 01 (str_off 2 str_len 2)
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
    uint32 wkbType; // 1
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
            wkb = endian + class_type + point
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
        dprint("wkb: "+wkb)
        csv_line = csv_line.replace(spatialite_geom_contents,wkb,1)
    else:
        dprint("not entering spatialite blob parse - len "+str(len(spatialite_geom_contents)))

    dprint("find_and_conv_spatialite_blob_to_wkb ret: "+csv_line)
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
    schema_per_log_name = "per_log_{}".format(table_name)

    if table_name == "logs":
        uline = line.replace('"log_hash" BIGINT,','"log_hash" BIGINT UNIQUE,',1)
        print "'logs' table cre - make log_hash unique for this table: ", uline
        line_adj = sql_adj_line(uline)
        
    if args['import_geom_column_in_location_table_only'] and table_name != "location":
        line_adj = sql_adj_line(line.replace(',"geom" BLOB','',1))

    if (g_unmerge_logs_row is not None):
        print "### unmerge mode - delete all rows for this azm in table: "+table_name
        
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
    dprint("ls :" + str(ls))
    ls = ls[1].split(");")[0]
    # split by comma
    ls = ls.split(",")
    
    # parse column names and keep for insert commands
    local_columns = []
    local_column_names = []    
    for lsp in ls:
        splitted = lsp.split('"')
        dprint("splitted: "+str(splitted))
        col_name = splitted[1]
        col_type = splitted[2].strip()                

        omit_col = False

        """
        import_geom_column_in_location_table_only feature already implemented at line_adj above
        if args['import_geom_column_in_location_table_only'] and col_name == "geom" and table_name != "location":
            omit_col = True
        """
            
        if omit_col == False:
            local_columns.append([col_name, col_type])
            local_column_names.append(col_name)
    
    # args['prev_create_statement_column_names'] 
    g_prev_create_statement_column_names = str(local_column_names).replace("'","").replace("[","(").replace("]",")")
    
    remote_column_names = None

    is_contains_geom_col = False
    
    try:
        dprint("create sqlstr: "+sqlstr)
        
        if g_is_postgre:

            if args['pg10_partition_by_log']:
                if table_name == "logs":
                    # dont partition logs table
                    pass
                else:
                    # create target partition for this log + table
                    # ok - partition this table
                    sqlstr = sqlstr.replace(";","") +" PARTITION BY LIST (log_hash);"
                    try:                        
                        with g_conn as c:
                            g_cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}';".format(schema_per_log_name))
                            if bool(g_cursor.rowcount):
                                print "schema_per_log_name already exists:", schema_per_log_name
                                pass
                            else:
                                print "cre schema now because: NOT schema_per_log_name already exists:", schema_per_log_name
                                with g_conn as c:
                                    c_table_per_log_sql = "create schema {};".format(schema_per_log_name)
                                    ret = g_cursor.execute(c_table_per_log_sql)
                                    g_conn.commit()
                                    print "success: create per_log ["+c_table_per_log_sql+"] success"
                    except:
                        type_, value_, traceback_ = sys.exc_info()
                        exstr = str(traceback.format_exception(type_, value_, traceback_))
                        print "WARNING: create table_per_log schema failed:", exstr


            dprint("create sqlstr postgres mod: "+sqlstr)
            is_contains_geom_col = True            
            # postgis automatically creates/maintains "geometry_columns" 'view'

        

        if g_is_ms:
            dprint("create sqlstr mod mssql geom: "+sqlstr)
            is_contains_geom_col = True            

        if g_is_postgre:
            with g_conn as c:
                g_cursor.execute("select * from information_schema.tables where table_schema=%s and table_name=%s", (args["pg_schema"],table_name,))
                if bool(g_cursor.rowcount):
                    print "omit already existing table - raise exception to check columns instead"
                    raise Exception("table {} already exists - no need to create".format(table_name))
                    
        
        ret = None
        # use with for auto rollback() on g_conn on expected fails like already exists
        with g_conn as c:
            ret = g_cursor.execute(sqlstr)
        # commit now otherwise COPY might not see partitions
        g_conn.commit()
        dprint("create execute ret: "+str(ret))
        
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
            
            dprint("""This table already exists -
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
                dprint("n_cols_to_add == 0 - no need to alter table")
            else:
                print "n_cols_to_add: " + str(n_cols_to_add) + " - need to alter table - add cols:" + str(local_columns_not_in_remote) + "\nremote_cols:\n"+str(remote_columns)
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
                print "execute alter_str: " + sqlstr
                ret = g_cursor.execute(sqlstr)
                print "execute alter_str done - commit now (commit required for alter otherwise the next COPY cmds wont work..."
                g_conn.commit()
                
                # re-get remote cols
                remote_columns = get_remote_columns(args, table_name)
                remote_column_names = get_col_names(remote_columns)
                print("get_remote_columns after alter: "+str(remote_column_names))
                
        else:
            raise Exception("FATAL: create table error - : \nemsg:\n "+emsg+" \nsqlstr:\n"+sqlstr)
            

    if g_bulk_insert_mode:

        if args['pg10_partition_by_log']:
            if table_name == "logs":
                # dont partition logs table
                pass
            else:
                log_hash = args['log_hash']
                ntn = "log_hash_{}".format(log_hash) # simpler name because we got cases where schema's table name got truncated: activate_dedicated_eps_bearer_context_request_params_3170932708
                pltn = "{}.{}".format(schema_per_log_name, ntn)
                with g_conn as c:
                    check_sql = "select * from information_schema.tables where table_schema='{}' and table_name='{}'".format(schema_per_log_name, ntn)
                    print "check_sql:", check_sql
                    g_cursor.execute(check_sql)
                    if bool(g_cursor.rowcount):
                        print "omit create already existing per_log table:", pltn
                    else:
                        print "NOT omit create already existing per_log table:", pltn
                        cre_target_pt_sql = "CREATE TABLE {} PARTITION OF {} FOR VALUES IN ({});".format(pltn, table_name, log_hash)
                        dprint("cre_target_pt_sql:", cre_target_pt_sql)
                        try:
                            with g_conn as c:
                                ret = g_cursor.execute(cre_target_pt_sql)
                                print("cre_target_pt_sql execute ret: "+str(ret))
                                g_conn.commit()
                        except:
                            type_, value_, traceback_ = sys.exc_info()
                            exstr = str(traceback.format_exception(type_, value_, traceback_))
                            print "WARNING: create target partition for this log + table exception:", exstr
        
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

        geom_col_index = -1

        i = 0
        col_select = ""
        first = True
        dprint("local_columns: "+str(local_columns))
        for col in local_columns:
            col_name = col[0]
            col_type = col[1]
            if first:
                first = False
            else:
                col_select = col_select + ","
                
            pre = " "
            post = ""
            if col_type == "geometry" or (g_is_postgre and col_type == "bytea") or (g_is_ms and col_type.startswith("varbinary")):
                pre = " hex("
                post = ")"
                if col_name == "geom":
                    geom_col_index = i                
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
                'select '+col_select+' from '+ table_name
                ], shell = False
            )

        if g_is_postgre:
            dump_cmd = [
                args['sqlite3_executable'],
                args['file'],
                "-ascii",
                "-csv",
                '-separator',',',
                '-newline', '\n',
                '.out ' + '"' +table_dump_fp.replace("\\","\\\\") + '"', # double backslash because it needs to go inside sqlite3 cmd parsing again
                'select '+col_select+' from '+ table_name
                ]
            dprint("dump_cmd:", dump_cmd)
            ret = call(
                dump_cmd,
                shell=False
            )

        table_dump_fp_adj = table_dump_fp + "_adj.csv"
        of = open(table_dump_fp,"r")
        nf = open(table_dump_fp_adj,"wb") # wb required for windows so that \n is 0x0A - otherwise \n will be 0x0D 0x0A and doest go with our fmt file and only 1 row will be inserted per table csv in bulk inserts... 

        while True:
            ofl = of.readline()
            if g_is_postgre:
                ofl = ofl.replace(',""',',')
            
            ofl = find_and_conv_spatialite_blob_to_wkb(ofl)
            
            if ofl == "":
                break
            
            nf.write(ofl)
            #nf.write('\n')
        nf.close()
        of.close()

        table_dump_fp = table_dump_fp_adj

        dprint("dump table: "+table_name+" for bulk insert ret: "+str(ret))
        
        if (ret != 0):
            print "WARNING: dump table: "+table_name+" for bulk insert failed - likely sqlite db file error like: database disk image is malformed. In many cases, data is still correct/complete so continue."
            
            
        if (os.stat(table_dump_fp).st_size == 0):
            print "this table is empty..."
            return True
        
        # if control reaches here then the table is not empty
        
        if g_is_ms and is_contains_geom_col:
            # add this table:geom to 'geometry_columns' (table_name was set to UNIQUE so it will fail if already exists...
            """ somehow not working - let QGIS detect automatically itself for now...
            try:
                insert_geomcol_sqlstr = "INSERT INTO \"geometry_columns\" VALUES('azq','dbo','{}','geom',NULL,4326,'POINT');".format(table_name)
                dprint("insert_geomcol_sqlstr: "+insert_geomcol_sqlstr)
                ret = g_cursor.execute(insert_geomcol_sqlstr)
                print "insert this table:geom into geometry_columns done"
            except Exception as e:
                estr = str(e)
                dprint("insert this table:geom into geometry_columns exception: "+estr)
                pass
            """
            
        
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
                dprint("remote_column_names: "+str(remote_column_names))
                pass
            dprint("col: "+str(col))
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
                
            sqlstr = "copy \"{}\" ({}) from '{}' with (format csv, NULL '')".format(
                table_name,
                colnames,
                table_dump_fp
            )
        
        dprint("START bulk insert sqlstr: "+sqlstr)
        g_exec_buf.append(sqlstr)
        # print("DONE bulk insert - nrows inserted: "+str(ret.rowcount))
    
    return True


### below are functions not used by azq_db_merge


def sql_adj_line(line):

    global g_is_postgre
    
    sqlstr = line
    #sqlstr = sqlstr.replace('`', '"')
    sqlstr = sqlstr.replace("\" double", "\" float")
    sqlstr = sqlstr.replace("\" DOUBLE", "\" float")

    sqlstr = sqlstr.replace("\" smallint", "\" bigint")
    sqlstr = sqlstr.replace("\" INT", "\" bigint")

    sqlstr = sqlstr.replace('"geom" BLOB','"geom" geometry',1)

    if g_is_postgre:
        sqlstr = sqlstr.replace("\" DATETIME", "\" timestamp")
        sqlstr = sqlstr.replace("\" datetime", "\" timestamp")
        sqlstr = sqlstr.replace("\" BLOB", "\" bytea")
        
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
    
    dprint("table_name: "+table_name)
    sqlstr = ""
    if g_is_ms:
        sqlstr = "sp_columns @table_name=\"{}\"".format(table_name)
    if g_is_postgre:
        sqlstr = "select * from \"{}\" where false".format(table_name)
        
    dprint("check table columns sqlstr: "+sqlstr)
    ret = g_cursor.execute(sqlstr)
    dprint("query execute ret: "+str(ret))
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
            row n: {0: u'azqdemo', 1: u'dbo', 2: u'android_metadata', 3: u'locale', 4: -1, 5: u'text', u'DATA_TYPE': -1, 7: 2147483647, 8: None, 9: None, 10: 1, 11: None, 12: None, 13: -1, 14: None, 15: 2147483647, u'COLUMN_DEF': None, 17: u'YES', 18: 35, u'SCALE': None, u'TABLE_NAME': u'android_metadata', u'SQL_DATA_TYPE': -1, 6: 2147483647, u'NULLABLE': 1, u'REMARKS': None, u'CHAR_OCTET_LENGTH': 2147483647, u'COLUMN_NAME': u'locale', u'SQL_DATETIME_SUB': None, u'TABLE_OWNER': u'dbo', 16: 1, u'RADIX': None, u'SS_DATA_TYPE': 35, u'TYPE_NAME': u'text', u'PRECISION': 2147483647, u'IS_NULLABLE': u'YES', u'LENGTH': 2147483647, u'ORDINAL_POSITION': 1, u'TABLE_QUALIFIER': u'azqdemo'}

            Result:
            col_name: locale
            col_type: text
            '''
            rs = str(row)
            dprint("row n: " + rs)
            splitted = rs.split(", u")
            col_name = splitted[3].split("'")[1]
            dprint("col_name: "+col_name)
            col_type = splitted[4].split("'")[1]
            dprint("col_type: "+col_type)
            remote_columns.append([col_name,col_type])

        return remote_columns


