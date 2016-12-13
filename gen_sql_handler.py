'''
module to handle merging (importing) of (azqdata.db from
azq .azm files) sqlite3 dump lines into a PostgreSQL and Microsoft SQL Server db.

Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.

'''


import pyodbc
from debug_helpers import dprint
import azm_db_constants
from subprocess import call
import os
from pyodbc import SQL_MAX_TABLE_NAME_LEN

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
        print "using connect_str: "+connect_str
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
        print "using connect_str: "+connect_str
        g_conn = psycopg2.connect(connect_str)
    if (g_conn is None):
        print "psycopg2.connect returned None"
        return False
    
    print "connected"
    
    g_cursor = g_conn.cursor()

    return True


def check_if_already_merged(args, log_ori_file_name):
    
    global g_unmerge_logs_row
    global g_cursor
    global g_exec_buf
    global g_is_ms, g_is_postgre
    
    try:
        print "checking if this log has already been imported/merged: "+log_ori_file_name
        # select log_ori_file_name from logs where log_ori_file_name like '358096071732800 16_11_2016 17.14.15.azm'
        sqlstr = "select * from \"logs\" where \"log_ori_file_name\" like ?"
        if g_is_postgre:
            sqlstr = sqlstr.replace("?","%s")
        dprint("cmd: "+sqlstr)

        row = None

        # use with for auto rollback() on g_conn on exception - otherwise we cant use the cursor again - would fail as: current transaction is aborted, commands ignored until end of transaction block
        ret = None
        with g_conn as c:
            ret = g_cursor.execute(sqlstr, [log_ori_file_name])
            row = g_cursor.fetchone()

        dprint("after cmd")

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
                
                dprint("um0")

                if g_is_postgre or g_is_ms:
                    dprint("upg 0")
                    # row is a tuple - make it a dict
                    dprint("upg 01")
                    cols = get_remote_columns(args,'logs')
                    dprint("upg 1")
                    drow = {}
                    i = 0
                    for col in cols:
                        print row[i]
                        drow[col[0]] = row[i]
                        i = i+1
                    row = drow
                
                dprint("um1")
                    
                print "### unmerge mode - delete start for azm: imei {} , log_start_time {} , log_end_time {}".format(row['imei_id'], row['log_start_time'], row['log_end_time'])
                g_unmerge_logs_row = row
                
                # check if log duration more than 24 hrs - might be time change or bug - dont delete and let user check manually
                MAX_AUTO_UNMERGE_LOG_DURATION_SECONDS = 3600 * 24
                log_dur_seconds = (g_unmerge_logs_row['log_end_time'] - g_unmerge_logs_row['log_start_time']).total_seconds()
                
                print "= log duration seconds: "+str(log_dur_seconds)
                
                if (log_dur_seconds > MAX_AUTO_UNMERGE_LOG_DURATION_SECONDS):
                    if (args['force_unmerge_logs_longer_than_24_hrs']):
                        print "= --force_unmerge_logs_longer_than_24_hrs flag specified - omit log dur > 24 hrs check"
                        pass
                    else:
                        raise Exception("""ABORT: log duration > 24 hrs - might be time change or
                        some time bug that might accidentally remove other
                        logs' data from this imei - please check this azm's .db
                        manually and maybe use
                        --force_unmerge_logs_longer_than_24_hrs
                        to force unmerge this log too - ABORT.""")
                        
        
                                
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
            pass # first time import - no table named logs exists yet
        else:
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


def create(args, line):
    global g_cursor, g_conn
    global g_prev_create_statement_table_name
    global g_prev_create_statement_column_names
    global g_exec_buf
    global g_is_ms, g_is_postgre

    g_prev_create_statement_column_names = None
    
    line_adj = sql_adj_line(line)
    table_name = get_table_name(line_adj)
    
    if (g_unmerge_logs_row is not None):
        print "### unmerge mode - delete all rows for this azm in table: "+table_name
        
        # "time" needs to be quoted to tell that it is a column name!
        # example: select * from event where imei_id like '358096071732800' and "time" between '2016-11-16 16:06:21.510' and '2016-11-16 17:14:15.220'
        
        # remove 3 traling 000 from microsecs str
        start_dt_str = str(g_unmerge_logs_row['log_start_time'])[:-3] 
        end_dt_str = str(g_unmerge_logs_row['log_end_time'])[:-3]
        
        sqlstr = "delete from \""+table_name+"\" where \"imei_id\" = {} and \"time\" between '{}' and '{}'".format(g_unmerge_logs_row['imei_id'], start_dt_str, end_dt_str)
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
        local_columns.append([col_name, col_type])
        local_column_names.append(col_name)
    
    # args['prev_create_statement_column_names'] 
    g_prev_create_statement_column_names = str(local_column_names).replace("'","").replace("[","(").replace("]",")")
    
    remote_column_names = None
    
    try:
        dprint("create sqlstr: "+sqlstr)
        ret = None
        # use with for auto rollback() on g_conn on expected fails like already exists
        with g_conn as c:
            ret = g_cursor.execute(sqlstr)
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
                alter_str = "ALTER TABLE \"{}\" ADD ".format(table_name)
                alter_cols = ""                
                
                for new_col in local_columns_not_in_remote:
                    # not first
                    prefix = ""
                    if (alter_cols != ""):
                        prefix = ", "
                    alter_cols = alter_cols + prefix + new_col
                    
                alter_str = alter_str + alter_cols + ";"
                
                sqlstr = sql_adj_line(alter_str)
                print "execute alter_str: " + sqlstr
                ret = g_cursor.execute(sqlstr)
                print "execute alter_str done"
                
                # re-get remote cols
                remote_columns = get_remote_columns(args, table_name)
                remote_column_names = get_col_names(remote_columns)
                print("get_remote_columns after alter: "+str(remote_column_names))
                
        else:
            raise Exception("FATAL: create table error - : \nemsg:\n "+emsg+" \nsqlstr:\n"+sqlstr)
            
    
    
    if g_bulk_insert_mode:
        
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
                'select * from '+ table_name
                ], shell = False
            )

        if g_is_postgre:
            ret = call(
                [
                args['sqlite3_executable'],
                args['file'],
                "-ascii",
                "-csv",
                '-separator',',',
                '-newline', '\n',
                '.out ' + '"' +table_dump_fp.replace("\\","\\\\") + '"', # double backslash because it needs to go inside sqlite3 cmd parsing again      
                'select * from '+ table_name
                ], shell = False
            )

            table_dump_fp_adj = table_dump_fp + "_adj.csv"
            of = open(table_dump_fp,"r")
            nf = open(table_dump_fp_adj,"w")
            while True:
                ofl = of.readline()
                ofl = ofl.replace(',""',',')
                if ofl == "":
                    break
                nf.write(ofl)
                #nf.write('\n')
            nf.close()
            of.close()
            table_dump_fp = table_dump_fp_adj

        dprint("dump table: "+table_name+" for bulk insert ret: "+str(ret))
        
        if (ret != 0):
            raise Exception("FATAL: dump table: "+table_name+" for bulk insert failed")
            
            
        if (os.stat(table_dump_fp).st_size == 0):
            print "this table is empty..."
            return True
        
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
            dprint("remote_column_names: "+str(remote_column_names))
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
        
        # dprint("START bulk insert sqlstr: "+sqlstr)
        g_exec_buf.append(sqlstr)
        # print("DONE bulk insert - nrows inserted: "+str(ret.rowcount))
    
    return True


### below are functions not used by azq_db_merge


def sql_adj_line(line):

    global g_is_postgre
    
    sqlstr = line
    #sqlstr = sqlstr.replace('`', '"')
    sqlstr = sqlstr.replace("\" blob", "\" text")

    sqlstr = sqlstr.replace("\" double", "\" float")
    sqlstr = sqlstr.replace("\" DOUBLE", "\" float")

    sqlstr = sqlstr.replace("\" smallint", "\" bigint")
    sqlstr = sqlstr.replace("\" INT", "\" bigint")

    if (g_is_postgre):
        sqlstr = sqlstr.replace("\" DATETIME", "\" timestamp")
        sqlstr = sqlstr.replace("\" datetime", "\" timestamp")
        
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


''' currently bulk mode is used/tested - non bulk is not tested regarding atomicity expecially after unmerge yet

def handle_sqlite3_dump_insert(args, line_adj):
    
    global g_cursor
    
    ret = False
        
    if (g_unmerge_logs_row is not None):
        raise Exception("using unmerge mode - handle_sqlite3_dump_insert should never be called - ABORT")
        
    if g_bulk_insert_mode:
        raise Exception("using .schema dump only and manual bulk insert - handle_sqlite3_dump_insert must not be called for mssql - ABORT")
    else:
        ret = mssql_single_insert(args, line_adj)
        
    return ret


def mssql_single_insert(args, line):
    # old insert row by row cpde:    
    global g_prev_create_statement_column_names 
    global g_conn   
    line_adj = line # no change required in these newer versions
    
    # inserts dont have types so just use faster one replace
    # line_adj = line.replace('`', '"')    
    table_name = get_table_name(line_adj)
    
    try:
        
        # dprint("insert ori: "+sqlstr)
        line_adj = line_adj.replace('"'+table_name+'"','"'+table_name+'"'+" "+g_prev_create_statement_column_names,1)
        
        # dprint("insert mod add cols: "+sqlstr)
        
        line_adj = line_adj
                    
        g_cursor.execute(line_adj)
        if g_always_commit:
            g_conn.commit()
        
        return True
            
        
    except Exception as e:
        estr = str(e)
        print("ABORT - Exception while executing insert line: "+line_adj)
        raise e
    
    
    return False
    '''
