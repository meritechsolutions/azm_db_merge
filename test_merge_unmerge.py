import subprocess
import psycopg2


def make_sure_public_logs_doesnt_exist(dbcon, dbcur):
    try:
        with dbcon:
            dbcur.execute("select * from public.logs")
            raise Exception("INVALID: public.logs should not be created - azm_db_merge is not honoring --pg_schema option")
    except Exception as e:
        if "--pg_schema option" in str(e):
            raise(e)
        elif "does not exist" in str(e):            
            pass
        else:
            exstr = "check public.logs must not exist - Invalid case - ABORT"
            print exstr
            raise Exception(exstr)


def test():

    server_user = "kasidit"
    server_pass = "pass"
    TMP_DB = "tmp_pytest_db"
    connect_str = "dbname=postgres user={} password={}".format(
        server_user,
        server_pass
    )
    dbcon = psycopg2.connect(connect_str)

    ### delete existing TMP_DB, create a new empty one
    dbcon.autocommit = True  # required to do drop database below
    dbcur = dbcon.cursor()
    try:
        with dbcon:
            dbcur.execute("drop database {};".format(TMP_DB))
    except Exception as e:
        if "does not exist" in str(e):
            pass
        else:
            print "drop TMP_DB exception:", str(e)
            raise e
        
    dbcur.execute("create database {};".format(TMP_DB))

    # create new db con later after merge
    dbcur.close()
    dbcon.close()
    dbcon = None

    # read azm's db schema mode
    assert 0 == subprocess.call("./ex_get_db_schema.sh",shell=True)

  
    ### basic operations in existing db
    # try unmerge from pg first if exists
    cmd = 'python azm_db_merge.py --unmerge --azm_file "example_logs/" --target_db_type postgresql --server_user {} --server_password {} --server_database {} --pg_schema all_logs | grep "unmerge mode called on an empty database"'.format(server_user, server_pass, TMP_DB)
    print "test cmd:", cmd
    assert 0 == subprocess.call(cmd, shell=True)

    # first import + table creation in schema
    cmd = 'python azm_db_merge.py --azm_file "example_logs/" --target_db_type postgresql --server_user {} --server_password {} --server_database {} --pg_schema all_logs'.format(server_user, server_pass, TMP_DB)
    assert 0 == subprocess.call(cmd,shell=True)

    # new dbcon to TMP_DB database - not autocommit anymore
    connect_str = "dbname={} user={} password={}".format(
        TMP_DB,
        server_user,
        server_pass
    )

    dbcon = psycopg2.connect(connect_str)
    dbcur = dbcon.cursor()

    make_sure_public_logs_doesnt_exist(dbcon, dbcur)
    
    dbcur.execute("select * from all_logs.logs")
    assert 1 == dbcur.rowcount

    # unmerge it
    cmd = 'python azm_db_merge.py --azm_file "example_logs/" --target_db_type postgresql --server_user {} --server_password {} --server_database {} --pg_schema all_logs --unmerge'.format(server_user, server_pass, TMP_DB)
    assert 0 == subprocess.call(cmd,shell=True)
    dbcur.execute("select * from all_logs.logs")
    assert 0 == dbcur.rowcount
    make_sure_public_logs_doesnt_exist(dbcon, dbcur)
    
    # merge it back again
    cmd = 'python azm_db_merge.py --azm_file "example_logs/" --target_db_type postgresql --server_user {} --server_password {} --server_database {} --pg_schema all_logs'.format(server_user, server_pass, TMP_DB)
    assert 0 == subprocess.call(cmd,shell=True)
    dbcur.execute("select * from all_logs.logs")
    assert 1 == dbcur.rowcount
    make_sure_public_logs_doesnt_exist(dbcon, dbcur)

    
if __name__ == '__main__':
    test()
