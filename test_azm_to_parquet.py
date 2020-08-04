import os

def test():
    ret = os.system('python azm_db_merge.py --azm_file "example_logs/358096071732800 2_1_2017 13.12.49.azm" --target_db_type parquet --server_user dummy --server_password dummy --server_database dummy --parquet_folder pq_out')
    return ret

if __name__ == "__main__":
    test()
