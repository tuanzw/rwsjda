import oracledb
import csv
import os
from glob import glob

import schedule
from datetime import timedelta, datetime
import time

from o365api import SharePoint

from dotenv import dotenv_values

env = dotenv_values('.env')

username=env.get('username')
password=env.get('password')
site=env.get('site')
site_name=env.get('site_name')
doc_lib=env.get('doc_lib')
intray=env.get('intray')
outtray=env.get('outtray')
local_folder=env.get('local_folder')

sp = SharePoint(username, password, site, site_name, doc_lib)

def download_files(sp: SharePoint, source_folder, download_path):
    for file in sp._get_files_list(source_folder):
        if file:
            download_file_name = os.path.join(download_path, file.name)
            with open(download_file_name, 'wb') as local_file:
                file.download(local_file).execute_query()
                print(f'[OK] Downloaded: {download_file_name}')
                file.delete_object().execute_query()

def get_file_content(file_path):
    with open(file_path, "rb") as f:
        return f.read()

def upload(sp: SharePoint, to_folder):
	files = glob('*.csv')
	for file in files:
		sp.upload_file(os.path.basename(file), to_folder, get_file_content(file))
		os.remove(file)
		print(f'Uploaded file: {os.path.abspath(file)}')
	
	print('No more files')

def file_to_list(filename):
    with open(filename, 'r', encoding='utf-8') as f_read:
        data = f_read.read()
        return data.split("\n")
        

def append_to_outfile(filename, rows):
    
    with open(filename, 'a', encoding='utf-8') as f_out:
        output = csv.writer(f_out, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar='\\')
        for row in rows:
            output.writerow(row)

def get_sql_statement_from_file(filename):
    sql = ''
    with open(filename, 'r') as f_in:
        for line in f_in:
            sql += line
    return sql
    
def extract_data_to_file(buff_ids, sql_fn, out_fn):
    CONN_INFO = {
        'host': env.get('dbhost'),
        'port': env.get('dbport'),
        'user': env.get('dbuser'),
        'password': env.get('dbpwd'),
        'service_name': env.get('dbname'),
    }

    ids_filename = buff_ids
    
    out_filename = f"{os.getcwd()}\\{out_fn}.csv"
    sql_filename = f"{os.getcwd()}\\{sql_fn}.sql"
    
    total_record = 0
    sql_statement = get_sql_statement_from_file(sql_filename)
    print(sql_statement)
    
    ids_list = file_to_list(ids_filename)
    appended_header = False
    
    with oracledb.connect(**CONN_INFO) as connection:

        print("Database version:", connection.version)
        
        
        with connection.cursor() as cursor:
            for id in ids_list:
                cursor.execute(sql_statement, buff_id=id)
                # append header if not
                if not appended_header:
                    header_cols = []
                    for col in cursor.description:
                        header_cols.append(col[0])
                    append_to_outfile(out_filename, [header_cols])
                    appended_header = True
                # append data
                row = cursor.fetchall()
                rownum = len(row)
                if rownum > 0:
                    total_record += rownum
                    append_to_outfile(out_filename, row)
                else:
                    print(f"Buff: {id}")

    print(f'JDA EXPORTED: {total_record} records to {out_filename}')

def job():
    print('**************Running a scheduled job', datetime.now())
    # download_files(sp, intray, local_folder)

    # files = glob('*.txt')
    # for file in files:
    #     extract_data_to_file(file, 'cc_trans_filter', file.split('.')[0])
    #     os.remove(file)
    #     upload(sp, outtray)    
                    
if __name__=='__main__':
    # schedule.every(30).minutes.until(timedelta(hours=4)).do(job)
    schedule.every(50).seconds.until(timedelta(minutes=1)).do(job)
    while True:
        n = schedule.idle_seconds()
        if n is None:
            break
        elif n > 0:
            print('waiting...', n)
            time.sleep(n)

        schedule.run_pending()
