import oracledb
import csv
from datetime import datetime, timedelta
import os
from glob import glob
import argparse
from dotenv import dotenv_values


env = dotenv_values('.env')

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
    
def extract_data_to_file(sql_fn, out_fn, sdate, edate):
    CONN_INFO = {
        'host': env.get('dbhost'),
        'port': env.get('dbport'),
        'user': env.get('dbuser'),
        'password': env.get('dbpwd'),
        'service_name': env.get('dbname'),
    }

    out_filename = f"{os.getcwd()}/{sdate[4:]}_{out_fn}"
    sql_filename = f"{os.getcwd()}/{sql_fn}.sql"
    total_record = 0
    valid_record_no = 0
    header_flag = 0
    outfn_suf = 0
    max_record = 300001
    sql_statement = get_sql_statement_from_file(sql_filename)
    print(sql_statement)
    
    #encoding="US-ASCII" ISO-8859-1
    with oracledb.connect(**CONN_INFO) as connection:

        print("Database version:", connection.version)
        
        
        with connection.cursor() as cursor:
        
            cursor.arraysize = 1000
            
            begin_time = datetime.now()
            cursor.execute(sql_statement, sdate=sdate, edate=edate)
            print(f"sql_excecute_elapsed_time = {datetime.now() - begin_time}") 
            
            header_cols = []
            for col in cursor.description:
                header_cols.append(col[0])
                
            if header_flag == 0:
                append_to_outfile(f"{out_filename}.csv", [header_cols])
                header_flag = 1
                
            while True:
                rows = cursor.fetchmany()
                rownum = len(rows)
                if rownum < 1:
                    print("No more row")
                    break
                else:
                    
                    total_record += rownum
                    valid_record_no += rownum
                    if valid_record_no < max_record:
                        if outfn_suf:
                            append_to_outfile(f"{out_filename}_{outfn_suf}.csv", rows)
                        else:
                            append_to_outfile(f"{out_filename}.csv", rows)
                    else:
                        valid_record_no = rownum
                        outfn_suf += 1
                        append_to_outfile(f"{out_filename}_{outfn_suf}.csv", [header_cols])
                        append_to_outfile(f"{out_filename}_{outfn_suf}.csv", rows)
                    
                    print(f"Currently exported {valid_record_no}, to file: {out_filename}{outfn_suf}.csv")
                        
    print(f"Total records exported: {total_record}")
    print(f"*******excecute_elapsed_time = {datetime.now() - begin_time}")
    
def parseArguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Optional arguments
    parser.add_argument("-i", "--sql", help="sql", type=str, default=None)
    parser.add_argument("-o", "--csv", help="csv", type=str, default=None)
    parser.add_argument("-l", "--filter", help="filter", type=str, default=None)
    parser.add_argument("-s", "--startdate", help="startdate", type=str, default=datetime.strftime(datetime.now() - timedelta(days = 1), '%Y%m%d'))
    parser.add_argument("-e", "--enddate", help="enddate", type=str, default=datetime.strftime(datetime.now(), '%Y%m%d'))

    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = parser.parse_args()

    return args
                    
if __name__ == "__main__":
    # Parse the arguments
    args = parseArguments()
    _sdate = args.startdate
    _edate = args.enddate
    
    print(f'Filter: {args.filter}*.sql -s {args.startdate} -e {args.enddate}')
    
    files = glob(f'{args.filter}*.sql')
    for file in files:
        _sql = file.split('.')[0]
        _csv = _sql
        #extract_data_to_file(_sql, _csv, _sdate, _edate)
        print(f'extract_data_to_file({_sql}, {_csv}, {_sdate}, {_edate})')


