from suds import client
import ssl
import os
import csv
import paramiko
import shutil
import glob
from dotenv import dotenv_values
from datetime import datetime
from interfaces import order_header, order_line
from logapi import logger

ZWL_APP_ENV = os.getenv('ZWL_APP_ENV')

if not ZWL_APP_ENV:
    ZWL_APP_ENV = 'zwl.uat'

env = dotenv_values(f'.env.{ZWL_APP_ENV}')

ymdhms = datetime.now().strftime('%Y%m%d%H%M%S')

idx_order_id = 0
idx_line_id = 1
idx_sku_id = 2
idx_qty_ordered =  4
idx_udt1 = 5
idx_udt2 = 6
idx_component_id = 7
idx_component_qty = 8

def suds_to_dict(obj):
    if not hasattr(obj, '__keylist__'):
        return obj
    out = {}
    for key in obj.__keylist__:
        value = getattr(obj, key)
        if isinstance(value, list):
            out[key] = [suds_to_dict(v) for v in value]
        else:
            out[key] = suds_to_dict(value)
    return out

def extract_rows(query_results):
    return [query_row.get('row') for query_row in query_results.get('results')]

def write_rows_to_csv(rows, filename):
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(rows)

def get_sql(path):
    sql = ''
    with open(path, 'r') as f:
        for line in f:
            sql += line
    return sql


def getDefaultArg0(cli):
    arg0 = cli.factory.create('interfaceSettings')
    arg0.authenticationKey = env.get('authkey')
    arg0.password = env.get('password')
    arg0.stationId = env.get('stationid')
    arg0.userId = env.get('userid')
    arg0.identifier  = env.get('identifier')
    return arg0

def getMarketOrderDetails(cli):
    arg0 = getDefaultArg0(cli)
    arg1 = cli.factory.create('queryParameters')
    arg1.maxRecords = 10000
    
    arg1.query = get_sql('zwl_order_details.sql')
    response = cli.service.runUserQuery(arg0, arg1)
    response = suds_to_dict(response)
    if not response.get('success'):
        logger.error(f'{response.get('errorCode')}: {response.get('errorDescription')}')
    else:
        if response.get('results'):
            return extract_rows(response)

    return None

def sftp_upload(host, port, username, password, filenames, remote_folder, backup_folder):
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, port=port, username=username, password=password, look_for_keys=False)
        sftp = ssh.open_sftp()
        sftp.chdir(remote_folder)
        for filename in filenames:
            sftp.put(localpath=f"{os.getcwd()}\\{filename}", remotepath=filename)
            shutil.move(src=f"{os.getcwd()}\\{filename}", dst=f"{os.getcwd()}\\{backup_folder}\\{filename}")

def new_header(order_id) -> dict:
    new_header = {k:'@' for k, v in order_header.items()}
    new_header['record_type'] = 'ODH'
    new_header['merge_action'] = 'U'
    new_header['from_site_id'] = 'VNSORH2'
    new_header['client_id'] = 'ZWILLING'
    new_header['owner_id'] = 'ZWILLING'
    new_header['order_id'] = order_id
    new_header['user_def_chk_4'] = 'Y'
    new_header['repack'] = 'N'
    new_header['repack_loc_id'] = ''
    return new_header
def update_kit_order_line(order_id, line_id, sku_id, qty_ordered) -> dict:
    new_line = {**order_line}
    new_line['client_id'] = 'ZWILLING'
    new_line['owner_id'] = 'ZWILLING'
    new_line['merge_action'] = 'U'
    new_line['order_id'] = order_id
    new_line['line_id'] = line_id
    new_line['sku_id'] = sku_id
    new_line['qty_ordered'] = qty_ordered
    new_line['unallocatable'] = 'Y'
    new_line['product_price'] = '@'
    new_line['product_currency'] = '@'
    new_line['user_def_type_1'] = '@'
    new_line['user_def_type_2'] = '@'
    new_line['notes'] = 'Splitted'
    return new_line

def generate_IF_files(rows) -> bool:
    order_id = ''
    kit_order_id = ''
    kit_id = ''
    kit_line_id = 0
    max_line_id = 0
    kit_qty_ordered = 0
    is_row_updated = False
    odl = []
    odh = []
    for row in rows:
        # generate IF_ODL line if previous row is kit_id (is_row_updated) and sku_id is changed
        # do not generate IF_ODL for repeated kit_id until go through all for same kit_id
        if is_row_updated and kit_id != row[idx_sku_id]:
            odl.append(update_kit_order_line(order_id=kit_order_id, line_id=kit_line_id,
                                                sku_id=kit_id, qty_ordered=kit_qty_ordered))
            is_row_updated = False

        if order_id != row[idx_order_id]: # order_id changed -> generate IF_ODH line to update order_header that already proceeded!
            order_id = row[idx_order_id]
            max_line_id = int(row[idx_line_id])
            odh.append(new_header(order_id))


        if row[idx_component_id]: # need to split kit item to component lines
            new_line = {**order_line}
            max_line_id = max_line_id + 1
            is_row_updated = True
            kit_order_id = row[idx_order_id]
            kit_line_id = row[idx_line_id]
            kit_id = row[idx_sku_id]
            kit_qty_ordered = float(row[idx_qty_ordered])
            new_line['client_id'] = 'ZWILLING'
            new_line['owner_id'] = 'ZWILLING'
            new_line['order_id'] = row[idx_order_id]
            new_line['line_id'] = max_line_id
            new_line['sku_id'] = row[idx_component_id]
            new_line['qty_ordered'] = float(row[idx_qty_ordered])*float(row[idx_component_qty])
            new_line['user_def_type_1'] = row[idx_udt1]
            new_line['user_def_type_2'] = row[idx_udt2]
            new_line['user_def_type_8'] = row[idx_sku_id]
            new_line['notes'] = row[idx_line_id]
            odl.append(new_line)

    if is_row_updated:
        # if last row is kit line or kit line in the last order of rows
        #-> generate IF_ODL line to update the kit line if existed.
        odl.append(update_kit_order_line(order_id=kit_order_id, line_id=kit_line_id,
                                            sku_id=kit_id, qty_ordered=kit_qty_ordered))
        is_row_updated = False

    filename_list = []
    if odh or odl:
        output = {'IF__ODH_': odh, 'IF__ODL_': odl}
        for k, v in output.items():
            if v:
                fieldnames = v[0].keys()
                with open(f'{k}ZWL_{ymdhms}.txt', 'w', newline='') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames)
                    writer.writerows(v)
                    filename_list.append(f'{k}ZWL_{ymdhms}.txt')
        return filename_list
    else:
        return None

def main():
    try:
        logger.info('START')
        if hasattr(ssl, '_create_unverified_context'):
            ssl._create_default_https_context = ssl._create_unverified_context
        apiCli = client.Client(env.get('wsdl')) # PROD
        print("API initialized successfully!")
        logger.info('1. API initialized successfully!')
        
        rows = getMarketOrderDetails(apiCli)

        if rows:
            write_rows_to_csv(rows,f'{os.getcwd()}\\{env.get('backup')}\\ZWL_{ymdhms}.csv')
            logger.info('2. Output to csv')
            if_filename = generate_IF_files(rows)
            if if_filename:
                logger.info('3. Generate IF__ files')
                sftp_upload(host=env.get('sftp_host'), port=env.get('sftp_port'),
                    username=env.get('sftp_username'), password=env.get('sftp_password'),
                    remote_folder=env.get('sftp_remote_folder'), filenames=glob.glob(f'IF*{ymdhms}*.txt'),
                    backup_folder=env.get('backup'))
                logger.info(f'4. Put to intray: {if_filename}')
            else:
                logger.info('No IF__ files')
        else:
            logger.info('No IF__ & csv files as no orders need to be proceeded')
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info('END')

if __name__ == '__main__':
    main()





