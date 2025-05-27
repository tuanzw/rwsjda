from suds import client
import ssl
import os
import smtplib
from dotenv import dotenv_values
from datetime import datetime
from logapi import logger

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.header import Header

from jinja2 import Environment, FileSystemLoader

# FISCH_APP_ENV = os.getenv('FISCH_APP_ENV')

FISCH_APP_ENV = 'fisch.prod'

# if not FISCH_APP_ENV:
#     FISCH_APP_ENV = 'fisch.uat'

env = dotenv_values(f'.env.{FISCH_APP_ENV}')

# SMTP Credentials
mail_host = env.get('mail_host')
mail_host_port = env.get('mail_host_port')
mail_user = env.get('mail_user')
mail_pass = env.get('mail_pass')

ymdhms = datetime.now().strftime('%Y%m%d%H%M%S')

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

def getDG_Orders(cli):
    arg0 = getDefaultArg0(cli)
    arg1 = cli.factory.create('queryParameters')
    arg1.maxRecords = 10000
    
    arg1.query =  get_sql('fisch_expiry_sku.sql')
    response = cli.service.runUserQuery(arg0, arg1)
    response = suds_to_dict(response)
    if not response.get('success'):
        logger.error(f'{response.get('errorCode')}: {response.get('errorDescription')}')
    else:
        if response.get('results'):
            return extract_rows(response)

    return None


def build_mail_body(rows: list) -> str:
    fileloader = FileSystemLoader("templates")
    jinja_env = Environment(loader=fileloader)
    htmlBody = jinja_env.get_template("fisch_expiry_sku.html").render(rows=rows)
    return htmlBody

def Send_Html_Mail(to_list,subject,content, attachments = None):
    try:
        smtpObj = smtplib.SMTP(mail_host, mail_host_port)  # Create an SMTP object with the host and port
        smtpObj.connect(mail_host, mail_host_port)  # Connect to Amazon SES
        smtpObj.starttls()  # Start TLS encryption for secure email transmission
        smtpObj.login(mail_user, mail_pass)  # Login with your AWS credentials
        msg = MIMEMultipart()
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = "no.reply@ap.rhenus.com"
        msg['To'] = to_list
        msg['X-Priority'] = '1'
        # style = '<style>table {border-collapse: collapse;} table, th, td {border: 1px solid black;}</style>'
        
        msg.attach(MIMEText(content, 'html', 'utf-8'))  # Create a HTML message with utf-8 encoding and attach it to the email message object
        if attachments != None:
            # check if attachments is a list
            if not isinstance(attachments, list):
                attachments = [attachments]
            for attachment in attachments:
                part = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
                part['Content-Type'] = 'application/octet-stream'
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
                msg.attach(part)
            

        smtpObj.sendmail("no.reply@ap.rhenus.com", to_list.split(','), msg.as_string()) 
        
    except Exception as e:
        print(f"Error: unable to send email {e}")
    finally:
        smtpObj.quit()

def main():
    try:
        logger.info(f'START with using {FISCH_APP_ENV}, debug {env.get('debug')}')
        if hasattr(ssl, '_create_unverified_context'):
            ssl._create_default_https_context = ssl._create_unverified_context
        apiCli = client.Client(env.get('wsdl')) # PROD
        print("API initialized successfully!")
        logger.info('1. API initialized successfully!')
        
        rows = getDG_Orders(apiCli)
        if rows:
            content = build_mail_body(rows)
            Send_Html_Mail(env.get('receipients_expiry'), f'{datetime.now().strftime('%Y%m%d')} - Early Warning: Items approaching best-before date – plan sell-off', content)
        else:
            print(f'{FISCH_APP_ENV.split('.')[1].upper()} - No expiry SKU')
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info('END')

if __name__ == '__main__':
    main()





