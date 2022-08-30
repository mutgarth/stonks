import MySQLdb as mdb
import os
from dotenv import dotenv_values, find_dotenv

dotenv_path = find_dotenv('vars.env')
config = dotenv_values(dotenv_path)

def db_connect():
    db_host = config.get('DATABASE_HOST')
    db_user = config.get('DATABASE_USER')
    db_pass = config.get('DATABASE_PW')
    db_name = config.get('DATABASE_NAME')

    return mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    