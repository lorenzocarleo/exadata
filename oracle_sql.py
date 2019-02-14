#!/usr/bin/python
#
# (c) 2019, Lorenzo Carleo <lorenzo.carleo@gmail.com>
# This module has been developed for Banca Intesa
# To query Oracle Exadata with Ansible 2.7

import os
import sys
import time
import datetime
import os.path
import logging
import shutil

try:
  import cx_Oracle
except importError:
  cx_ora = True
else:
  cx_ora= False

# Create a logger object
logger = logging.getLogger()

# Configure logger
logging.basicConfig(filename="oracle_sql.py.log", format='%(filename)s: %(message)s', filemode='w')

# Setting threshold level
logger.setLevel(logging.DEBUG)

date = datetime.datetime.now().strftime("%Y%m%d")
schema = sys.argv[1]
sql = sys.argv[2]
env = sys.argv[3]
id = sys.argv[4]
path = "/" + env + "/" + date + "/" + schema + "/" + id + "/"
temp = "/tmp/" + id + "/" + sql
tdir = "/tmp/" + id

def createdir(path):
  if not os.path.isdir(path):
    try:  
      os.makedirs(path)
      logger.info("directory does not exist : %s " % path)
    except OSError:
      logger.error("Creation of the directory %s failed" % path)
    else:  
      logger.info("Successfully created the directory %s " % path)
  

def movefile(sql,path):
  # make sure that these directories exist
  if os.path.isdir(path):
    dsrc = "/etc/ansible/"
    ddst = path 
    file = sql
    sfile = os.path.join(dsrc, file)
    dfile = os.path.join(ddst, file)
    shutil.move(sfile, dfile)

def read_file(sql):
  try:
    f = open(sql, 'r')
    sqlfile = f.read()
    f.close()
  except IOError as error:
    logger.error('Could not open the SQL file')
  retur

def clean_sqlfile(sql):
  sqlfile = sql.strip()
  sqlfile = sql.lstrip()
  sqlfile = sql.lstrip()
  sqlfile = os.linesep.join([s for s in sql.splitlines() if s])
  return sqlfile


def read_file(sql):
  try:
    f = open(sql,'r')
    sqlfile = f.read()
    f.close()
  except IOError as error_file:
    logger.error("ERRROR opening the file %s " % sql)

def execute_sql(cursor, con, sql):
  if 'INSERT' or 'UPDATE' or 'DELETE' in sql.upper():
    commit = True
  else:
    commit = False
  try:
    cursor.execute("insert into ansible1.product values (50,'raffaele')")
    #cursor.execute(sql)
  except cx_Oracle.DatabaseError as error_cx:
    ecx = error_cx.args
    logger.error('ERROR : on EXECUTE SQL %s' % ecx)
    return False
  if commit:
    con.commit()
  return

def main():
  # variables
  user = 'xxxxxxxxxxxx'
  password = 'xxxxxxxxxx'
  host = 'xxxxxxxxxxx'
  port = '1521'
  sn = 'xxxxxxxxxxxx'
  createdir(path)
#  movefile(sql,path)

  if not cx_ora:
    logger.error("CX_ORACLE must be installed")

  # LTES CONNECT TO THE DATABASE AND OPERATE
  service_conn = '/@%s' % (sn)
  try:
  #dsn = cx_Oracle.makedsn(host, port, sn)
    par = user + "/" + password + "@" + host + "/" + sn
    conn = cx_Oracle.connect(par)
  #conn = cx_Oracle.connect(user, password, dsn)
  except cx_Oracle.DatabaseError as error_cx:
    ecx = error_cx.args
    logger.error('ERROR : Could Not connect to the database  - Service Name/Connect descriptor:')
  cursor = conn.cursor()

  sqlfile = read_file(sql)
  #if len(sql) > 0:
  #  sqlfile = clean_sqlfile(sql)
  #  if sqlfile.endswith('/') or ('CREATE OR REPLACE') in sqlfile.upper():
  #    sqldelim = '/'
  #  else:
  #    sqldelim = ';'

  #  sqlfile = sqlfile.strip(sqldelim)

  #  for query in sqlfile:
  logger.info("query %s " % sql)
  execute_sql(cursor, conn, sql)
  conn.close
  #  stdout = 'Finished running script %s \n Content : \n' % (sql)
  #else:
  #  stderr = 'ERROR : File %s is empty!' % (sql)

if __name__ == '__main__':
  main()
