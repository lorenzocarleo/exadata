#!/usr/bin/python
#
# (c) 2019, Lorenzo Carleo <lorenzo.carleo@gmail.com>
# This module has been developed for Banca Intesa
# To query Oracle Exadata with Ansible 2.7

DOCUMENTATION = '''
---
module: mysql_query
short_description: Query Oracle Exadata Database and return results
description:
   - Mysql SELECT, INSERT, UPDATE or DELETE query
      - returns results on SELECT query

version_added: "0.1_beta"
options:
Changes:
'''
EXAMPLES = """
# Execute arbitrary script1
- oracle_query:
    service_name: "{{ dbname  }}"
    script: /home/oraclnt/orasql.sql
"""
# import OS libraries
import os
# import module snippets
from ansible.module_utils.basic import AnsibleModule

try:
  import cx_Oracle
except importError:
  cx_ora = True
else:
  cx_ora= False

# LETS READ THE SQL FILE
def read_file(module,script):
  try:
    f = open(script, 'r')
    sqlfile = f.read()
    f.close()
  except IOError as error:
    stderr = 'Could not open the SQL file %s.' % (error)
    module.fail_json(msg=stderr, changed=false)
  retur
# LETS STRIP THE LINES
def clean_sqlfile(sqlfile):
  sqlfile = sqlfile.strip()
  sqlfile = sqlfile.lstrip()
  sqlfile = sqlfile.lstrip()
  sqlfile = os.linesep.join([s for s in sqlfile.splitlines() if s])
  return sqlfile


# LETS EXECUTE THE QUERY and COMMIT if it's INSERT,UPDATE,DELETE
def exec_sql(module, cursor, conn, sql):
  if 'INSERT' or 'UPDATE' or 'DELETE' in sql.upper():
    commit = True
  else:
    commit = False

  try:
    cursor.execute(sql)
  except cx_Oracle.DatabaseError as error_cx:
    ecx = error_cx.args
    stderr = 'ERROR : %s on SQL %s' % (ecx.message,sql)
    module.fail_json(msg=stderr, changed=False)
    return False
  if commit:
    conn.commit()
  return True


def main():
  module = AnsibleModule(
    argument_spec=dict(
      user=dict(required=False, aliases=['un', 'username']),
      password=dict(required=False, no_log=True, aliases=['pw']),
      mode=dict(default="normal", choices=["sysasm", "sysdba", "normal"]),
      service_name=dict(required=False, aliases=['sn']),
      hostname=dict(required=False, default='localhost', aliases=['host']),
      port=dict(required=False, default=1521),
      sql=dict(required=False),
      script=dict(required=False)
    )
  )

  user = module.params["user"]
  password = module.params["password"]
  service_name = module.params["service_name"]
  hostname = module.params["hostname"]
  port = module.params["port"]
  script = module.params["script"]

  # LETS SHOW IF THE CX_ORACLE COULD BE IMPORTED PROPERLY ON LINE 35
  if not cx_ora:
    stderr = 'CX_ORACLE must be installed'
    module.fail_json(msg=stderr)

  # LTES CONNECT TO THE DATABASE AND OPERATE
  service_conn = '/@%s' % (service_name)
  try:
    dns = cx_Oracle.makedns(host=hostname, port=port, service_name=service_name)
    connect = dns
    conn = cx_Oracle.connect(user, password, dns)

  except cx_Oracle.DatabaseError as error_cx:
    ecx = error_cx.args
    stderr = 'ERROR : Could Not connect to the database %s - Service Name/Connect descriptor: %s' % (ecx.message,connect)
    module.fail_json(msg=stderr, changed=False)

  cursor = conn.cursor()

  sqlfile = read_file(module, script)
  if len(sqlfile) > 0:
    sqlfile = clean_sql(sqlfile)
    if sqlfile.endswith('/') or ('CREATE OR REPLACE') in sqlfile.upper():
      sqldelim = '/'
    else:
      sqldelim = ';'

    sqlfile = sqlfile.strip(sqldelim)

    for query in sql:
      execute_sql(module, cursor, conn, query)
    stdout = 'Finished running script %s \n Content : \n%s\n' % (script, sqlfile)
    module.exit_json(msg=stdout, changed=True)
  else:
    stderr = 'ERROR : File %s is empty!' % (script)
    module.fail_json(msg=stderr, changed=False)

if __name__ == '__main__':
  main()
