import datetime
import json
import logging
import pdb

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

DEFAULT_TABLE_NAME = 'githubarchive'
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090

class ThriftHbase:
    transport = None
    protocol = None
    client = None

    def __init__(self):
        # Connect to HBase Thrift server
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(DEFAULT_HOST, DEFAULT_PORT))
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        # Do Something
        self.client = Hbase.Client(self.protocol)

    def close(self):
        self.transport.close()

    def is_open(self):
        return self.transport.isOpen()

    def reconnect(self):
        if not self.transport.isOpen():
            self.transport.open()

if __name__ == '__main__':
    th = ThriftHbase()

    # Test whether a table exists
    th.reconnect()
    tables = th.client.getTableNames()
    found = False

    for table in tables:
        if table == DEFAULT_TABLE_NAME:
            found = True

    # create table if not found
    if not found:
        th.reconnect()
        th.client.createTable(DEFAULT_TABLE_NAME, [
            Hbase.ColumnDescriptor(name='created_at'),
            Hbase.ColumnDescriptor(name='actor'),
            Hbase.ColumnDescriptor(name='repository'),
            Hbase.ColumnDescriptor(name='type')
        ])

    # parse JSON
    line = '{"created_at":"2013-09-20T09:03:41-07:00","payload":{"shas":[["7af40c10725e4fe8a5b593ef887bc5757f3e7495","inodetelic@gmail.com","Added year check for today date","inod",true],["dea687bf8213acf25ccc05c5122166e02ac5022d","donatj@gmail.com","Merge pull request #2 from inod/patch-1Added year check for today date","Jesse Donat",true]],"size":2,"ref":"refs/heads/master","head":"dea687bf8213acf25ccc05c5122166e02ac5022d"},"public":true,"type":"PushEvent","url":"https://github.com/donatj/SimpleCalendar/compare/bcdd0b6799...dea687bf82","actor":"donatj","actor_attributes":{"login":"donatj","type":"User","gravatar_id":"50cb4981bbaa51b1713269a29f78d826","name":"Jesse Donat","company":"Donat Studios","blog":"http://donatstudios.com","location":"Minnesota","email":"donatj@gmail.com"},"repository":{"id":749771,"name":"SimpleCalendar","url":"https://github.com/donatj/SimpleCalendar","description":"A simple PHP calendar rendering class","homepage":"http://donatstudios.com","watchers":13,"stargazers":13,"forks":8,"fork":false,"size":189,"owner":"donatj","private":false,"open_issues":1,"has_issues":true,"has_downloads":true,"has_wiki":true,"language":"PHP","created_at":"2010-06-30T12:38:08-07:00","pushed_at":"2013-09-20T09:03:40-07:00","master_branch":"master"}}'
    try:
        event = json.loads(line)
    except:
        logging.warn("Failed on line")
        # continue
        pass

    try:
        mutations = []

        # Add data to 'column families' created_at
        temp = event['created_at'][:-6] # remove 5 character at the end of string
        created_at = datetime.datetime.strptime(temp,
            "%Y-%m-%dT%H:%M:%S")

        # Add data to 'column families' type
        event_type = event['type']

        # Add data to 'column families' actor
        actor = event['actor_attributes']

        # Add data to 'column families' repository
        repo = event['repository']

        # Define rowkey
        # pdb.set_trace()
        rowkey = "%d%d%d%s" % (created_at.year, created_at.month, created_at.day, actor['login'])

        mutations = [
            Hbase.Mutation(column='created_at:year', value=str(created_at.year)),
            Hbase.Mutation(column='created_at:month', value=str(created_at.month)),
            Hbase.Mutation(column='created_at:day', value=str(created_at.day)),
            Hbase.Mutation(column='type:name', value=str(event_type)),
            Hbase.Mutation(column='actor:login', value=actor['login']),
            Hbase.Mutation(column='actor:name', value=actor['name']),
            Hbase.Mutation(column='repository:id', value=str(repo['id'])),
            Hbase.Mutation(column='repository:url', value=repo['url']),
            Hbase.Mutation(column='repository:watchers', value=str(repo['watchers'])),
            Hbase.Mutation(column='repository:stargazers', value=str(repo['stargazers'])),
            Hbase.Mutation(column='repository:forks', value=str(repo['forks'])),
            Hbase.Mutation(column='repository:language', value=repo['language'])
        ]

        th.reconnect()
        th.client.mutateRow(DEFAULT_TABLE_NAME, 'rowkey', mutations, {})
    except KeyError:
        logging.warn("Key not found: %s" % key)

