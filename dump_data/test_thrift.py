import datetime
import json
import logging
import sys
import pdb

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

DEFAULT_TABLE_NAME = 'github_events'
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
            Hbase.ColumnDescriptor(name='event')
        ])

    # parse JSON
    # line = '{"created_at":"2013-09-20T10:00:06-07:00","payload":{"pages":[{"page_name":"Home","title":"Home","summary":null,"action":"created","sha":"fbc868e990ef516a0be2a5c87b17228a75eb655f","html_url":"https://github.com/iSamuraii/FFReader/wiki/Home"}]},"public":true,"type":"GollumEvent","url":"https://github.com/iSamuraii/FFReader/wiki/Home","actor":"iSamuraii","actor_attributes":{"login":"iSamuraii","type":"User","gravatar_id":"65fbae24a4f3ba884f52bff624b62985"},"repository":{"id":12917102,"name":"FFReader","url":"https://github.com/iSamuraii/FFReader","description":"An API based reader for Forumfree","watchers":0,"stargazers":0,"forks":0,"fork":false,"size":268,"owner":"iSamuraii","private":false,"open_issues":0,"has_issues":true,"has_downloads":true,"has_wiki":true,"language":"PHP","created_at":"2013-09-18T00:44:20-07:00","pushed_at":"2013-09-20T09:59:14-07:00","master_branch":"master"}}'
    for line in sys.stdin:
    # pdb.set_trace()
        line = line.decode('ascii', 'ignore')
        # encoding = chardet.detect(line)
        # print encoding['encoding']
        # if encoding['encoding'] != 'ascii':
        #     pass
        #     # continue

        try:
            event = json.loads(line)
        except:
            logging.warn("Failed parsing json on line")
            continue
            # pass

        try:
            mutations = []

            # Add data to 'column families' type
            event_type = event['type']

            if event_type not in ['CommitCommentEvent',
                'IssueCommentEvent',
                'IssuesEvent',
                'PublicEvent',
                'PushEvent',
                'PullRequestEvent',
                'PullRequestReviewCommentEvent',
                'ForkApplyEvent']:
                continue

            # Add data to 'column families' created_at
            print event[-6:]
            temp = event['created_at'][:-6] # remove 5 character at the end of string
            created_at = datetime.datetime.strptime(temp,
                "%Y-%m-%dT%H:%M:%S")

            # Add data to 'column families' actor
            actor = event['actor_attributes']

            # Add data to 'column families' repository
            repo = event['repository']

            # Define rowkey
            # pdb.set_trace()
            rowkey = "%d%d%d_%s" % (created_at.year, created_at.month, created_at.day, actor['login'])

            mutations = [
                Hbase.Mutation(column='created_at:year', value=str(created_at.year)),
                Hbase.Mutation(column='created_at:month', value=str(created_at.month)),
                Hbase.Mutation(column='created_at:day', value=str(created_at.day)),
                Hbase.Mutation(column='created_at:weekday', value=str(created_at.weekday())),
                Hbase.Mutation(column='created_at:hour', value=str(created_at.hour)),
                Hbase.Mutation(column='created_at:timestamp', value=temp),
                Hbase.Mutation(column='event:type', value=str(event_type)),
                Hbase.Mutation(column='actor:login', value=actor['login']),
                Hbase.Mutation(column='actor:name', value=actor.get('name', '')),
                Hbase.Mutation(column='repository:id', value=str(repo['id'])),
                Hbase.Mutation(column='repository:name', value=str(repo.get('name',''))),
                Hbase.Mutation(column='repository:url', value=repo['url']),
                Hbase.Mutation(column='repository:watchers', value=str(repo['watchers'])),
                Hbase.Mutation(column='repository:stargazers', value=str(repo['stargazers'])),
                Hbase.Mutation(column='repository:forks', value=str(repo['forks'])),
                Hbase.Mutation(column='repository:language', value=repo.get('language', ''))
            ]

            th.reconnect()
            th.client.mutateRow(DEFAULT_TABLE_NAME, rowkey, mutations, {})
        except KeyError as e:
            # pdb.set_trace()
            logging.warn("Key not found: %s for event type: %s" % (e, event.get('type', '')))

