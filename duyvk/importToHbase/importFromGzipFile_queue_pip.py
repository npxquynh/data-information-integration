import datetime
import json
import logging
import sys
import time
import pdb

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

import argparse
import multiprocessing
import os
import Queue
import tempfile
import threading
import io
import glob
import gzip

DEFAULT_TABLE_NAME = 'test'
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

th = ThriftHbase()
def main():
    print "abc"
    limit , concurrency, directory = handle_commandline()
    print concurrency
    directory = os.path.abspath(str(directory))
    print directory

    jobs = Queue.Queue()
    results = Queue.Queue()

    create_thread(limit, jobs, results, concurrency)
    todo = add_jobs(directory, jobs)

    process (todo, jobs, results, concurrency)


def create_thread(limit, jobs, results, concurrency):
    for _ in range(concurrency):
        thread = threading.Thread(target=workers, args=(limit, jobs, results))
        thread.daemon = True
        thread.start()

def workers(limit, jobs , results):
    while True:
        line = jobs.get()
        result = transform(line)
        results.put(result)
        jobs.task_done()


def transform(line):
    result = {}
    line = line.decode('ascii', 'ignore')
    try:
        event = json.loads(line)
    except:
        logging.warn("Failed parsing json on line")
        return result

    try:
        mutations = []

        # Add data to 'column families' type
        event_type = event['type']

        event_message = ""

        if event_type == "PushEvent":
            payload = event['payload']
            shas = payload['shas']
            if shas is not None and len(shas)> 0:
                event_message = shas[0][2]

        if event_type not in ['CommitCommentEvent',
            'IssueCommentEvent',
            'IssuesEvent',
            'PublicEvent',
            'PushEvent',
            'PullRequestEvent',
            'PullRequestReviewCommentEvent',
            'ForkApplyEvent']:
            return result

        # Add data to 'column families' created_at
        # print event[-6:]
        temp = event['created_at'][:-6] # remove 5 character at the end of string
        created_at = datetime.datetime.strptime(temp,
            "%Y-%m-%dT%H:%M:%S")

        # Add data to 'column families' actor
        actor = event['actor_attributes']

        # Add data to 'column families' repository
        repo = event['repository']



        # Define rowkey
        rowkey = "%s_%s" % (event['created_at'], actor['login'])
        
        mutations = [
            Hbase.Mutation(column='created_at:year', value=str(created_at.year)),
            Hbase.Mutation(column='created_at:month', value=str(created_at.month)),
            Hbase.Mutation(column='created_at:day', value=str(created_at.day)),
            Hbase.Mutation(column='created_at:weekday', value=str(created_at.weekday())),
            Hbase.Mutation(column='created_at:hour', value=str(created_at.hour)),
            Hbase.Mutation(column='created_at:timestamp', value=created_at.strftime("%Y-%m-%d %H:%M:%S")),
            Hbase.Mutation(column='event:type', value=str(event_type)),
            Hbase.Mutation(column='event:message', value=str(event_message)),
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
        result['rowkey'] = rowkey
        result['mutations'] = mutations
    except KeyError as e:
            # pdb.set_trace()
            logging.warn("Key not found: %s for event type: %s" % (e, event.get('type', '')))
    
    return result

def add_jobs(directory, jobs):
    filelist = glob.glob(directory+'/*.gz')
    for part_file in filelist:
        with gzip.open(part_file, 'rb') as f:
            for line in f:
                jobs.put(line)
    return 1


def process( todo, jobs, results, concurrency):
    canceled = False

    try:
        jobs.join()
    except KeyboardInterrupt:
        print "Cancel..."
        canceled = True
    if canceled:
        done = results.qsize()
        print "done: " + done
    else:
        insertToHbase(results)

def insertToHbase(results):
    while not results.empty():
        result = results.get_nowait()
        if len(result)!=0:
            th.reconnect()
            th.client.mutateRow(DEFAULT_TABLE_NAME, result['rowkey'], result['mutations'], {})

def handle_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--limit", type=int, default=0,
        help="the maximum items per feed [default: unlimited]")
    parser.add_argument("-c", "--concurrency", type=int,
        default=multiprocessing.cpu_count()*4,
        help="specify the concurrency (for debuging and timing"
            ") [default: %(default)d]")
    parser.add_argument("-d", "--directory")
    args = parser.parse_args()
    return args.limit, args.concurrency, args.directory
    
if __name__ == '__main__':
    start_time = time.time()
    main()
    print time.time() - start_time, "seconds"