import sys
import urllib2
import json
import datetime
import pdb

START_END_FILE = 'start_end.json'
ERROR_FILE = 'error_files.txt'
URL_PREFIX = 'http://data.githubarchive.org/'
URL_SUFFIX = '.json.gz'
STORAGE_FOLDER = './archives/'

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class InputError(Error):
    """Raised when an input filename is not in the correct format.

    Attributes:
        expr -- input expression in which the error occurred
        msg  -- explanation of the error
    """

    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg

def get_start_and_end_point(filename):
    data = json.loads(open(filename, 'r').read())
    try:
        start_time_str = data['start']
        end_time_str = data['end']
    except KeyError, e:
        print ('Error in the format of %s file: %s. THE PROGRAM WILL EXIT'%
            (filename, e))
        sys.exit(1)

    return start_time_str, end_time_str

def time_to_github_filename(timestamp):
    date_str = timestamp.strftime('%Y-%m-%d')
    hour_str = timestamp.hour
    return '%s-%s' % (date_str, hour_str)

def github_filename_to_time(filename):
    data = filename.split('-')
    if len(data) != 4:
        raise InputError(filename, 'not the correct format github filename')

def one_hour_ago(timestamp):
    return timestamp - datetime.timedelta(minutes=60)

def download_one_file(basename):
    url = URL_PREFIX + basename + URL_SUFFIX
    try:
        data = urllib2.urlopen(url).read()
        # TODO: use os.path
        filepath = STORAGE_FOLDER + basename + URL_SUFFIX
        with open(filepath, 'wb') as output:
            output.write(data)
    except urllib2.HTTPError:
        write_file_not_downloaded(basename)
        pass

def write_file_not_downloaded(basename):
    with open(ERROR_FILE, 'a') as output:
        output.write('%s\n' % basename)

def download_backward_files(time_str, times):
    """Download files before this point of time.

    Attributes:
        time_str -- start downloading file from this point backward
        times -- number of previous files to download
    """
    if time_str == '':
        timestamp = datetime.datetime.utcnow()
    else:
        timestamp = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')

    for i in range(0, times):
        timestamp = one_hour_ago(timestamp)
        print timestamp.__str__()
        basename = time_to_github_filename(timestamp)
        download_one_file(basename)

    return timestamp

def update_starting_ending_point(start_timestamp, end_timestamp):
    with open(START_END_FILE, 'w') as outfile:
        json.dump({
            'start': start_timestamp.__str__(),
            'end': end_timestamp.__str__()
            }, outfile, indent=4)

if __name__ == "__main__":
    start_time_str, end_time_str = get_start_and_end_point(START_END_FILE)
    start_timestamp = download_backward_files(start_time_str, 2000)
    end_timestamp = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S.%f')
    update_starting_ending_point(start_timestamp, end_timestamp)

