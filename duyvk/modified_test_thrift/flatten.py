import os
import sys
import logging
import json

from optparse import OptionParser
import pdb

def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = {}
    if type(structure) != dict:
        flattened[((path + "_") if path else "") + key] = structure
    else:
        for new_key, value in structure.items():
            if (path and key):
                tmp_path = path + "_" + key
            elif (not path and key):
                tmp_path = key
            else:
                tmp_path = ""
            flatten(value, new_key, tmp_path, flattened)

    return flattened

def write_to_file(file_path, file_content):
    """
    file_content = [
        [gene_name, frequency_1],
        [gene_name_2, frequency_2]
    ]
    """
    with open(file_path, 'a') as output_file:
        json.dump(file_content, output_file)
        output_file.write('\n')

if __name__ == '__main__':
    # Handling command line arguments
    args = sys.argv[1:]
    if len(args) != 1:
        sys.exit("Expect 1 argument")

    file_name = args[0]

    original_file_path = "/media/quynh/DATA/Linux/DII_Github_Data/original"
    modified_file_path = "/media/quynh/DATA/Linux/DII_Github_Data/flattened"

    with open(os.path.join(original_file_path, file_name)) as f:
        for line in f:
            event = {}
            # pdb.set_trace()
            # Parse the JSON of this event
            # line = '{"created_at":"2013-09-20T09:03:41-07:00","payload":{"shas":[["7af40c10725e4fe8a5b593ef887bc5757f3e7495","inodetelic@gmail.com","Added year check for today date","inod",true],["dea687bf8213acf25ccc05c5122166e02ac5022d","donatj@gmail.com","Merge pull request #2 from inod/patch-1Added year check for today date","Jesse Donat",true]],"size":2,"ref":"refs/heads/master","head":"dea687bf8213acf25ccc05c5122166e02ac5022d"},"public":true,"type":"PushEvent","url":"https://github.com/donatj/SimpleCalendar/compare/bcdd0b6799...dea687bf82","actor":"donatj","actor_attributes":{"login":"donatj","type":"User","gravatar_id":"50cb4981bbaa51b1713269a29f78d826","name":"Jesse Donat","company":"Donat Studios","blog":"http://donatstudios.com","location":"Minnesota","email":"donatj@gmail.com"},"repository":{"id":749771,"name":"SimpleCalendar","url":"https://github.com/donatj/SimpleCalendar","description":"A simple PHP calendar rendering class","homepage":"http://donatstudios.com","watchers":13,"stargazers":13,"forks":8,"fork":false,"size":189,"owner":"donatj","private":false,"open_issues":1,"has_issues":true,"has_downloads":true,"has_wiki":true,"language":"PHP","created_at":"2010-06-30T12:38:08-07:00","pushed_at":"2013-09-20T09:03:40-07:00","master_branch":"master"}}'
            try:
                event = json.loads(line)
            except:
                logging.warn("Failed on line")
                continue

            event = flatten(event)
            write_to_file(os.path.join(modified_file_path, file_name), event)
