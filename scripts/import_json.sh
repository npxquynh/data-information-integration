cd ~/git/githubarchive.org/crawler/data/

# Move the first 10 files to different folder
ls | head -10 | while read file; do mv $file "./processing/$file"; done

# Extract

# Calling python
cd /home/quynh/code/data-information-integration/dump_data
python test_thrift.py < /media/quynh/DATA/Courses/DII/github\ data/processing/2013*.json