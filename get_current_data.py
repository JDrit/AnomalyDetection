#!/usr/bin/python3
"""
Downloads the current's hour data
"""
import urllib.request
from datetime import datetime, timedelta
import time
import os
import gzip
import codecs
from subprocess import call

def logging(s):
    with open('/home/jd/output', 'a') as f:
        s =  '%s: %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(s))
        f.write(str(s) + '\n')
        print(str(s))

def download_timestamp(download_dir, stats_time):
    timestamp = (stats_time - datetime.utcfromtimestamp(0)).total_seconds()
    logging("downloading for %s" % stats_time)
    for second in range(0, 30):
        try:
            time.sleep(1) # deals with rate limiting for the download server
            url = ("http://dumps.wikimedia.org/other/pagecounts-raw/%d/%d-%02d/pagecounts-%d%02d%02d-%02d00%02d.gz" %
                (stats_time.year, stats_time.year, stats_time.month, stats_time.year, stats_time.month,
                    stats_time.day, stats_time.hour, second))
            path = "%s/%d.gz" % (download_dir, timestamp)
            urllib.request.urlretrieve(url, path)
            logging('File finished downloading')
            return path
        except Exception as e:
            logging("trying a timeoffset of %02d seconds" % (second + 1))
            logging(e)
    else:
        logging("could not get stats for timestamp")

def create_new_data(file_path):
    pages_viewed = 0
    total_views = 0
    output_file_name = file_path[:-3]
    logging('Starting to create new data')
    with gzip.open(file_path, 'rb') as input_file, open(output_file_name, 'w') as output_file:
        for line in input_file:
            line = line.decode('latin-1')
            if line.startswith('en '):
                split = line.split(' ')
                if len(split[1]) < 128:
                    output_file.write(split[1] + '\t' + timestamp.strftime('%Y-%m-%d %H:%M:%S') + '\t' + split[2] + '\n')
                    total_views += int(split[2])
                    pages_viewed += 1
    logging('Finished parsing English data')
    logging('pages viewed: %d' % pages_viewed)
    logging('total views: %d' % total_views)
    return output_file_name

if __name__ == '__main__':
    download_dir    = '/tmp'
    hadoop_new_data = '/user/jd/new_data/'
    hadoop_trending = '/user/jd/trending_data/'
    hadoop_raw_data = '/user/jd/raw_data/'

    timestamp = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    logging('Start download for %s' % timestamp)
    file_path = download_timestamp(download_dir, timestamp)
    if file_path:
        output_file_name = create_new_data(file_path)
        call(['hadoop', 'fs', '-rm', '-r', hadoop_new_data])
        call(['hadoop', 'fs', '-mkdir', hadoop_new_data])
        call(['hadoop', 'fs', '-copyFromLocal', output_file_name, hadoop_new_data])
        call(['hadoop', 'fs', '-mkdir', hadoop_trending])
        call(['hadoop', 'fs', '-copyFromLocal', output_file_name, hadoop_trending])
        old_timestamp = (datetime.now() - timedelta(days=30)).strftime('%s')
        logging('removing old timestamp: %s' % old_timestamp)
        call(['hadoop', 'fs', '-rm', hadoop_trending + old_timestamp])
        call(['hadoop', 'fs', '-mkdir', hadoop_raw_data])
        call(['hadoop', 'fs', '-moveFromLocal', output_file_name, hadoop_raw_data])
    else:
        logging('Error downloading data')
    logging('finished processing for %s' % timestamp)
    os.remove(file_path)
    logging('--------------------------------------------')

