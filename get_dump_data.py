#!/usr/bin/python3
"""
Gets the dump of data from the given start and end times
"""
import urllib.request
from datetime import datetime, timedelta
import time
import os
import gzip
import codecs
import sys
from subprocess import call

def logging(s):
    with open('output', 'a') as f:
        s =  '%s: %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(s))
        f.write(str(s) + '\n')
        print(str(s))

def download_timestamp(download_dir, stats_time):
    """
    Downloads the current timestamps data into the given directory
    """
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
            logging(url)
            logging(e)
    else:
        logging("could not get stats for timestamp")

def create_new_data(file_path, timestamp):
    """
    Parses the dump data and creates the correct format
    """
    pages_viewed = 0
    total_views = 0
    output_file_name = file_path[:-3]
    logging('Starting to create new data')
    with gzip.open(file_path, 'rb') as input_file, open(output_file_name, 'w') as output_file:
        for line in input_file:
            line = line.decode('latin-1')
            if line.startswith('en '):
                split = line.split(' ')
                output_file.write(split[1] + '\t' + timestamp.strftime('%Y-%m-%d %H:%M:%S') +
                        '\t' + split[2] + '\n')
                total_views += int(split[2])
                pages_viewed += 1
    logging('Finished parsing English data')
    logging('pages viewed: %d' % pages_viewed)
    logging('total views: %d' % total_views)
    return output_file_name

def get_time_range(start_timestamp, end_timestamp):
    """
    Uploads the data from the given timerange to the dump folder
    """
    download_dir = '/tmp'
    hadoop_dump = '/user/jd/dump2'
    start_timestamp = start_timestamp.replace(minute=0, second=0, microsecond=0)
    end_timestamp = end_timestamp.replace(minute=0, second=0, microsecond=0)

    while start_timestamp < end_timestamp:
        file_path = download_timestamp(download_dir, start_timestamp)
        if file_path:
            output_file_name = create_new_data(file_path, start_timestamp)
            call(['hadoop', 'fs', '-moveFromLocal', output_file_name, hadoop_dump])
            os.remove(file_path)
        else:
            logging('Failed to get data for %s' % start_timestamp)
        start_timestamp += timedelta(hours=1)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Given timestamps as Month/Day/Year Hour")
        print("%s [start timestamp] [end timestam]" % sys.argv[0])
        sys.exit(1)
    start_timestamp = datetime.strptime(sys.argv[1], "%m/%d/%Y-%H")
    end_timestamp = datetime.strptime(sys.argv[2], "%m/%d/%Y-%H")
    logging('start: %s\tend: %s' % (start_timestamp, end_timestamp))

    get_time_range(start_timestamp, end_timestamp)
