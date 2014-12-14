"""
This parses out the wikipedia source xml and generates an adjacency list for all
articles that is capablable of being processed by Apache Spark
"""

import datetime
import time
import sys

def readFile(input_file, output_file):
    output = open(output_file, 'w')
    f = open(input_file)  # the xml file to read from
    start = False
    pageTitle = ''  # article's title
    links = []
    count = 0

    line = f.readline()
    while not line == '':
        if '<text' in line or start:
            start = True
            while '[[' in line:
                link = line[line.find('[[') + 2:line.find(']]')]
                if '|' in link:  # removes the second part of the link
                    link = link[:link.find('|')]
                if '#' in link:  # this removes the href second part of a link
                    link = link[:link.find('#')]
                if not ':' in link:  # if it has a ':', it is a file or something
                    if not link == '':
                        link = link[0].upper() + link[1:]  # uppercases the first letter
                    links.append(link)
                line = line[line.find(']]') + 2:]
        if '<title>' in line:
            pageTitle = line[11:-9]
        if '</text>' in line:
            count += 1
            output.write(pageTitle + "\t" + "\t".join(links) + "\n")
            start = False
            links = []
            if count % 1000000 == 0:
                print(format(count, ",d") + " done.")
        line = f.readline()
    f.close()
    output.close()
    print('element count: ' + format(count, ",d"))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Need to specify the input and output files\n python %s [input] [output]" % sys.argv[0])
        sys.exit(1)
    start = time.clock()
    readFile(sys.argv[1], sys.argv[2])
    end = time.clock()
    print('time diff: ' + str(datetime.timedelta(seconds=(end - start))))
