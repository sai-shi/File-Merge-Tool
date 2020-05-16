"""
Cloud Computing Project 2: MapReduce Merging tool
Sai Shi
tul67232
04/25/2020
"""
import zipfile
from multiprocessing import Pool
import matplotlib.pyplot as plt
from mapper_reducer import *
import urllib.request
import ssl
import time
import json
from collections import Counter


from boto3.session import Session
import boto3

session = Session(aws_access_key_id='##############',
                  aws_secret_access_key='#################')
s3 = session.resource('s3')
bucket = s3.Bucket('mapreduce-filemerge')

for s3_file in bucket.objects.all():
    print(s3_file.key) # prints the contents of bucket

s3 = boto3.client('s3')

bucket.download_file('data.zip', 'data.zip')


# Set data path and output file path globally
DATA_PATH = os.getcwd() + '/For Merge Disqus/'
OUT_PATH = os.getcwd() + '/merged_files/'
logs_file = open("logs.txt", "a")

# Create output dir
if 'merged_files' not in os.listdir(os.getcwd()):
    os.mkdir(os.getcwd() + '/merged_files/')

# Choose number of parallel CPUs
pool = Pool(8)

# Unzip data files
try:
    if 'For Merge Disqus' not in os.listdir(os.getcwd()):
        # print('Beginning file download with urllib2...')
        # urllib.request.urlretrieve(url, 'data.zip')
        for file in os.listdir(os.getcwd()):
            if file.endswith('.zip'):
                print('Zip file found. Unzipping data files....')
                with zipfile.ZipFile(file, 'r') as zip_ref:
                    zip_ref.extractall()
                    print('Unzip done! Data file created.')
                break
    else:
        print('Data file found.')
except IOError:
    print('Error. No zip file exists.')


# Chunk all files
def divide_file_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def write_to_outdir(result):
    freq = Counter()
    for article in result:
        freq.update(result[article][2])
        with open(OUT_PATH + article + '.txt', 'w') as outfile:
            json.dump(result[article][1], outfile)
            outfile.close()
        try:
            s3.upload_file(OUT_PATH + article + '.txt', 'mapreduce-filemerge', 'merged_files/')
            # print("Upload Successful")
        except FileNotFoundError:
            print("The file was not found")

    return freq


def plot_histogram(freq):
    plt.figure(figsize=(10, 5))
    plt.barh(list(freq.keys()), freq.values(), 0.5, color='blue')
    plt.title('Frequency of value change per attribute')
    plt.xlabel('Frequency')
    for i, v in enumerate(list(freq.values())):
        plt.text(v + 1, i + .01, str(v), color='red', fontweight='bold')
    plt.tight_layout()
    plt.savefig('freq_histgram.jpg', dpi=300, format='jpg')
    plt.show()


def main():
    file_chunks = list(divide_file_chunks(os.listdir(DATA_PATH), 32))
    start = time.time()
    # step 1:
    mapped = pool.map(chunk_mapper, file_chunks)
    # step 2:
    reduced = reduce(reducer, mapped)

    end = time.time()
    print('Program running time: %.2f s' % (end-start))

    freq = write_to_outdir(reduced)

    print(dict(freq))

    plot_histogram(freq)

    logs_file.close()


if __name__ == '__main__':
    main()
