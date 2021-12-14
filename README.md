# File-Merge-Tool
## Cloud computing course project ##
### Summary: ###
-Generate merged files and log file \
-Show logs to keep track of the duplicates and update \
-Use Hadoop MapReduce and Amazon EC2 instance & S3 storage \
-Multiprocessing is applied to parallel the algorithm and accelerate it \
### Source Files: ###
Mapper_reducer.py: map each file into list of dictionaries and reduce two files into one file with all duplicates removed. \
Mapreduce_merge.py: read the data, chunk the files, and output to multiple files.
