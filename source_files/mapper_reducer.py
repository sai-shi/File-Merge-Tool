import os
import json
from functools import reduce

# Set data path and output file path globally
DATA_PATH = os.getcwd() + '/For Merge Disqus/'
OUT_PATH = os.getcwd() + '/merged_files/'
logs_file = open("logs.txt", "a")


def mapper(fname):
    comment_time = fname.split('.')[0].split('_')[1]
    article_id = fname.split('_')[0]
    comment_nodes = []
    with open(DATA_PATH + fname, 'r') as infile:
        data = json.loads("[" + infile.read().replace("}\n{", "},\n{") + "]")
    for seg in data:
        for r in seg['response']:
            comment_nodes.append(r)
    pair = {article_id: (comment_time, comment_nodes, {})}
    return pair


def reducer(d1, d2):
    intersect = set(d1.keys()) & set(d2.keys())
    union = set(d1.keys()) | set(d2.keys())
    output = {}

    for article in union:
        if article in intersect:
            t1 = d1[article][0]
            t2 = d2[article][0]

            if t1 < t2:
                old, new = d1[article][1], d2[article][1]
                update_freq = d2[article][2]
            else:
                old, new = d2[article][1], d1[article][1]
                update_freq = d1[article][2]

            old_ids = [c['id'] for c in old]
            new_ids = [c['id'] for c in new]
            common_ids = set(old_ids) & set(new_ids)
            union_ids = set(old_ids) | set(new_ids)
            all_comments = []

            for comment_id in union_ids:
                if comment_id in common_ids:
                    c1 = list(filter(lambda c: c['id'] == comment_id, old))[0]
                    c2 = list(filter(lambda c: c['id'] == comment_id, new))[0]
                    # print(n1, n2)
                    merged_comment, update_freq = merge_two_comments(c1, c2, article, update_freq)
                    all_comments.append(merged_comment)
                else:
                    if comment_id in new_ids:
                        c = list(filter(lambda c: c['id'] == comment_id, new))[0]
                        all_comments.append(c)
                    else:
                        print('Deletion happens! Article ID: %s, Comment ID: %s' % (article, comment_id))
            output[article] = (max(t1, t2), all_comments, update_freq)

        else:
            if article in d1:
                output[article] = d1[article]
            else:
                output[article] = d2[article]

    return output


def merge_two_comments(old, new, article, update_freq):

    if old != new:
        diff_items = [k for k in old if old[k] != new[k]]
        if len(diff_items) != 0:
            logs_file.write('Duplicates found: article_id=%s, comment_id=%s. \n' % (article, old['id']))
            for item in diff_items:
                if item not in update_freq:
                    update_freq[item] = 1
                else:
                    update_freq[item] += 1
                logs_file.write('Attribute value: \'%s\' are updated. (%s --> %s) .\n' % (item, old[item], new[item]))
            logs_file.write('-----------------------------------------------------------\n')

    return new, update_freq


def chunk_mapper(chunk):
    mapped = map(mapper, chunk)
    reduced = reduce(reducer, mapped)
    return reduced