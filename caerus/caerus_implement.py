import pandas as pd
import numpy as np
import os
import sys
import json
from multiprocessing import Manager, Pool
from multiprocessing.pool import ThreadPool
import pywren
import pandas.util.testing as tm
import datetime
from six.moves import cPickle as pickle


from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
from pandas import pipeline_merge
import pandas.util.testing as tm
import sys
import copy
import random
import table_schemas
from table_schemas import *

try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered

from multiprocessing import Process
import time
from hashlib import md5
from io import StringIO
import boto3
from io import BytesIO
from s3fs import S3FileSystem

from jiffy import JiffyClient

import subprocess
#subprocess.call("/home/ubuntu/jiffy/sbin/refresh.sh")
import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

scale = 1000   # total data volume generated in TPCDS
DUP = 1   ##### this is in case there is not enough input data
mod = {}
mod_key0 = 'testruntime_cjr0'
mod_key1 = 'testruntime_cjr1'
mod_key2 = 'testruntime_cjr2'
mod_key3 = 'testruntime_cjr3'
mod_key4 = 'testruntime_cjr4'
mod_key5 = 'testruntime_cjr5'
mod_key6 = 'testruntime_cjr6'
mod_key7 = 'testruntime_cjr7'
mod_key8 = 'testruntime_cjr8'

# mod['key1'] = 'testruntime_cjr1'
# mod['key2'] = 'testruntime_cjr2'
# mod['key3'] = 'testruntime_cjr3'
# mod['key4'] = 'testruntime_cjr4'
# mod['key5'] = 'testruntime_cjr5'
# mod['key6'] = 'testruntime_cjr6'
# mod['key7'] = 'testruntime_cjr7'
# mod['key8'] = 'testruntime_cjr8'
parall_1 = 100    # how many tasks per stage
parall_2 = 100
parall_3 = 100
parall_4 = 100
parall_5 = 1

storage_mode = 's3-redis'  ### only affact the first read and final put

EXTRA_ENV = {'PYTHONHASHSEED': '0'}
INVOKE_POOL_THREADS = 64

# wrenexec = pywren.standalone_executor()
#wrenexec = pywren.default_executor(shard_runtime=True)

wrenexec = pywren.default_executor()

# mod = {}
# mod_key0 = 'testruntime_cjr0'
# mod_key1 = 'testruntime_cjr1'
# mod['key1'] = 'testruntime_cjr1'
# mod['key2'] = 'testruntime_cjr2'
# mod['key3'] = 'testruntime_cjr3'

query_name = "16"

# stage_info_load = {}
# stage_info_filename = "stage_info_load_" + query_name + ".pickle"
# if os.path.exists(stage_info_filename):
#     stage_info_load = pickle.load(open(stage_info_filename, "r"))

# pm = [str(parall_1), str(parall_2), str(parall_3), str(pywren_rate), str(n_nodes)]
# filename = "nomiti.cluster-" +  storage_mode + '-tpcds-q' + query_name + '-scale' + str(scale) + "-" + "-".join(pm) + "-b"  + ".pickle"
# #filename = "simple-test.pickle"

print("Scale is " + str(scale))


######################################################
# functions
######################################################

def get_type(typename):
    if typename == "date":
        return datetime.datetime
    if "decimal" in typename:
        return np.dtype("float")
    if typename == "int" or typename == "long":
        return np.dtype("float")
    if typename == "float":
        return np.dtype(typename)
    if typename == "str":
        return np.dtype(typename)
    raise Exception("Not supported type: " + typename)





def get_name_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    names = [a[0] for a in schema]
    return names

def get_dtypes_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    dtypes = {}
    for a,b in schema:
        dtypes[a] = get_type(b)
    return dtypes

def get_s3_locations(table):
   # print("WARNING: get from S3 locations, might be slow locally.")
    s3 = S3FileSystem()
    ls_path = os.path.join("hong-tpcds-data", "scale" + str(scale), table)
    all_files = s3.ls(ls_path)
    return ["s3://" + f for f in all_files if f.endswith(".csv")]

def get_input_locations(table_name, partition_num, rounds):
###################### rounds * partition_num  should equal to the total chunks
    all_locs = get_s3_locations(table_name)
#     random.shuffle(all_locs)######## shuffle it
    all_locs = all_locs*DUP
    chunks = [all_locs[x:min(x+rounds,len(all_locs))] for x in range(0, len(all_locs), rounds)]
    print(chunks[0:partition_num])
    return chunks[0:partition_num]

def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
#                 print(key)
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield "s3://hong-tpcds-data/" + obj["Key"]

def get_input_locations_all(table_name, rand, partition_num, rounds):
###################### rounds * partition_num  should equal to the total chunks
    if rand == 'yes':
        table_name = 'rand-' + table_name
    flag = 0
    flag_2 = 0

    table_name_filename = "table_names_locations.pickle"

    if os.path.exists(table_name_filename):
        flag_2 = 1
        locations = pickle.load(open(table_name_filename, "rb"))
        if table_name in locations:
            all_locs = locations[table_name]
            flag = 1
    if flag == 0:
        all_locs = list(get_matching_s3_keys("hong-tpcds-data", prefix="scale1000/" + table_name, suffix=".csv"))
        if flag_2 == 0:
            locations = dict()
        else:
            locations[table_name] = all_locs
        pickle.dump(locations, open(table_name_filename, 'wb'))

    chunks = [all_locs[x:min(x+rounds,len(all_locs))] for x in range(0, len(all_locs), rounds)]

    print(chunks[0:partition_num])
    return chunks[0:partition_num]





def read_s3_table(key, roundId, s3_client=None):
    loc = key['loc'][roundId]
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")
    if s3_client == None:
        s3_client = boto3.client("s3")
    data = []
    obj = s3_client.get_object(Bucket='hong-tpcds-data', Key=loc[21:])['Body'].read()
#    data.append(obj)
#     if isinstance(key['loc'], str):
#     #    loc = key['loc']
#         obj = s3_client.get_object(Bucket='hong-tpcds-data', Key=loc[21:])['Body'].read()
# #         print(sys.getsizeof(obj))
# #         print(type(obj))
#         data.append(obj)
#     else:
#         for loc in key['loc']:
#             obj = s3_client.get_object(Bucket='hong-tpcds-data', Key=loc[21:])['Body'].read()
#             data.append(obj)
    logger.debug("here")
    logger.debug(names)
    logger.debug(dtypes)
    logger.debug(range(len(names)-1))
    part_data = pd.read_table(BytesIO(obj),       ####### bytesIO may not work here?
                              delimiter="|",
                              header=None,
                              names=names,
                              usecols=range(len(names)-1),
                              dtype=dtypes,
                              na_values = "-",
                              parse_dates=parse_dates)
    logger.debug("read done")
    #print(part_data.info())
    return part_data

def hash_key_to_index(key, number):
    return int(md5(key).hexdigest()[8:], 16) % number

def my_new_hash_function(tups):
    #print(tups)
    return hash("".join([str(t) for t in tups])) % 65536

# left['new_col'] = list(left.iloc[:,[0,1]].itertuples(index=False,name=None))
# h = left['new_col'].apply(lambda x: my_hash_function3(x))
#print(left['new_col'])

def my_hash_function(row, indices):
    # print indices
    #return int(sha1("".join([str(row[index]) for index in indices])).hexdigest()[8:], 16) % 65536
    #return hashxx("".join([str(row[index]) for index in indices]))% 65536
    #return random.randint(0,65536)
    return hash("".join([str(row[index]) for index in indices])) % 65536

def add_bin(df, indices, bintype, partitions):
    logger.debug("indices are")
    logger.debug(indices)
    t1 = time.time()
    d_index = Series(list(df.iloc[:,indices].itertuples(index=False,name=None)))
    hvalues = d_index.apply(lambda x: my_new_hash_function(x))
#     hvalues = df.apply(lambda x: my_hash_function(tuple(x), indices), axis = 1)
    logger.debug("hash function takes " + str(time.time()-t1))
    if bintype == 'uniform':
        #_, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
        bins = np.linspace(0, 65536, num=(partitions+1), endpoint=True)
    elif bintype == 'sample':
        samples = hvalues.sample(n=min(hvalues.size, max(hvalues.size/8, 65536)))
        _, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
    else:
        raise Exception()
    #print("here is " + str(time.time() - tstart))
    #TODO: FIX this for outputinfo
#     if hvalues.empty:
#         return []
    t1 = time.time()
    logger.debug('here bins are:')
    logger.debug(bins)
    logger.debug(hvalues.values)
    #hvalues = np.random.randint(25, 100, size=hvalues.size)
    df['bin'] = pd.cut(hvalues.values, bins=bins, labels=False, include_lowest=False)
    logger.debug("cut by bin takes " + str(time.time()-t1))
    logger.debug(df['bin'])

    #print("here is " + str(time.time() - tstart))
    return bins

##########stageId is the source stage (to make it unique)
######## partition number is the partion numbuer of the output (receiver)
########这些queue 应该是可以从sender,receiver以及scheduleer都能打开的
############name is the identifier
############### the stageid should be stageid + stepid
def open_or_create_jiffy_queues(em, appName, partitions,stageId,role):    # multi writer
    logger.debug("here????????????")
    if role == "sender":
        data_ques = [0]*partitions
        msg_que = []
        for reduceId in range(partitions):
            data_path = "/" + appName + "/" + str(stageId) +   "/" +  str(reduceId)
      #      data_path = "/" + key['appName'] + "/" +  str(reduceId)
            logger.debug("before open data queue " + str(data_path))
            data_ques[reduceId] = em.open_or_create_queue(data_path,"local://tmp", 1,1)
            logger.debug("data queue opened")
#             print(reduceId)
#             print(data_path)
#             print(data_ques)
        msg_path = "/" + appName + "/"  + "msg" + "/" + str(stageId)
    #      msg_path = "/" + key['appName'] + "-msg" + "/" + str(reduceId)
        logger.debug("before open msg queue " + str(msg_path))
        msg_que = em.open_or_create_queue(msg_path, "local://tmp", 1,1)
        logger.debug("msg queue opened")
        return data_ques, msg_que

    elif role == "receiver":
        data_ques = [0]*partitions
        for reduceId in range(partitions):
            data_path = "/" + appName + "/" + str(stageId) +  "/" +  str(reduceId)
            logger.debug("before open data queue " + str(data_path))
            data_ques[reduceId] = em.open_or_create_queue(data_path,"local://tmp", 1,1)
            logger.debug("data queue opened")
#             print(data_path)
#             print(data_ques)
        return data_ques

    elif role == "scheduler":
        msg_que = []
        msg_path = "/" + appName + "/"  + "msg" + "/" + str(stageId)

#         msg_path = "/" + appName + "-msg" + "/" + str(stageId)
        msg_que = em.open_or_create_queue(msg_path, "local://tmp", 1,1)
        return msg_que,msg_path

##################### update message also
# fin: its the last chunk

def write_s3_intermediate(output_loc, table, s3_client=None):
    csv_buffer = BytesIO()
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns
   # table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)   does not work here
    tmp = table.to_csv(sep="|", header=False, index=False, columns=slt_columns)
    if s3_client == None:
        s3_client = boto3.client('s3')

    bucket_index = int(md5(output_loc).hexdigest()[8:], 16) % n_buckets     ########## does it work work here?
    s3_client.put_object(Bucket="hong-tpcds-" + str(bucket_index),
                         Key=output_loc,
                         Body=csv_buffer.getvalue())
    output_info = {}
    output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]

    return output_info

def write_jiffy_intermediate(table, data_queue, reduceId, fin):       ##### data_queue[reduceId] is output location, fin is an identifier
    csv_buffer = BytesIO()
    logger.debug("flag1 ")
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns
#     print(table)
   ####### table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    t1 = time.time()
#     logger.debug(table)
    tmp = table.to_csv(sep="|", header=False, index=False, columns=slt_columns)

    encoded = tmp.encode('utf-8')
    logger.debug("encode takes " + str(time.time()-t1))
#     print("before putting data to queue" + str(reduceId))
    t2 = time.time()
    data_queue[reduceId].put(encoded)
    logger.debug("put data to queue " + str(data_queue[reduceId]))
    logger.debug("put data takes " + str(time.time()-t2))

#     data = data_queue[reduceId].get()
#     logger.debug("test get data size is" + str(sys.getsizeof(data)))
#     print("put data to queue" + str(reduceId))
#     ########## check if right
   # msg['datasize'][reduceId] = table[0].count
  #  print(table)
  #  print("size is" + str(table[0].count))
   ############ shuffle_size = len(table)    #table[0].count

    shuffle_size = sys.getsizeof(encoded)

    #bucket_index = int(md5(output_loc).hexdigest()[8:], 16) % n_buckets
  #  s3_client.put_object(Bucket="hong-tpcds-" + str(bucket_index),
    if fin == 1:
        a = 'fin'
        data_queue[reduceId].put(a.encode('utf-8'))
    output_info = {}
  #  output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]
#     logger.debug("here")
#     logger.debug(slt_columns)
#     logger.debug(table.dtypes[slt_columns])
    return output_info, shuffle_size

def write_s3_partitions(df, column_names, bintype, partitions, storage):   ########### "storage" is similar to the address of the jiffy queue we opened
    #print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
    #print((bins))
    #print(df)
    s3_client = boto3.client("s3")
    outputs_info = []
    def write_task(bin_index):
        logger.debug("df is now")
        logger.debug(df)

        split = df[df['bin'] == bin_index]
        logger.debug(split)
        if split.size > 0 or split.size < 1:
            # print(split.size)
            split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            outputs_info.append(write_s3_intermediate(output_loc, split, s3_client))
    write_pool = ThreadPool(1)
    write_pool.map(write_task, range(len(bins)))
    write_pool.close()
    write_pool.join()
    t2 = time.time()

    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1-t0), (t2-t1)]
    return results

######## msg is a dict including per round message
############# write to all reducer queues, write to message queue
def write_jiffy_partitions(df, column_names, bintype, partitions, data_ques, msg_que, msg, fin):
    #print(df.columns)

    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
#     logger.debug("indices are now xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:")

#     logger.debug(df)

    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
#     logger.debug(df)
#     logger.debug(bins)
    logger.debug("calculate partition takes " + str(t1-t0))
    # print("t1 - t0 is " + str(t1-t0))
#     print((bins))
#     print(range(len(bins)))
#     print(df)

    outputs_info = []
############ bin_index is reduceId

    def write_task(bin_index):
        tt = time.time()
        split = df[df['bin'] == bin_index]
        logger.debug("calculate split takes" + str(time.time() - tt))
        if split.size > 0 or split.size < 1:
            # print(split.size)
            tx = time.time()

            split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
        #    output_loc = storage + str(bin_index) + ".csv"
       #     print(split)
            logger.debug("the table is:")
#             logger.debug(split)
        #    outputs_info.append(write_jiffy_intermediate(split, data_ques, bin_index, fin, msg))
            out, shuffle_size = write_jiffy_intermediate(split, data_ques, bin_index, fin)
            logger.debug("call write_jiffy_intermediate takes " + str(time.time()-tx))
            outputs_info.append(out)
            msg['data_size'][bin_index] = shuffle_size
    tt2 = time.time()
#     write_pool = ThreadPool(1)
#     write_pool.map(write_task, range(len(bins)-1))   ###########make sure it is right
    logger.debug("bins are:")
    logger.debug(bins)
    for i in range(len(bins)-1):
        write_task(i)

#     if len(bins) == 0:
#         output_info = {}
#       #  output_info['loc'] = output_loc
#         output_info['names'] = df.columns
#         output_info['dtypes'] = df.dtypes
#         msg['data_size'] = len(msg['data_size'] )*[0]
#         outputs_info.append(output_info)
  #  write_pool.map(write_task, range(4))
    logger.debug("before pool close takes" + str(time.time()-tt2))

#     write_pool.close()
#     write_pool.join()
    logger.debug("after pool close takes " + str(time.time()-tt2))

    t2 = time.time()

    results = {}
#     results['outputs_info'] = outputs_info
    results['outputs_info'] = outputs_info[0]
    results['breakdown'] = [(t1-t0), (t2-t1)]
    ############
    #msg['roundId'] =
    msg['round_time'] = time.time() -  msg['round_time']    #msg['round_time'] + (t2-t0)
    logger.debug(msg)
    msg_que.put(json.dumps(msg))
    logger.debug("put message to msg queue takes " + str(time.time()-t2))
    logger.debug("total write takes " + str(time.time()-t0))
    return results,msg


#def create_msg(roundId,total_round,round_time):
def create_msg(total_round,taskId,roundId,stageId,partition_num, t_start):
    msg = {}
    msg['total_round'] = total_round
    msg['roundId'] = roundId
    msg['stageId'] = stageId
    msg['taskId'] = taskId
    msg['round_time'] = t_start
    msg['input_size'] = partition_num*[0]
    msg['data_size'] = partition_num*[0]
 #   msg['fin'] = 0
 #   if roundId == total_round:
 #       msg['fin'] = 1
    return msg


################### read one data piece from the data_que with the reduceId
########### difference with read_s3_intermediate is that data_ques[reduceId] is not in the key
def read_jiffy_intermediate(key, reduceId, data_ques):
    try:
        logger.debug("before get")
        obj = data_ques[reduceId].get()
        logger.debug("after get" + str(sys.getsizeof(obj)))
    #         print(sys.getsizeof(obj))
    #      print("the size of obj is"+ str(sys.getsizeof(obj)))
        ############ change here
        if sys.getsizeof(obj)  == 36:
    #         part_data == 'fin'
    #         print("the fin is " + part_data)
    #             print("here I am fin")
            return('fin')
        else:
    #         elif sys.getsizeof(obj) >1000:
            logger.debug("before read table")
            names = list(key['names'])
            logger.debug(names)
    #         dtypes = key['dtypes']
    #         logger.debug(dyptes)
            parse_dates = []
    #         for d in dtypes:
    #             if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
    #                 parse_dates.append(d)
    #                 logger.debug("here")
    #                 dtypes[d] = np.dtype("str")
    #             part_data = pd.read_table(BytesIO(obj),
    #                                       delimiter="|",
    #                                       header=None,
    #                                       names=names,
    #                                       dtype=dtypes,
    #                                       parse_dates=parse_dates)
            part_data = pd.read_table(BytesIO(obj),header = None, delimiter="|",names=names)
#             logger.debug("what is the table")
#             logger.debug(part_data)

        ############### the above read table line works, but we now try the more complicated read table command above
          #  b = pd.read_table(BytesIO(a),header = None, delimiter="|",  names = ['key','value','value2'])
    #             print(part_data)
    except:                      ################## no data is in the queue right now
        part_data = 'empty'
############### here just want to return sth with a certain size differnt from 'fin'
    return part_data
#### 3 possible return: fin, emptyxxxx, data


###### a wraper which count fin number and support read in batch

######### see if adding dtypes will break it
####### fin_num is the currently seen fins  fin_size is the total fins supposed to see
##### batch size is how much data we get in one round (one call)
def read_jiffy_splits(names, dtypes, reduceId, data_ques, fin_num, batch_size, fin_size):
    dtypes_dict = {}
    count = 0
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]
    lim = 0   ############################### it is just to prevent dead events, should be adjusted if more data
  #  ds = pd. DataFrame()
    ds = []
    key = {}
    key['names'] = names
    key['dtypes'] = dtypes_dict
    logger.debug("read jiffy splits here")
    logger.debug(key)
#     key['dtypes'] = dtypes_dict
    while count < batch_size and lim < 6000:   ######################### this should be infinity, the time to wait for input
        d = read_jiffy_intermediate(key, reduceId, data_ques)
        logger.debug("read success")
#         print(type(d))
        logger.debug(type(d))
        lim += 1
       # print("this is d: " + d)
      #  if sys.getsizeof(d) <70:
        if  isinstance(d,str):
            if d == 'fin':
                logger.debug("1")
                fin_num += 1
                logger.debug("now what is fin number?")
                logger.debug(fin_num)
                if fin_num == fin_size:
                    break
#                     continue
    #                 break
           # elif sys.getsizeof(d) > 1000:
        elif isinstance(d, pd.DataFrame):      #d != 'empty':
            logger.debug("2")
            ds.append(d)
            logger.debug("ds")
#             print("ds is:")
#             print(ds)

            count += 1
        else:
            time.sleep(0.001)

        logger.debug("here")
    if len(ds) > 0:
        return pd.concat(ds), fin_num
    else:
        return ds,fin_num


###################### execute a stage ,tasks are keylists

def execute_lambda_stage(stage_function, tasks):
    t0 = time.time()
    #futures = wrenexec.map(stage_function, tasks, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
    #pywren.wait(futures, 1, 64, 1)
    for task in tasks:
        task['write_output'] = True


    futures = wrenexec.map(stage_function, tasks, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     futures = wrenexec.map_sync_with_rate_and_retries(stage_function, tasks, straggler=False, WAIT_DUR_SEC=5, rate=pywren_rate)


    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    t1 = time.time()
    res = {'results' : results,
           't0' : t0,
           't1' : t1,
           'run_statuses' : run_statuses,
           'invoke_statuses' : invoke_statuses}
    return res


###################### update the status of a stage by reading message queue
def update_stage_status(msg_que,acc_size,round_per_map,time_per_map,total_round_per_map):

    try:
        msg = msg_que.get()
        msg = json.loads(msg)
#         print("now pringing the message!!!!!!!!!!!!!!!!!")
#         print(msg)
#         acc_size[msg['taskId']] = acc_size + msg['data_size']
        acc_size[msg['taskId']] = acc_size[msg['taskId']] + msg['data_size']
#         acc_input_size[msg['taskId']] = acc_input_size[msg['taskId']] + msg['input_size']
        round_per_map[msg['taskId']] = msg['roundId']
        time_per_map[msg['taskId']] = msg['round_time']
        total_round_per_map[msg['taskId']] = msg['total_round']
    #    res = [acc_size,round_per_map,time_per_map,total_round_per_map]
    #    print(res)

        print(acc_size)
        print(round_per_map)
        print(time_per_map)
        return acc_size,round_per_map,time_per_map,total_round_per_map
    except:

        return acc_size,round_per_map,time_per_map,total_round_per_map
#     %,acc_input_size

#     msg['roundId'] = roundId
#     msg['taskId'] = taskId
#     msg['round_time'] = round_time
#     msg['data_size'] = partition_num*[0]

################# you need some function at the scheduler to close data and msg queue when a stage finishes
def close_stage_ques(em, stageId, appName):
    for reduceId in range(num_partitions):
        data_path = "/" + appName + "/" + str(stageId) +  str(reduceId)
  #      data_path = "/" + key['appName'] + "/" +  str(reduceId)
        em.close(data_path)
    msg_path = "/" + appName + "-msg" + "/" + str(stageId)
    em.close(msg_path)

##############################################
# stage launchers
##############################################
def map_stage_launcher1(func_name, key, table_name, shared_var):
  #  pro = Pool(processes = key['partition_num'])

    partition_num = key['partition_num'][key['stageId']-1]
    names = get_name_for_table(table_name)
    dtypes = get_dtypes_for_table(table_name)
    rounds = key['rounds']
    chunks = get_input_locations_all(table_name, 'yes', partition_num, rounds)
    keylist = []
    for i in range(partition_num):
        key_now = copy.copy(key)
        key_now['loc'] = chunks[i]
        key_now['taskId'] = i
        key_now['names'] = names
        key_now['dtypes'] = dtypes
        keylist.append(key_now)
    return keylist

def map_stage_launcher2(func_name,keylist,key, shared_var):
    print("tasks" +  " in  stage " + str(key['stageId']) + "has launched !!!!!!!!!!!!!!!!!!!")
#         print(keylist)
    globals()[func_name](keylist, shared_var)
 #   toymap(stageId, keylist,shared_var)

### keeps updating this
###########  a simplificiation: each stage only have one output? so only one queue.
def stage_launcher_new(key, shared_var, func_name, scheme, start_time):
    t_ini = time.time()

    stageId = key['stageId']
    partition_num = key['partition_num'][stageId-1]
    process_pool = Pool(processes = partition_num)
    launched_ones = []     # a dict
    unlaunched_ones = list(range(partition_num))

    em = JiffyClient(host=key['em'])


    def task_launcher(key, shared_var, func_name, taskId):
       # pro = Pool(processes = key['partition_num'])
#         partition_num = key['partition_num']
        rounds = key['rounds']
   #     wren = pywren.standalone_executor()

        key_now = copy.copy(key)
        key_now['taskId'] = taskId
    #    key_now['loc'] = chunk[i]
     #   key_now['loc'] = []
        process_pool.apply_async(func_name, (key_now, shared_var,))
   #     func_name(key_now, shared_var)
        print('task ' + str(i) + " in  stage: " + str(key_now['stageId']) + ' has been launched!!!!!!!!!!!!!!!!!!!!')
        launched_ones.append({'task':taskId,
                              'time':time.time()})
        unlaunched_ones.remove(taskId)
        return time.time()



    for j in range(step_num[stageId-1]):   ###### for each step, open queue and initiate values
        j = j+1
        print(j)
        p_step = p[str(stageId)+str(j)]   ##### p_step should be stage+step
        if 1: # p_step != -1:    # has a parent step
            partition_numx = key['partition_num'][int(p[(str(stageId) + str(j))]/10)-1]   # this is the partition number of the parent of the step

            globals()['msg_que'+str(j)], globals()['msg_path'+str(j)] = open_or_create_jiffy_queues(em, appName, partition_num, p_step, 'scheduler')
            globals()['acc_size'+str(j)] =  np.zeros((partition_numx,partition_num))   #### first partition is the mapper partitions, second is the reducer partitions
            globals()['acc_input_size'+str(j)] =  np.zeros((partition_num))  ####### similar to round
            globals()['round_per_map'+str(j)] = np.zeros(partition_numx)   #######
            globals()['time_per_map'+str(j)]  = np.zeros(partition_numx)
            globals()['total_round_per_map'+str(j)]  = 1000* np.ones(partition_numx)
            globals()['start_time'+str(j)]  = np.ones(partition_num)
            globals()['end_time'+str(j)]  = np.ones(partition_num)
#     min_portion = np.zeros(partition_num)   # the minimal portion among all tasks
    portion = np.zeros(partition_num)
    chunks_stage1 = 20
    chunks_stage2 = 5
    while (len(unlaunched_ones)) > 0:   ###### lets first forget about dynamic update
#     while min(portion) > 0.95:
        t_total = np.zeros(partition_num)  ##### the estimalted final finsh time for each task
        d_total = np.zeros(partition_num)  ##### the estimalted final run time for each task
        for j in range(step_num[stageId-1]): ##### update values and calculate stuff
            j = j+1
            p_step = p[str(stageId)+str(j)]
            if p_step != -1:    # has a parent ste
                globals()['acc_size'+str(j)], globals()['round_per_map'+str(j)], globals()['time_per_map'+str(j)], globals()['total_round_per_map'+str(j)] = update_stage_status(globals()['msg_que'+str(j)], globals()['acc_size'+str(j)], globals()['round_per_map'+str(j)], globals()['time_per_map'+str(j)], globals()['total_round_per_map'+str(j)])
#                 print(globals()['total_round_per_map'+str(j)])




                ###############################
                # how you predict rc and rp
                # update portion
                # its parents finish time
                # if started, based on portion
                # if not, based on its prediction
                # if parent has started
                    # its process size boased on portion and data recieved
                    # its process size based on fin
                # its total runtime = size/rate

                ###############################
                # now we have two sources: the message queue and the global variables
                # the message queue has the current progress of each task : how much data it has processed/generated, et al.
                #
                # the global variables has the estimated total size, the calculated fin and start time for each step
                # and how to select initial value?
                ############# portion of each map execution:

                ### portion can be calculated based on both round and size
                ### todo: lets just use round for simplicity for now !
#                 portion = (globals()['round_per_map'+str(j)] + 1)/ globals()['total_round_per_map'+str(j)]
#                 #             if parent stage = map: or groupby 后边
#         #                 portion = (locals()[round_per_map+str(j)] + 1)/ locals()[total_round_per_map+str(j)]
#         #             else:
#         #                 portion  = locals()[acc_input_size+str(j)]/ total_task_size[str(p_step)]
#             ## should be the min  or if it is a map stage                locals()[acc_size+str(j)]/ total_task_size
#                 ####todo: what if part of the parent has started?
#                 if sum(sum (globals()['acc_size'+str(j)] )) > 0:  ### if the parent has started

#                     ####### update finish time estimation for parent stage step
#                    # t_fin = max(time_per_map/np.maximum(round_per_map,1) * total_round_per_map)
#                     globals()['t_fin'+str(j)] = max(globals()['time_per_map'+str(j)]/np.maximum(portion,0.1))    ######### use it as final time

#                     ####### calculate the total size it will receive and update
#                     size =  sum((globals()['acc_size'+str(j)].T/np.maximum(portion,0.1)).T)  #### ### 现在两个行列时反的，check哪个是对的
#                     total_task_size[str(stageId)+str(j)] = size

#                 else:
#                     globals()['t_fin'+str(j)] = max(cal_fin_time[str(p_step)])   ### parenets finish time
#                     size = total_task_size[str(stageId)+str(j)]            #### its size

#                     ####### calcualte time
# #                 print(size)
# #                 print(total_cosume_rate)
#                 globals()['d'+str(j)] = size/ total_cosume_rate[str(stageId)+str(j)]  ####### or can also be fixed, t_fin is more dynamic   duration

#                     ### iteratively calculate actual fin
#             else:  ########## it is a fixed sized step
# #                 locals()['d'+str(j)] = total_time[str(stageId)+str(j)]
#                 globals()['d'+str(j)] = 0

#                 globals()['t_fin'+str(j)] = 0

        ############################################
            #update the consume time of its parent


        ######### update the task schedule
        ### todo: more striclty, the finish time of the current stage should be max(t_fin, t_parent_start + d)
#             t_total = max(t_total + globals()['d'+str(j)],   globals()['t_fin'+str(j)])
#             d_total = d_total +  globals()['d'+str(j)]
#             launch_t = t_total - d_total
#             t_reverse = t_total


#         for j in range(step_num):  ######### update the schedule made for the current step
#             k = step_num - j
#             cal_fin_time[str(stageId)+str(j)] = t_reverse
#             ######################## I removed the following sentense just for debug
# #             t_reverse = t_reverse - locals()['d'+str(k)]
#             cal_start_time[str(stageId)+str(j)] = t_reverse
        ########################### this is not the best place to determine how many rounds this stage should have for each step
#         key['rounds'] =  max(locals()['total_round_per_map1'])
#         key['rounds2'] =  max(locals()['total_round_per_map2'])

        if scheme == 'lazy':   ############### lazy and eager needs refactoring
            ok = 1
            for j in range(step_num[stageId-1]):
                j = j+1

                print("the total round per step " + str(j) + " in stage " + str(stageId) + " is " + str(globals()['total_round_per_map'+str(j)]))
                print("the current round per step " + str(j) + " in stage " + str(stageId) + " is " + str(min(globals()['round_per_map'+str(j)] +1)))

                if stageId == 3 and j == 1:
                    if (max(globals()['total_round_per_map'+str(j)]) / chunks_stage1 > min(globals()['round_per_map'+str(j)] +1)) and (p[str(stageId)+str(j)] > 0):
                        ok = 0
                elif stageId == 3 and j == 2:
                    if (max(globals()['total_round_per_map'+str(j)]) / chunks_stage2 > min(globals()['round_per_map'+str(j)] +1)) and (p[str(stageId)+str(j)] > 0):
                        ok = 0
                else:
                    if (max(globals()['total_round_per_map'+str(j)]) > min(globals()['round_per_map'+str(j)] +1)) and (p[str(stageId)+str(j)] > 0):
                        ok = 0
            if ok == 1:
                for i in range(partition_num):
                    task_launcher(key, shared_var, func_name, i)
        if scheme == 'eager':
            for i in range(partition_num):
                task_launcher(key, shared_var, func_name, i)
        ######### (1)an action threshhold P_thresh, (2)unlaunched, (3)the algorithm
        if scheme == 'reactive':
            for i in unlaunched_ones:

#                 if min(round_per_map/total_round_per_map)> P_thresh and t_fin - time_per_map[i]  < t_left + t_right[i] + STARTUP_TIME:
                if start_time[stageId-1][i] < time.time() - t_ini:
                    task_launcher(key, shared_var, func_name, i)
#                     print("still have " + str(t_fin - time_per_map[i]))
#                     print(t_right[i])
#                     print(t_left)
#                     print(min(round_per_map/total_round_per_map))

#                     print(time.time())

        time.sleep(0.005)
    process_pool.close()
    process_pool.join()

#########################
### how to write a stagte
###################
## map statge
#########################
######################################################################
def stage1(key,share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)

        logger.debug(key)
        t_s = time.time()
        partition_num = key['partition_num']

        rounds = key['rounds']
        #em = key['em']
        taskId = key['taskId']
        appName = key['appName']
        stageId = key['stageId']
        locations = key['loc']
        switch = key['cached']
#         partition_num = 1
#         rounds = 8
#         #em = key['em']
#         taskId = 1
#         appName = 'test-1'
        em = JiffyClient(host=key['em'])
        logger.debug("pandas version is:" + str(pd.__version__))
        logger.info("map of stage 1 " + str(taskId) + " starts")
        logger.debug("berfore queue")
        data_ques, msg_que = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(1)), 'sender')
        logger.debug("queue opend")
        logger.debug("the msg queue is: " + str(msg_que))


   #################################################### step 1:
        t_step1 = []   ############ time per round
        s_step1 = []  ############# size per round per destination
        chunks_stage1 = 20
        for k in range(int(rounds/ chunks_stage1)):
            logger.debug("round " + str(k) + " starts")
            t_in = time.time()

    ###########################     create message
            msg = create_msg(rounds, taskId, k, stageId, partition_num[c[str(stageId)]-1], t_s)
    ##############################     read table
            cs_merged = []
            for i in range(chunks_stage1):
                cs = read_s3_table(key, k * chunks_stage1 + i, s3_client=None)
                logger.debug("read s3 complete, takes " + str(time.time()-t_in))
    #########  process
                wanted_columns = ['cs_order_number',
                              'cs_ext_ship_cost',
                              'cs_net_profit',
                              'cs_ship_date_sk',
                              'cs_ship_addr_sk',
                              'cs_call_center_sk',
                              'cs_warehouse_sk']
                cs_s = cs[wanted_columns]
                cs_merged.append(cs_s)

            cs_s = pd.concat(cs_merged)

            logger.debug("what is cs_s here")
            logger.debug(cs_s)
  ######################## write table
            x = 0
            if k == int(rounds/chunks_stage1) - 1:
                x = 1
        #    logger.debug("the right table size is " + str(sys.getsizeof(right)))
            t1 = time.time()

#             cr = pd.read_csv(StringIO(""), names=cr.columns, dtype=dict(zip(cr.columns,cr.dtypes)))
#             logger.debug("the info of data is:")
#             logger.debug(cr.info())
   ############################         test purpose
            res = write_jiffy_partitions(cs_s, ['cs_order_number'], 'uniform', partition_num[c[str(stageId)]-1], data_ques, msg_que = msg_que, msg = msg, fin = x)

            logger.debug("write table takes " + str(time.time()-t1))
            logger.debug(res[0]['outputs_info'])

  #########################
#             data_size = 1024 * 1024 * 100
#             data_ques[0].put(b"a" * data_size)
#             logger.debug("Successful put!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#             ret = data_ques[0].get()


            ############# this is just test
#             fin_num = 0
#             d, fin_num = read_jiffy_splits(res[0]['outputs_info']['names'], res[0]['outputs_info']['dtypes'], taskId, data_ques, fin_num, batch_size = partition_num, fin_size = partition_num)
#             logger.debug("check read results")
#             logger.debug(d)
################################# update step 1 info
            t_step1.append(time.time()-t_in)
            s_step1.append(msg['data_size'])

        t_f = time.time()
#        share.append([t_s,t_f])
#         logger.debug(res[0]['outputs_info'])
        return [t_s,t_f], res[0]['outputs_info'], taskId, t_step1, s_step1
    keylist = []
    keylist.append(key)
#    wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key1, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key1, from_shared_storage=True, sync_to_shared_storage=False)
 #   futures = wrenexec.map(run_command, keylist[0], extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
    futures = wrenexec.map(run_command, keylist[0], module_dependencies_key=mod_key1, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#    futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)   this is for async launch
    pywren.wait(futures)
    results = [f.result() for f in futures]
    share.append(results)
#     for i in range(len(results)):
#         share.append(results[i][0])
    data_info['stage1'] = results[0][1]
    lifespan['stage1'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage1'] = change

def stage2(key,share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(key)
        t_s = time.time()
        partition_num = key['partition_num']
        rounds = key['rounds']
        #em = key['em']
        taskId = key['taskId']
        appName = key['appName']
        stageId = key['stageId']
        locations = key['loc']
        switch = key['cached']
#         partition_num = 1
#         rounds = 8
#         #em = key['em']
#         taskId = 1
#         appName = 'test-1'
        em = JiffyClient(host=key['em'])
        logger.info("map of stage 2 " + str(taskId) + " starts")
        logger.debug("berfore queue")
        data_ques, msg_que = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(1)), 'sender')



        logger.debug("queue opend")
#         logger.debug("the msg queue is: " + str(msg_que))

#         logger.debug("the msg queue1 is: " + str(msg_que2))
#         logger.debug("the msg queu3 is: " + str(msg_que3))
           #################################################### step 1:
        t_step1 = []   ############ time per round
        s_step1 = []  ############# size per round per destination

        chunks_stage2 = 5
        for k in range(int(rounds / chunks_stage2)):
            logger.debug("round " + str(k) + " starts")
            t_in = time.time()

    ###########################     create message
            msg = create_msg(rounds, taskId, k, stageId, partition_num[c[str(stageId)]-1], t_s)
    ##############################     read table
            cr_merged = []
            for i in range(chunks_stage2):
                cr = read_s3_table(key, k * chunks_stage2 + i, s3_client=None)
                logger.debug("read s3 complete, takes " + str(time.time()-t_in))
                cr_merged.append(cr)
    #########  process
            cr = pd.concat(cr_merged)

  ######################## write table
            x = 0
            if k == int(rounds / chunks_stage2) - 1:
                x = 1
        #    logger.debug("the right table size is " + str(sys.getsizeof(right)))
            t1 = time.time()
            res = write_jiffy_partitions(cr, ['cr_order_number'], 'uniform', partition_num[c[str(stageId)]-1], data_ques, msg_que = msg_que, msg = msg, fin = x)
            logger.debug("write table takes " + str(time.time()-t1))

################################# update step 1 info
            t_step1.append(time.time()-t_in)
            s_step1.append(msg['data_size'])


#             msg_que2, num = open_or_create_jiffy_queues(em, appName, partition_num, int(str(1) + str(1)), 'scheduler')
#             rec_msg = msg_que2.get()
#             rec_msg = json.loads(rec_msg)
#             logger.debug("now pringing the message!!!!!!!!!!!!!!!!!")
#             logger.debug(rec_msg)

        t_f = time.time()
#        share.append([t_s,t_f])
#         logger.debug(res[0]['outputs_info'])
        return [t_s,t_f], res[0]['outputs_info'], taskId, t_step1, s_step1

    keylist = []
    keylist.append(key)
#    wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=True, sync_to_shared_storage=False)
 #   futures = wrenexec.map(run_command, keylist[0], extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
    futures = wrenexec.map(run_command, keylist[0], module_dependencies_key=mod_key2, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#    futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)   this is for async launch
    pywren.wait(futures)
    results = [f.result() for f in futures]
#     share.append(results[0][0])
    share.append(results)
    data_info['stage2'] = results[0][1]
    lifespan['stage2'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage2'] = change

def stage4(key,share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)

        logger.debug(key)
        t_s = time.time()
        partition_num = key['partition_num']
        rounds = key['rounds']
        #em = key['em']
        taskId = key['taskId']
        appName = key['appName']
        stageId = key['stageId']
        locations = key['loc']
        switch = key['cached']
#         partition_num = 1
#         rounds = 8
#         #em = key['em']
#         taskId = 1
#         appName = 'test-1'
        em = JiffyClient(host=key['em'])
        logger.info("map of stage 4" + str(taskId) + " starts")
        logger.debug("berfore queue")
        data_ques, msg_que = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(1)), 'sender')
        logger.debug("queue opend")


   #################################################### step 1:
        t_step1 = []   ############ time per round
        s_step1 = []  ############# size per round per destination
        for i in range(rounds):
            t_in = time.time()
            logger.debug("round " + str(i) + " starts")
    ###########################     create message
            msg = create_msg(rounds, taskId, i, stageId, partition_num[c[str(stageId)]-1], t_s)
    ##############################     read table

            cs = read_s3_table(key, i, s3_client=None)
            cs = cs[cs.ca_state == 'GA'][['ca_address_sk']]

            logger.debug("read s3 complete, takes " + str(time.time()-t_in))
    #########  process

  ######################## write table
            x = 0
            if i == rounds - 1:
                x = 1
        #    logger.debug("the right table size is " + str(sys.getsizeof(right)))
            t1 = time.time()
            res = write_jiffy_partitions(cs, ['ca_address_sk'], 'uniform', partition_num[c[str(stageId)]-1], data_ques, msg_que = msg_que, msg = msg, fin = x)
            logger.debug("write table takes " + str(time.time()-t1))

################################# update step 1 info
            t_step1.append(time.time()-t_in)
            s_step1.append(msg['data_size'])

        t_f = time.time()
#        share.append([t_s,t_f])
#         logger.debug(res[0]['outputs_info'])
        return [t_s,t_f], res[0]['outputs_info'], taskId, t_step1, s_step1

    keylist = []
    keylist.append(key)
#    wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key4, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key4, from_shared_storage=True, sync_to_shared_storage=False)
 #   futures = wrenexec.map(run_command, keylist[0], extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
    futures = wrenexec.map(run_command, keylist[0], module_dependencies_key=mod_key4, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#    futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)   this is for async launch
    pywren.wait(futures)
    results = [f.result() for f in futures]
#     share.append(results[0][0])
    share.append(results)
    data_info['stage4'] = results[0][1]
    lifespan['stage4'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage4'] = change



##################################### join stages
def stage3(key, share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)
        logger.debug("here!!!!!!!!")
        logger.debug(key)
        stageId = key['stageId']
        partition_num = key['partition_num']
        rounds = int(key['rounds'])     ###### the left table round
        rounds2 = int(key['rounds2'])    ###### the right table round
        rounds3 = int(key['rounds3'])   ############# the broadcast rounds
        em = JiffyClient(host=key['em'])
        reduceId = key['taskId']
        appName = key['appName']
#         alg_type = key['type']

        switch = key['cached']
        names1 = key['names1']
        names2 = key['names2']
        dtypes1 = key['dtypes1']
        dtypes2 = key['dtypes2']
        mod_key = 'key' + str(stageId)
        logger.info("join " + str(reduceId) + " at stage " + str(stageId) + " starts")
        ############# this is for the first step
        data_ques1 = open_or_create_jiffy_queues(em, appName, partition_num[stageId-1], p[(str(stageId) + str(1))], 'receiver')
        ############# this is for the second step
        data_ques2 = open_or_create_jiffy_queues(em, appName, partition_num[stageId-1], p[(str(stageId) + str(2))], 'receiver')
        data_ques3, msg_que3 = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(3)), 'sender')

        logger.debug("queues opened")

        input_array_1 = []
        input_array_2 = []
        t_start = time.time()

        ########################################## read s3 table
        broad = []
        for i in range(rounds3):
            t_in = time.time()
            logger.debug("round " + str(i) + " starts")
            dd_tmp = read_s3_table(key, i, s3_client=None)
            logger.debug("read s3 complete, takes " + str(time.time()-t_in))
            broad.append(dd_tmp)

        broad = pd.concat(broad)
#         logger.debug(broad)
        dd = broad[['d_date', 'd_date_sk']]

        logger.debug(dd)
        logger.debug(pd.to_datetime(dd['d_date']))
        dd_select = dd[(pd.to_datetime(dd['d_date']) > pd.to_datetime('2002-02-01')) & (pd.to_datetime(dd['d_date']) < pd.to_datetime('2002-04-01'))]
        logger.debug("here3")

        dd_filtered = dd_select[['d_date_sk']]
        logger.debug("receiving broadcast data complete!")
        t_read_table = time.time()
#         t_read_broad = time.time()

        ############################################### breaker here, read right table
        lim = 0   ############limit on trials
        ############ timings
        fin_num = 0
        a_1 = np.zeros(rounds2+2)
        a_2 = np.zeros(rounds2+2)
        a_3 = np.zeros(rounds2+2)
        a_4 = np.zeros(rounds2+2)
        a_5 = np.zeros(rounds2+2)

#         logger.debug("the mode is: " + alg_type)
#         if alg_type == 'pipelined':

        count = 0
        t_lim = time.time()
        flag = 1
        right_full = []
        cur_round = 0       # this is the round from the receiver's perspective
        logger.debug("before read right table")
        partition_numx = partition_num[int(p[(str(stageId) + str(2))]/10)-1]   # this is the partition number of the parent of the step
        logger.debug(partition_numx)
        while fin_num< partition_numx and time.time()-t_lim < 1200:
            if flag == 1:
#                 logger.debug("after read flags")
#                 logger.debug(a_1)
                a_1[cur_round] = time.time()
            lim += 1
            time.sleep(0.001)
            logger.debug("before get")
            logger.debug(partition_num[stageId-1])
            logger.debug(fin_num)
            ########################## read left table
            ###### partion number may not be the same across stages, here should be the partion of previous stages
            cr, fin_num = read_jiffy_splits(names2, dtypes2, reduceId, data_ques2, fin_num, batch_size = partition_numx , fin_size = partition_numx)

            logger.debug("this is supposed to be the right part of  round" + str(cur_round) + " at stage " + str(stageId) + " of join " + str(reduceId))
            logger.debug("the number of fins received is: " + str(fin_num))
            flag = 0 ####### flag indicates if a new round begins
#                 print(fin_num)
#             if len(d)>0:

            if isinstance(cr,list) == False:
                ####################################  this is the round counts

                input_size = cr.memory_usage(index=True).sum()
                input_array_1.append(input_size)

                a_2[cur_round] = time.time() - a_1[cur_round]
                flag = 1    ############# a  new round
                logger.debug("fetch data takes" + str(a_2-a_1))

                ################################ update join table


                right_full.append(cr)

                                    ### how to cal left stuff????????? based on comming d --- do I need left?
                a_3[cur_round] = time.time() - a_1[cur_round]
                logger.debug("I am here")
                cur_round += 1            ############ it is round 0, but .... I know it will proceed , round 0 starts to grow

              #      msg2 = create_msg(rounds, reduceId, cur_round, partition_num)
        right_full = pd.concat(right_full)

        logger.debug("read right table complete!")

        t_read_right = time.time()


        ############################################### breaker here, read left table
        lim = 0   ############limit on trials
        ############ timings
        fin_num = 0
        a_1 = np.zeros(rounds+2)
        a_2 = np.zeros(rounds+2)
        a_3 = np.zeros(rounds+2)
        a_4 = np.zeros(rounds+2)
        a_5 = np.zeros(rounds+2)

#         logger.debug("the mode is: " + alg_type)
#         if alg_type == 'pipelined':
        leftsorter = None
        leftcount = None
        fac = None
        intfac = None
        keys = None
        count = 0
        t_lim = time.time()
        flag = 1
        left_full = []
        left_2_full = []    # cause it has a self join
        cur_round = 0       # this is the round from the receiver's perspective
        logger.debug("here2")
        partition_numx = partition_num[int(p[(str(stageId) + str(1))]/10)-1]   # this is the partition number of the parent of the step

        while fin_num< partition_numx and time.time()-t_lim < 1200:
            if flag == 1:
                logger.debug("after read flags")
                logger.debug(a_1)
                a_1[cur_round] = time.time()
            lim += 1
            time.sleep(0.001)
            logger.debug("before get")
            logger.debug(partition_num[stageId-1])
            logger.debug(fin_num)
            ########################## read left table
            ###### partion number may not be the same across stages, here should be the partion of previous stages
            cs_tmp, fin_num = read_jiffy_splits(names1, dtypes1, reduceId, data_ques1, fin_num, batch_size = partition_numx , fin_size = partition_numx)
            logger.debug("read is all-right")

            logger.debug("this is supposed to be the first part of  round" + str(cur_round) + " at stage " + str(stageId) + " of reducer " + str(reduceId))
            logger.debug("the number of fins received is: " + str(fin_num))
            flag = 0 ####### flag indicates if a new round begins
#                 print(fin_num)
#             if len(d)>0:

            if isinstance(cs_tmp,list) == False:
                input_size = cs_tmp.memory_usage(index=True).sum()
                input_array_2.append(input_size)
                ####################################  this is the round counts
                cs_succient_tmp = cs_tmp[['cs_order_number', 'cs_warehouse_sk']]


                a_2[cur_round] = time.time() - a_1[cur_round]
                flag = 1    ############# a  new round
                logger.debug("fetch data takes" + str(a_2-a_1))

                ################################ update join table

                ##########################################add groupby here


               # result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, ds, factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=10, how="pipeline")

#                 keys, fac, intfac = pd.build_hash_table(d, factorizer=fac, intfactorizer=intfac, previous_keys = keys, left_on='d_date_sk')
                keys, fac, intfac = pd.build_hash_table(cs_tmp['cs_order_number'], factorizer=fac, intfactorizer=intfac, previous_keys = keys)

                left_full.append(cs_tmp)
                left_2_full.append(cs_succient_tmp)
                                    ### how to cal left stuff????????? based on comming d --- do I need left?
                logger.debug("hash takes " + str(time.time() - a_2))

                a_3[cur_round] = time.time() - a_1[cur_round]
                logger.debug("I am here")
                cur_round += 1            ############ it is round 0, but .... I know it will proceed , round 0 starts to grow

              #      msg2 = create_msg(rounds, reduceId, cur_round, partition_num)
        logger.debug("first(second) part complete!")
        left_full = pd.concat(left_full)
        left_2_full = pd.concat(left_2_full)

        t_read_left = time.time()
#################################################################### breaker here, process (selfjoin) left table
#     wh_uc = cs_succient.groupby(['ws_order_number']).agg({'ws_warehouse_sk':'nunique'})
#     target_order_numbers = wh_uc.loc[wh_uc['ws_warehouse_sk'] > 1].index.values
        logger.debug("here")
        wh_uc = left_2_full.groupby(['cs_order_number']).agg({'cs_warehouse_sk':'nunique'})
        logger.debug("here1")

        target_order_numbers = wh_uc.loc[wh_uc['cs_warehouse_sk'] > 1].index.values
        logger.debug(target_order_numbers)

##########################################add join here, isin or join
#         cs_sj_f1 = cs.loc[cs['ws_order_number'].isin(target_order_numbers)]
        unique_id_df = pd.DataFrame(target_order_numbers)

#         unique_id_df = target_order_numbers.unique()
        logger.debug("here3")

        unique_id_df.columns = ['A']
        logger.debug("here4")

#         cs_sj_f1 = left_full.merge(unique_id_df,
#                                    left_on='ws_order_number',
#                                    right_on = 'A',
#                                    how='inner')

#         logger.debug(cs_sj_f1)
        ################# can I reuse the hash table?

        cs_sj_f1, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(left_full, unique_id_df, factorizer=fac, intfactorizer=intfac, leftsorter=leftsorter, leftcount=leftcount, slices=2, how="pipeline", left_factorized_keys=keys,left_on='cs_order_number', right_on='A')
        logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~self join done~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        t_self_join = time.time()
#         logger.debug(cs_sj_f1)



        del left_full
        del left_2_full


#############################################################   breaker here, join left and right table
#         cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(cr.wr_order_number)]


#         unique_id_df = pd.DataFrame(right_full['wr_order_number'].unique())
#         unique_id_df.columns = ['A']

#         ss = time.time()
#         cs_sj_f2 = cs_sj_f1.merge(unique_id_df,
#                                    left_on='ws_order_number',
#                                    right_on = 'A',
#                                    how='inner')
#         ################# can I reuse the hash table AGAIN? maybe not good
#         logger.debug(cs_sj_f2)
#         s_e1 = time.time()
#         logger.debug(str(s_e1-ss))

#         ss = time.time()
#         cs_sj_f2, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(cs_sj_f1, unique_id_df, factorizer=fac, intfactorizer=intfac, leftsorter=None, leftcount=None, slices=2, how="pipeline", left_factorized_keys=None,left_on='ws_order_number', right_on='A')
# #         t_hash_join = time.time()
# #         logger.debug(cs_sj_f2)
#         logger.debug(cs_sj_f2)
#         s_e1 = time.time()
#         logger.debug(str(s_e1-ss))


        ss = time.time()

        cs_sj_f2 = cs_sj_f1.loc[~cs_sj_f1['cs_order_number'].isin(right_full.cr_order_number)]
        logger.debug(cs_sj_f2)
        s_e1 = time.time()
        logger.debug(str(s_e1-ss))



#         ss = time.time()

#         cs_sj_f2, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(cs_sj_f1, unique_id_df, factorizer=fac, intfactorizer=intfac, leftsorter=leftsorter, leftcount=leftcount, slices=2, how="pipeline", left_factorized_keys=keys,left_on='ws_order_number', right_on='A')
#         logger.debug(cs_sj_f2)
#         s_e1 = time.time()
#         logger.debug(str(s_e1-ss))

        t_hash_join = time.time()

        logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~hash join done~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        del cs_sj_f1




################################################################ breaker here, do broadcast join
        ss = time.time()
        merged = cs_sj_f2.merge(dd_filtered, left_on='cs_ship_date_sk', right_on='d_date_sk')
        s_e1 = time.time()
        logger.debug(merged)
        logger.debug(str(s_e1-ss))
        merged.drop('d_date_sk', axis=1, inplace=True)


#         ss = time.time()

#         merged, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(cs_sj_f2, dd_filtered, factorizer=fac, intfactorizer=intfac, leftsorter=leftsorter, leftcount=leftcount, slices=2, how="pipeline", left_factorized_keys=keys,left_on='ws_ship_date_sk', right_on='d_date_sk')

#         s_e1 = time.time()
#         logger.debug(merged)
#         logger.debug(str(s_e1-ss))


        del fac
        del intfac
        del leftsorter
        del leftcount

        del dd
        del cs_sj_f2
        del dd_select
        del dd_filtered
        logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~broadcast join done~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        t_broad_join = time.time()

################################################################ breaker here, write output

####### divide r into k rounds
######### or not, 确认fin 是对的

        t_s = time.time()

        rounds_out = rounds
        leng = len(merged)


        size_per_round = int(leng/rounds_out)
        divided = [merged.iloc[x:min(x+size_per_round,leng)] for x in range(0, leng, size_per_round)]
        logger.debug("division done")
        t_step2 = []   ############ time per round
        s_step2 = []  ############# size per round per destination
        logger.debug("calculate division takes: " + str(time.time() - t_s))


        for i in range(rounds_out):
            logger.debug("round " + str(i) + " starts")
    ###########################     create message
#             msg = create_msg(rounds2, reduceId, i, stageId, partition_num, t_s)
            msg = create_msg(rounds_out, reduceId, i, int(str(stageId) + str(3)), partition_num[c[str(stageId)]-1], t_s)

    ##############################     read table
            t_in = time.time()
    #########  process

  ######################## write table
            x = 0

            logger.debug("here")
            logger.debug(divided[i])

            if i == int(rounds_out)-1:
                x = 1
        #    logger.debug("the right table size is " + str(sys.getsizeof(right)))
            t1 = time.time()

#                 res = write_partitions(merged, ['ws_ship_addr_sk'], 'uniform', parall_2, storage)

            res = write_jiffy_partitions(divided[i], ['cs_ship_addr_sk'], 'uniform', partition_num[c[str(stageId)]-1], data_ques3, msg_que = msg_que3, msg = msg, fin = x)
            logger.debug("write table takes " + str(time.time()-t1))
            t_step2.append(time.time()-t_in)
            s_step2.append(msg['data_size'])

        t_fin = time.time()
#         share.append([t_start,t_fin, fin_num])
       # return ([t_start, t_fin,  a_2, a_3-a_2, a_4-a_3, a_5-a_4])
#         return ([t_start, t_mid, t_fin])
#         return [t_start, t_mid, t_fin], res[0]['outputs_info']

        s_step1 = []
#         return [t_start,t_mid1,t_mid2,t_fin], res[0]['outputs_info'], reduceId, a_3, s_step1, t_step2, s_step2
        return [t_start,t_read_table,t_read_right,t_read_left, t_self_join,t_hash_join, t_broad_join, t_fin], res[0]['outputs_info'], reduceId, a_3, s_step1, t_step2, s_step2, input_array_1, input_array_2

#[t_start,t_read_table,t_read_right,t_read_left, t_broad_join, t_fin]


    keylist = []
    keylist.append(key)
      #  print(keylist)
 #   wrenexec.parse_module_dependencies(run_command, mod_key, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key3, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key3, from_shared_storage=True, sync_to_shared_storage=False)
    futures = wrenexec.map(run_command, keylist, module_dependencies_key=mod_key3, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     print("map call success!")
    pywren.wait(futures)
    results = [f.result() for f in futures]
#     share.append(results[0][0])
    share.append(results)
    data_info['stage3'] = results[0][1]
    print("the result of stage 3 is:")
    print(results)
    lifespan['stage3'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage3'] = change
#     for key in keylist:
#         results = run_command(key)


  #  share.append(results)


############### I have to know the data types and names for all subsequent tables
#     key['names'] = info["outputs_info"][0]['names']
#     key['dtypes'] = info["outputs_info"][0]['dtypes']

#     key['names2'] = info2["outputs_info"][0]['names']
#     key['dtypes2'] = info2["outputs_info"][0]['dtypes']

################################################## stage5,


#     cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
#     ca = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])
#     cc = read_table(key['web_site'])

#     merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
#     merged.drop('ws_ship_addr_sk', axis=1, inplace=True)

#     cc_p = cc[cc['web_company_name'] == 'pri'][['web_site_sk']]

#     merged2 = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')

#     toshuffle = merged2[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]
#     res = write_partitions(toshuffle, ['ws_order_number'], 'uniform', parall_3, storage)

def stage5(key, share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)
        logger.debug("here!!!!!!!!")
        logger.debug(key)
        partition_num = key['partition_num']
        rounds = int(key['rounds'])
        rounds2 = int(key['rounds2'])
        rounds3 = int(key['rounds3'])

        em = JiffyClient(host=key['em'])
        reduceId = key['taskId']
        appName = key['appName']
#         alg_type = key['type']
        stageId = key['stageId']
        switch = key['cached']
        names1 = key['names1']
        names2 = key['names2']
        dtypes1 = key['dtypes1']
        dtypes2 = key['dtypes2']
        mod_key = 'key' + str(stageId)
        logger.info("join " + str(reduceId) + " at stage " + str(stageId) + " starts")
        ############# this is for the first step
        data_ques1 = open_or_create_jiffy_queues(em, appName, partition_num[stageId-1], p[(str(stageId) + str(1))], 'receiver')
        ############# this is for the second step
        data_ques2 = open_or_create_jiffy_queues(em, appName, partition_num[stageId-1], p[(str(stageId) + str(2))], 'receiver')
        data_ques3, msg_que3 = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(2)), 'sender')

        logger.debug("queues opened")
        input_array_1 = []
        input_array_2 = []

        t_in = time.time()
        t_start = t_in
        ####################################### read broadcast table and build hash
        broad = []
        keys2 = None
        leftsorter2 = None
        leftcount2 = None
        fac2 = None
        intfac2 = None

        for i in range(rounds3):
            logger.debug("round " + str(i) + " starts")
            dd_tmp = read_s3_table(key, i, s3_client=None)

            list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County']
            dd_tmp = dd_tmp[dd_tmp.cc_county.isin(list_addr)][['cc_call_center_sk']]

            keys2, fac2, intfac2 = pd.build_hash_table(dd_tmp['cc_call_center_sk'], factorizer=fac2, intfactorizer=intfac2, previous_keys = keys2)

            logger.debug("read s3 complete, takes " + str(time.time()-t_in))
            broad.append(dd_tmp)
        broad = pd.concat(broad)
        logger.debug("receiving broadcast data complete!")
        t_read_table = time.time()

        ################################################## read left table
        lim = 0   ############limit on trials
        ############ timings
        fin_num = 0
        a_1 = np.zeros(rounds+2)
        a_2 = np.zeros(rounds+2)
        a_3 = np.zeros(rounds+2)
        a_4 = np.zeros(rounds+2)
        a_5 = np.zeros(rounds+2)

#         logger.debug("the mode is: " + alg_type)
#         if alg_type == 'pipelined':
        leftsorter = None
        leftcount = None
        fac = None
        intfac = None
        keys = None
        count = 0
        t_lim = time.time()
        flag = 1
        left_full = []
        cur_round = 0       # this is the round from the receiver's perspective
        logger.debug("here2")
        partition_numx = partition_num[int(p[(str(stageId) + str(1))]/10)-1]   # this is the partition number of the parent of the step

        while fin_num<partition_numx and time.time()-t_lim < 1200:
            if flag == 1:
#                 logger.debug("after read flags")
#                 logger.debug(a_1)
                a_1[cur_round] = time.time()
            lim += 1
            time.sleep(0.001)
            logger.debug("before get")
#             logger.debug(partition_num)
            logger.debug(fin_num)
            ########################## read left table
            ###### partion number may not be the same across stages, here should be the partion of previous stages
           # d, fin_num = read_jiffy_splits(names1, dtypes1, reduceId, data_ques1, fin_num, batch_size = partition_num, fin_size = partition_num)
#             partition_numx = partion_num[int(p[(str(stageId) + str(1))]/10)-1]   # this is the partition number of the parent of the step
            d, fin_num = read_jiffy_splits(names1, dtypes1, reduceId, data_ques1, fin_num, batch_size = partition_numx, fin_size = partition_numx)

            logger.debug("this is supposed to be the first part of  round" + str(cur_round) + " at stage " + str(stageId) + " of reducer " + str(reduceId))
            logger.debug("the number of fins received is: " + str(fin_num))
            flag = 0 ####### flag indicates if a new round begins
#                 print(fin_num)
#             if len(d)>0:
            if isinstance(d,list) == False:
                input_size = d.memory_usage(index=True).sum()
                input_array_1.append(input_size)

                ####################################  this is the round counts


                a_2[cur_round] = time.time() - a_1[cur_round]
                flag = 1    ############# a  new round
                logger.debug("fetch data takes" + str(a_2-a_1))

                ################################ update join table
               # result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, ds, factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=10, how="pipeline")

#                 keys, fac, intfac = pd.build_hash_table(d, factorizer=fac, intfactorizer=intfac, previous_keys = keys, left_on='d_date_sk')
                keys, fac, intfac = pd.build_hash_table(d['ca_address_sk'], factorizer=fac, intfactorizer=intfac, previous_keys = keys)
                left_full.append(d)

                                    ### how to cal left stuff????????? based on comming d --- do I need left?
                logger.debug("hash takes " + str(time.time() - a_2))

                a_3[cur_round] = time.time() - a_1[cur_round]
                logger.debug("I am here")
                cur_round += 1            ############ it is round 0, but .... I know it will proceed , round 0 starts to grow

              #      msg2 = create_msg(rounds, reduceId, cur_round, partition_num)
        logger.debug("first part complete!")

        t_mid = time.time()

########################################## breaker here, join with right table and broadcast join
        ############## keeps fetching
        t_s = time.time()
        s_step2 = []  ############# size per round per destination
        logger.debug("I am here")
        left_full = pd.concat(left_full)



        fin_num = 0
        lim = 0
        b_1 = np.zeros(rounds2+2)
        b_2 = np.zeros(rounds2+2)
        b_3 = np.zeros(rounds2+2)
        b_4 = np.zeros(rounds2+2)
        b_5 = np.zeros(rounds2+2)
        right_full = []
#             leftsorter = None
#             leftcount = None
#             orizer = None
#             intrizer = None
#             count = 0
        partition_numx = partition_num[int(p[(str(stageId) + str(2))]/10)-1]   # this is the partition number of the parent of the step

# if alg_type == 'pipelined':
        t_lim = time.time()
        flag = 1
        cur_round = 0       # this is the round from the receiver's perspective
        while fin_num<partition_numx and time.time()-t_lim < 1200:
            if flag == 1:
                logger.debug("begin of round" + str(cur_round))
                logger.debug(int(key['rounds']))
                logger.debug(int(key['rounds2']))
                logger.debug(b_1)
                b_1[cur_round] = time.time()
                msg3 = create_msg(rounds2, reduceId, cur_round, int(str(stageId) + str(2)), partition_num[c[str(stageId)]-1], t_s)
            lim += 1
            time.sleep(0.001)
            logger.debug("before get")
            logger.debug("data_ques2")
            ########################## read
#             sr, fin_num = read_jiffy_splits(names2, dtypes2, reduceId, data_ques2, fin_num, batch_size = partition_num, fin_size = partition_num)
#             partition_numx = partion_num[int(p[(str(stageId) + str(2))]/10)-1]   # this is the partition number of the parent of the step
            sr, fin_num = read_jiffy_splits(names2, dtypes2, reduceId, data_ques2, fin_num, batch_size = partition_numx, fin_size = partition_numx)
            logger.debug("this is supposed to be the second part of round" + str(cur_round) + " at stage " + str(stageId) + " of reducer " + str(reduceId))
            logger.debug("the number of fins received is: " + str(fin_num))
            flag = 0 ####### flag indicates if a new round begins
#                 print(fin_num)
#             if len(sr)>0:
            if isinstance(sr,list) == False:
                input_size = sr.memory_usage(index=True).sum()
                input_array_2.append(input_size)

                b_2[cur_round] = time.time() - b_1[cur_round]
                cur_round += 1            ############ it is round 0, but .... I know it will proceed , round 0 starts to grow
                flag = 1
                logger.debug("fetch data takes" + str(time.time() -b_1[cur_round-1]))
                ################################ the hash join
#                 logger.debug("join input is:")
#                 logger.debug(left_full)
#                 logger.debug(str(sys.getsizeof(left_full)))
#                 logger.debug(sr)
#                 logger.debug(str(sys.getsizeof(sr)))

                before_hash = time.time()

#     merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
#     merged.drop('ws_ship_addr_sk', axis=1, inplace=True)
#                 logger.debug(sr)

#                 merged = left_full.merge(sr, left_on='ca_address_sk', right_on='ws_ship_addr_sk')
#                 logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#                 logger.debug(merged)

                merged, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(left_full, sr, factorizer=fac, intfactorizer=intfac, leftsorter=leftsorter, leftcount=leftcount, slices=10, how="pipeline", left_factorized_keys=keys,left_on='ca_address_sk', right_on='cs_ship_addr_sk')
                logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

                logger.debug(merged)

                merged.drop('cs_ship_addr_sk', axis=1, inplace=True)
              #  d.merge(sr, left_on='ctr_customer_sk', right_on='c_customer_sk')
#                     merged, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, sr, left_on='d_date_sk', right_on='sr_returned_date_sk', factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=10, how="pipeline")
                logger.debug("join before drop takes " + str(time.time() - before_hash))
#                 merged.drop('d_date_sk', axis=1, inplace=True)
                logger.debug("join takes " + str(time.time() - before_hash))
                logger.debug("join results is:")
                logger.debug(merged)
                logger.debug(str(sys.getsizeof(merged)))

                ######################################## the broadcast join

                #     merged2 = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')

#     toshuffle = merged2[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]
#     res = write_partitions(toshuffle, ['ws_order_number'], 'uniform', parall_3, storage)

#                 merged2 = broad.merge(merged, left_on='web_site_sk', right_on='ws_web_site_sk')
#                 logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#                 logger.debug(merged2)

#                 merged2 = merged.merge(broad, left_on='ws_web_site_sk', right_on='web_site_sk')
#                 logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#                 logger.debug(merged2)

                merged2, fac2, intfac2, leftsorter2, leftcount2 = pd.pipeline_merge(broad, merged, factorizer=fac2, intfactorizer=intfac2, leftsorter=leftsorter2, leftcount=leftcount2, slices=10, how="pipeline", left_factorized_keys=keys2,left_on='cc_call_center_sk', right_on='cs_call_center_sk')
                logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                logger.debug(merged2)



                logger.debug("broadcast join done")

                toshuffle = merged2[['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit']]
                b_3[cur_round-1] = time.time() - b_1[cur_round-1]
          #      msg2 = create_msg(rounds, reduceId, cur_round, partition_num)
############################ this is the process before write

                x = 0
             #   if cur_round == rounds-1:
                if cur_round == rounds2:
                    x = 1
                    logger.debug("ok, this stage finishes and write fin")
            #    logger.debug("the right table size is " + str(sys.getsizeof(right)))
                b_4[cur_round-1] = time.time()-b_1[cur_round-1]
                t_xx = time.time()
############################ this is write
                logger.debug("before write")

                res = write_jiffy_partitions(toshuffle, ['cs_order_number'], 'uniform', partition_num[c[str(stageId)]-1], data_ques3, msg_que = msg_que3, msg = msg3, fin = x)
                logger.debug("write table takes " + str(time.time()-t_xx))
                s_step2.append(msg3['data_size'])
                b_5[cur_round-1] = time.time()- b_1[cur_round-1]
                logger.debug("end of round" + str(cur_round-1))

        ########################### profiled time
        logger.debug("the time is:")
        logger.debug([a_2, a_3-a_2,])
        logger.debug([b_2, b_3-b_2, b_4-b_3, b_5-b_4])

        t_fin = time.time()
#         share.append([t_start,t_fin, fin_num])
       # return ([t_start, t_fin,  a_2, a_3-a_2, a_4-a_3, a_5-a_4])
#         return ([t_start, t_mid, t_fin])
#         return [t_start, t_mid, t_fin], res[0]['outputs_info']
        s_step1 = []

        return [t_start,t_read_table,t_mid,t_fin], res[0]['outputs_info'], reduceId, a_3, s_step1, b_5, s_step2, input_array_1, input_array_2 #, left_full, right_full
#

    keylist = []
    keylist.append(key)
      #  print(keylist)
 #   wrenexec.parse_module_dependencies(run_command, mod_key, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key5, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key5, from_shared_storage=True, sync_to_shared_storage=False)
    futures = wrenexec.map(run_command, keylist, module_dependencies_key=mod_key5, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     print("map call success!")
    pywren.wait(futures)
    results = [f.result() for f in futures]
#     share.append(results[0][0])
    share.append(results)
    data_info['stage5'] = results[0][1]
    lifespan['stage5'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage5'] = change
#     for key in keylist:
#         results = run_command(key)


############################################# groupby stage
#     cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])

#     a1 = pd.unique(cs['ws_order_number']).size
#     a2 = cs['ws_ext_ship_cost'].sum()
#     a3 = cs['ws_net_profit'].sum()


def stage6(key, share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)
        logger.debug("here!!!!!!!!")
        partition_num = key['partition_num']
        rounds = int(key['rounds'])
        em = JiffyClient(host=key['em'])
        reduceId = key['taskId']
        appName = key['appName']
#         alg_type = key['type']
        stageId = key['stageId']
        switch = key['cached']
        names = key['names']
        dtypes = key['dtypes']
        mod_key = 'key' + str(stageId)
        logger.info("groupby " + str(reduceId) + " at stage " + str(stageId) + " starts")
        ############# this is for the first step
        data_ques1 = open_or_create_jiffy_queues(em, appName, partition_num[stageId-1], p[(str(stageId) + str(1))], 'receiver')
        ############# this is for the second step
#         data_ques2, msg_que2 = open_or_create_jiffy_queues(em, appName, partition_num[c[str(stageId)]-1], int(str(stageId) + str(2)), 'sender')

        logger.debug("queues opened")

        ######### read left table
        t_start = time.time()
        lim = 0   ############limit on trials
        ############ timings
        fin_num = 0
        a_1 = np.zeros(rounds+2)
        a_2 = np.zeros(rounds+2)
        a_3 = np.zeros(rounds+2)
        a_4 = np.zeros(rounds+2)
        a_5 = np.zeros(rounds+2)
#         logger.debug("the mode is: " + alg_type)
        aggregated = []
        a1 = 0
        a2 = 0
        a3 = 0
        partition_numx = partition_num[int(p[(str(stageId) + str(1))]/10)-1]   # this is the partition number of the parent of the step
        input_array = []

        if 1:
            count = 0
            t_lim = time.time()
            flag = 1
            cur_round = 0       # this is the round from the receiver's perspective
            while fin_num<partition_numx and time.time()-t_lim < 1200:
                if flag == 1:
                    a_1[cur_round] = time.time()
                lim += 1
                time.sleep(0.001)
                logger.debug("before get")
                ########################## read left table
                ds, fin_num = read_jiffy_splits(names, dtypes, reduceId, data_ques1, fin_num, batch_size = partition_numx, fin_size = partition_numx)

                logger.debug("this is supposed to be round" + str(cur_round) + " at stage " + str(stageId) + " of reducer " + str(reduceId))
                logger.debug("the number of fins received is: " + str(fin_num))
                flag = 0 ####### flag indicates if a new round begins
#                 print(fin_num)
#                 if len(ds)>0:
                if isinstance(ds,list) == False:
                    input_size = ds.memory_usage(index=True).sum()
                    input_array.append(input_size)

                    a_2[cur_round] = time.time() - a_1[cur_round]
                    cur_round += 1            ############ it is round 0, but .... I know it will proceed , round 0 starts to grow
                    flag = 1
                    logger.debug("fetch data takes" + str(a_2-a_1))

                    ################################ update groupby results

#                     r = d.groupby(['sr_customer_sk', 'sr_store_sk']).agg({'sr_return_amt':'sum'}).reset_index()
#                     r.rename(columns = {'sr_store_sk':'ctr_store_sk',
#                         'sr_customer_sk':'ctr_customer_sk',
#                         'sr_return_amt':'ctr_total_return'
#                         }, inplace = True)
                    if ds.empty == False:
                        a2 = a2 + ds['cs_ext_ship_cost'].sum()
                        a3 = a3 + ds['cs_net_profit'].sum()
                    aggregated.append(ds)


                    ### how to cal left stuff????????? based on comming d --- do I need left?
                    logger.debug("partial groupby takes " + str(time.time() - a_2))

                    a_3[cur_round-1] = time.time() - a_1[cur_round-1]
              #      msg2 = create_msg(rounds, reduceId, cur_round, partition_num)
        t_mid1 = time.time()
# storage = output_address + "/part_" + str(key['task_id']) + "_"
# res = write_partitions(r, ['ctr_store_sk'], 'uniform', parall_3, storage)


 ############################################### breaker here
        logger.debug("aggrgation starts")
        logger.debug(aggregated)
        if aggregated:
            aggregated = pd.concat(aggregated)

            logger.debug("concatinated")
    #         debug_filename = "debug1111111111111111111111111.pickle"
    #         logger.debug(str(os.getcwd()))
    #         pickle.dump(aggregated, open(debug_filename, 'wb'))
    #         return aggregated
    #         aggregated = aggregated.groupby(['sr_customer_sk']).agg({'sr_return_amt':'sum'}).reset_index()
            logger.debug("the colomns are:")
            logger.debug(aggregated.columns)
            if aggregated.empty == False:

                a1 = pd.unique(aggregated['cs_order_number']).size
#         a2 = aggregated['ws_ext_ship_cost'].sum()
#         a3 = aggregated['ws_net_profit'].sum()
        logger.debug(a2)
        logger.debug(a3)

        logger.debug("aggrgation ends")
#         t_mid2 = time.time()
########################################## breaker here


        t_fin = time.time()
#         share.append([t_start,t_fin, fin_num])
       # return ([t_start, t_fin,  a_2, a_3-a_2, a_4-a_3, a_5-a_4])
#         return ([t_start, t_mid, t_fin])
#         return [t_start, t_mid, t_fin], res[0]['outputs_info']

        s_step1 = []
#         return [t_start,t_mid1,t_mid2,t_fin], res[0]['outputs_info'], reduceId, a_3, s_step1, t_step2, s_step2
#         return [t_start,t_mid1,t_fin], res[0]['outputs_info'], reduceId, a_3, s_step1, t_step2, s_step2
        input_array_2 = []
        return [t_start,t_mid1,t_fin], 0, reduceId, a_3, s_step1, input_array, input_array_2

######################### mid1 mid2 as there are 3 steps
    keylist = []
    keylist.append(key)
      #  print(keylist)
 #   wrenexec.parse_module_dependencies(run_command, mod_key, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key6, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key6, from_shared_storage=True, sync_to_shared_storage=False)
    futures = wrenexec.map(run_command, keylist, module_dependencies_key=mod_key6, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#     print("map call success!")
    pywren.wait(futures)
    results = [f.result() for f in futures]
#     for i in range(len(results)):
#         share.append(results[i][0])
    share.append(results)
    data_info['stage6'] = results[0][1]
    lifespan['stage6'] = len(results)*[0]
    change = len(results)*[0]
    for i in range(len(results)):   ########### manager.dict cannot change the second level value!
        taskId = results[i][2]
        change[taskId] = results[i][0]
    lifespan['stage6'] = change
#     share.append(results)
#     for key in keylist:
#         results = run_command(key)

################################################################################################ the complex stage 8 which joins stage 6 and 7, then groupby

################################################## stage6, joinning results of stage 4 and 5
#     merged = d.merge(sr, left_on='ctr_store_sk', right_on='s_store_sk')
#     merged_u = merged[['ctr_store_sk','c_customer_id','ctr_total_return','ctr_customer_sk']]


###################################################################### split tables
###################
## map statge
#########################
######################################################################
def split_table(key):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.INFO)
        logger = logging.getLogger(__name__)

        logger.debug(key)
        t_s = time.time()
        partition_num = key['partition_num']
        #em = key['em']
        taskId = key['taskId']
        appName = key['appName']
        stageId = key['stageId']
        locations = key['loc']
        switch = key['cached']

        rounds = len(key['loc'])
        for i in range(rounds):
            logger.debug("round " + str(i) + " starts")
    ###########################     create message
    ##############################     read table
            t_in = time.time()
            dd = read_s3_table(key, i, s3_client=None)
            logger.debug("read s3 complete, takes " + str(time.time()-t_in))
    #########  split
            leng = len(dd)/split_ratio
            chunks = [df.iloc[x:min(x+leng,len(dd))] for x in range(0, len(dd), leng)]

            for j in range(len(chunks)):
                chunks[i]


    keylist = []
    keylist.append(key)
#    wrenexec.parse_module_dependencies(run_command, mod_key2, from_shared_storage=False, sync_to_shared_storage=True)
    if switch == 'cold':
        wrenexec.parse_module_dependencies(run_command, mod_key0, from_shared_storage=False, sync_to_shared_storage=True)
    else:
        wrenexec.parse_module_dependencies(run_command, mod_key0, from_shared_storage=True, sync_to_shared_storage=False)
 #   futures = wrenexec.map(run_command, keylist[0], extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
    futures = wrenexec.map(run_command, keylist[0], module_dependencies_key=mod_key0, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)
#    futures = wrenexec.map(run_command, keylist, extra_env=EXTRA_ENV, invoke_pool_threads=INVOKE_POOL_THREADS)   this is for async launch
    pywren.wait(futures)
    results = [f.result() for f in futures]

###################################################### calculate task launch time based on the profiled infomation
def calculator():
    ########### initialize
    start_time = stage_num*[[]]
    end_time = stage_num*[[]]
#     schedule_map_list = [1,2,5,7]
    min_start_time_to_produce  = stage_num*[[]]

    def map_cal(stageId):
        start_time[stageId-1] = np.zeros(partition_num[stageId-1])
        end_time[stageId-1] = start_time[stageId-1] + duration[str(stageId)]
    ######### initialize

    def join_cal(stageId):
        start_time[stageId-1] = np.zeros(partition_num[stageId-1])
        end_time[stageId-1] = np.zeros(partition_num[stageId-1])
        max_p1_end = max((end_time[int(p[str(stageId)+str(1)]/10)-1]))  # max finish time of p1
        min_p1_start = min((start_time[int(p[str(stageId)+str(1)]/10)-1]))  # min start time of p1
        ################ the start time should be the step rather than the whole start time, should correct


        max_p2_end = max((end_time[int(p[str(stageId)+str(2)]/10)-1]))  # max finish time of p2
        min_p2_start = min((start_time[int(p[str(stageId)+str(2)]/10)-1]))  # min start time of p2
        temp_end1 = np.zeros(partition_num[stageId-1])
        temp_end2 = np.zeros(partition_num[stageId-1])
        print(max_p1_end)
        print(max_p2_end)
        print(min_p1_start)
        print(min_p2_start)
        print(duration[str(stageId)+str(1)])
        print(duration[str(stageId)+str(2)])
        for i in range(len(start_time[stageId-1])):
            temp_end1[i] = max(max_p1_end, min_p1_start + duration[str(stageId)+str(1)][i])    #  this is not accurate for the best end time, see paper
            temp_end2[i] = max(max_p2_end, min_p2_start + duration[str(stageId)+str(2)][i])
#             print(end_time[stageId-1])
            print(temp_end1)
            print(temp_end2)
#             print(duration[str(stageId)+str(2)])
            end_time[stageId-1][i] = max(temp_end1[i] + duration[str(stageId)+str(2)][i], temp_end2[i])
            start_time[stageId-1][i] = end_time[stageId-1][i] -  duration[str(stageId)+str(1)][i] - duration[str(stageId)+str(2)][i]
        print(end_time[stageId-1])
        print(start_time[stageId-1])
        print("finish")

    def groupby_cal(stageId):
        start_time[stageId-1] = np.zeros(partition_num[stageId-1])
        end_time[stageId-1] = np.zeros(partition_num[stageId-1])
        max_p1_end = max((end_time[int(p[str(stageId)+str(1)]/10)-1]))  # max finish time of p1
        min_p1_start = min_start_time_to_produce[int(p[str(stageId)+str(1)]/10)-1]   # min start time of p1

#         print(start_time)
#         print(end_time)
#         print(int(p[str(stageId)+str(1)]/10)-1)
#         min_p1_start = min((start_time[int(p[str(stageId)+str(1)]/10)-1]))  # min start time of p1
#         max_p2_end = max(int(end_time[int(p[stageId+str(2)]/10)-1]))  # max finish time of p2
#         min_p2_start = min(int(start_time[int(p[stageId+str(2)]/10)-1]))  # min start time of p2
        temp_end1 = np.zeros(partition_num[stageId-1])

        print(max_p1_end)
        print(min_p1_start)
        print(duration[str(stageId)+str(1)])
        print(duration[str(stageId)+str(2)])

        for i in range(len(start_time[stageId-1])):
            temp_end1[i] = max(max_p1_end, min_p1_start + duration[str(stageId)+str(1)][i])    #  this is not accurate for the best end time, see paper
            end_time[stageId-1][i] = temp_end1[i] + duration[str(stageId)+str(2)][i]
            start_time[stageId-1][i] = end_time[stageId-1][i] -  duration[str(stageId)+str(1)][i] - duration[str(stageId)+str(2)][i]
        print(end_time[stageId-1])
        print(start_time[stageId-1])


    def stage_3_cal():
        stageId = 3
#      results of stage 3   [t_start,t_read_table,t_read_right,t_read_left, t_self_join,t_hash_join, t_broad_join, t_fin]
# note that here p1 and p2 are just parent sun relation about input output steps across stages, not the absolute step id

##### it reads p2 (stage 2 output, the right table first)
        start_time[stageId-1] = np.zeros(partition_num[stageId-1])
        print(int(p[str(stageId)+str(1)]/10)-1)
        print(end_time[0])
        print(end_time[1])

        end_time[stageId-1] = np.zeros(partition_num[stageId-1])
        max_p1_end = max((end_time[int(p[str(stageId)+str(1)]/10)-1]))  # max finish time of p1
        min_p1_start = min((start_time[int(p[str(stageId)+str(1)]/10)-1]))  # min start time of p1
        max_p2_end = max((end_time[int(p[str(stageId)+str(2)]/10)-1]))  # max finish time of p2
        min_p2_start = min((start_time[int(p[str(stageId)+str(2)]/10)-1]))  # min start time of p2
        temp_end1 = np.zeros(partition_num[stageId-1])
        temp_end2 = np.zeros(partition_num[stageId-1])
#         print(max_p1_end)
#         print(max_p2_end)
#         print(min_p1_start)
#         print(min_p2_start)
#         print(duration[str(stageId)+str(1)])
#         print(duration[str(stageId)+str(2)])
        min_start_time_to_produce[stageId-1] = 10000000000
        for i in range(len(start_time[stageId-1])):
            temp_end1[i] = max(max_p2_end, max(duration[str(stageId)+str(1)][i], min_p2_start) + duration[str(stageId)+str(2)][i])    #  this is the best possible time to finish reading p2. Not that it reads p2 (stage 2 output, the right table) first.
            #max(duration[str(stageId)+str(1)][i], min_p2_start) is the optimal start time of reading p2
            ### then it is a fix duration for execution
            temp_end2[i] = max(max_p1_end, max(min_p1_start, temp_end1[i]) + duration[str(stageId)+str(3)][i])
#  max(min_p1_start, tmp_end1[i]) is the optimal start time of reading p1
#             print(end_time[stageId-1])
#this is the best possible time to finish reading p1
#             fix_duration = duration[str(stageId)+str(4)][i] + duration[str(stageId)+str(5)][i] + duration[str(stageId)+str(6)][i]
#             print(temp_end1)
#             print(temp_end2)
#             print(fix_druation)
############ then
            end_time[stageId-1][i] = temp_end2[i] + duration[str(stageId)+str(4)][i] + duration[str(stageId)+str(5)][i] + duration[str(stageId)+str(6)][i] + duration[str(stageId)+str(7)][i]
            min_start_time_to_produce[stageId-1] = min(min_start_time_to_produce[stageId-1], temp_end2[i] + duration[str(stageId)+str(4)][i] + duration[str(stageId)+str(5)][i] + duration[str(stageId)+str(6)][i])

            start_time[stageId-1][i] = max(temp_end2[i] - duration[str(stageId)+str(3)][i] -duration[str(stageId)+str(2)][i] - duration[str(stageId)+str(1)][i], 0)
        print(end_time[stageId-1])
        print(start_time[stageId-1])
        print("finish")



# results of stage 5 [t_start,t_read_table,t_mid,t_fin]
    def stage_5_cal():
        stageId = 5
        start_time[stageId-1] = np.zeros(partition_num[stageId-1])
        end_time[stageId-1] = np.zeros(partition_num[stageId-1])
        max_p1_end = max((end_time[int(p[str(stageId)+str(1)]/10)-1]))  # max finish time of p1
        min_p1_start = min((start_time[int(p[str(stageId)+str(1)]/10)-1]))  # min start time of p1
        max_p2_end = max((end_time[int(p[str(stageId)+str(2)]/10)-1]))  # max finish time of p2
#         min_p2_start = min((start_time[int(p[str(stageId)+str(2)]/10)-1]))  # min start time of p2
        min_p2_start = min_start_time_to_produce[int(p[str(stageId)+str(2)]/10)-1]   # min start time of p2
        temp_end1 = np.zeros(partition_num[stageId-1])
        temp_end2 = np.zeros(partition_num[stageId-1])

        min_start_time_to_produce[stageId-1] = 10000000000

        for i in range(len(start_time[stageId-1])):
            temp_end1[i] = max(max_p1_end, max(duration[str(stageId)+str(1)][i], min_p1_start) + duration[str(stageId)+str(2)][i])    #
            temp_end2[i] = max(max_p2_end, max(min_p2_start, temp_end1[i]) + duration[str(stageId)+str(3)][i])

#             print(fix_druation)
############ then
            end_time[stageId-1][i] = temp_end2[i]
            min_start_time_to_produce[stageId-1] = min(min_start_time_to_produce[stageId-1], temp_end2[i] - duration[str(stageId)+str(3)][i])
            start_time[stageId-1][i] = max(temp_end2[i] - duration[str(stageId)+str(3)][i] -duration[str(stageId)+str(2)][i] - duration[str(stageId)+str(1)][i], 0)
        print(end_time[stageId-1])
        print(start_time[stageId-1])
        print("finish")





    if exe_stage > 0:
        map_cal(1)
    if exe_stage > 1:
        map_cal(2)
    if exe_stage > 3:
        map_cal(4)

    if exe_stage > 2:
        stage_3_cal()
    if exe_stage > 4:
        stage_5_cal()

    if exe_stage > 5:
        groupby_cal(6)


    return start_time, end_time

def queue_opener():
    emnew = JiffyClient(host=em)
    for i in range(stage_num-1):
        stageId = i + 1
        ########### only need to open ques as senders as it includes both msg and data queues
        data_ques, msg_que = open_or_create_jiffy_queues(emnew, appName, partition_array[c[str(stageId)]-1], int(str(stageId) + str(s[str(stageId)])), 'sender')
        logger.debug("queue in stage " + str(stageId) + " opend")



#     def final(stageId):
#         start_time[stageId-1] = np.zeros(partition_num[stageId-1])
#         max_p1_end = max(int(end_time[int(p[stageId+str(1)]/10)-1]))  # max finish time of p1
#         min_p1_start = min(int(start_time[int(p[stageId+str(1)]/10)-1]))  # min start time of p1
#         max_p2_end = max(int(end_time[int(p[stageId+str(2)]/10)-1]))  # max finish time of p2
#         min_p2_start = min(int(start_time[int(p[stageId+str(2)]/10)-1]))  # min start time of p2
#         temp_end1 = np.zeros(partition_num[stageId-1])
#         temp_end2 = np.zeros(partition_num[stageId-1])
#         for i in range(len(start_time[stageId-1])):
#             temp_end1[i] = max(max_p1_end, min_p1_start + duration['pi1'][i])    #  this is not accurate for the best end time, see paper
#             temp_end2[i] = max(max_p2_end, min_p2_start + duration['pi2'][i])
#             end_time[stageId-1][i] = max(temp_end1[i] + duration['pi2'][i], temp_end2[i])
#             start_time[stageId-1] = end_time[stageId-1][i] -  duration['pi1'][i] - duration['pi2'][i]

#####################################################################
if __name__ == '__main__':
    em = '172.31.53.197'

    exe_stage = int(sys.argv[5])    ######### the number of stages executed, for debug purpose
    rounds = int(sys.argv[1])
    partitions = int(sys.argv[2])
    profiled = int(sys.argv[4])
    scheme = sys.argv[3]
    switch = (sys.argv[6])
    pal_ini = sys.argv[7].strip('[]').split(',')
    round_ini= sys.argv[8].strip('[]').split(',')
    r_seed = int(sys.argv[9])
    appName = (sys.argv[10])
    query_number = int(sys.argv[11])
    em = (sys.argv[12])
#     appName = 'test-1'

    random.seed(r_seed)


    partition_array = [int(i) for i in pal_ini]   ##### for all stages

    #example [1,5,5,5,5,5,1,5]

    round_array = [int(i) for i in round_ini]   ####### only for map stages
   #example [1,5,-1,-1, 5, -1 , 5, -1]
#     profiled = 0   ############## it is based on previous information
############# here the number of tasks in each stage equals to a same partition number, can modify later



    RIGHT_IN_RATE = 6500000
    RIGHT_OUT_RATE = 5500000
    LEFT_RATE = 100000000     #### Bps
    STARTUP_TIME = 4    ###ms start up time
    ALPHA = 0   ###### a paramter for looser launch condition
    P_thresh = 0.03   ### when start to launch task
    OUT_RATIO = 0.25
    stage_num = 6
    step_num = [1,1,3,1,2,2]

    ############### create round and parallism array for all stages
#     parall = partitions
#     partition_array = stage_num*[partitions]
#     partition_array[0] = 1
#     partition_array[6] = 1
#     partition_array[7] = 1

#     partition_array[4] = 1   # stage 5




#     round_arry = stage_num*[rounds]
#     round_arry[0] =
#     round_arry[]

################################################ profiles
    profile_filename = "profile" + appName +  query_name + ".pickle"
    if os.path.exists(profile_filename):
        profile_info = pickle.load(open(profile_filename, "rb"))

 #   pm = [str(parall_1), str(parall_2), str(parall_3), str(pywren_rate), str(n_nodes)]
#    filename = "nomiti.cluster-" +  storage_mode + '-tpcds-q' + query_name + '-scale' + str(scale) + "-" + "-".join(pm) + "-b" + str(n_buckets) + ".pickle"


########### a global variable for each step, indicating the total data it is supposed to process
# for stage i, it is calcualted and updated by stage i's scheduler, by checking
#(1)its parents execution (in message queue) (2) its parents total data to process
    manager = Manager()
###initiate dict
################## some variables that carries job characteristics and intermediete states


    total_task_size = manager.dict()
    total_cosume_rate = manager.dict()
    total_output_rate = manager.dict()
    cal_start_time = manager.dict()
    cal_fin_time = manager.dict()
    total_time = manager.dict()


    data_info = manager.dict()  ### format info
    lifespan = manager.dict()
    size_info = manager.dict()
    total_output = manager.dict()

#     total_cosume_rate

    if profiled == 1:
        for i in range(stage_num):
            i = i+1
            for j in range(step_num[i-1]):
                j = j+1
                total_task_size[str(i)+str(j)] = profile_info['total_task_size'][str(i)+str(j)]   # now calculated based on portion
                total_cosume_rate[str(i)+str(j)] = profile_info['total_cosume_rate'][str(i)+str(j)] ## this is needed in all case
                total_output_rate[str(i)+str(j)] = profile_info['total_output_rate'][str(i)+str(j)]   ## not used now
                cal_start_time[str(i)+str(j)] = profile_info['cal_start_time'][str(i)+str(j)]     ## not used now
                cal_fin_time[str(i)+str(j)] = profile_info['cal_fin_time'][str(i)+str(j)]
                total_time[str(i)+str(j)] = profile_info['total_time'][str(i)+str(j)]   ### basicly this is the time for each step profiled

    else:
        for i in range(stage_num):
            i = i+1
            for j in range(step_num[i-1]):
                j = j+1
                total_task_size[str(i)+str(j)] = partition_array[i-1]*[10000000]   # now calculated based on portion
                total_cosume_rate[str(i)+str(j)] = partition_array[i-1]*[100000] ## this is needed in all case
                total_output_rate[str(i)+str(j)] = partition_array[i-1]*[10000000]   ## not used now
                cal_start_time[str(i)+str(j)] = partition_array[i-1]*[-1]     ## not used now
                cal_fin_time[str(i)+str(j)] = partition_array[i-1]*[1000000]
                total_time[str(i)+str(j)] = partition_array[i-1]*[-1]   ### basicly this is the time for each step profiled


    ###initiate step dependancies
    p = dict()   #### parent relation
    c = dict()    #### child relation
    t = dict()    #####type : join /map stage
    s = dict()    ##### the sender step in each stage
    for i in range(stage_num):
        i = i+1
        for j in range(step_num[i-1]):
            j = j+1
            p[str(i)+str(j)] = -1

    p['31'] = 11     ### the parent of the 1st step in stage 3 is the 1st step of stage 1
    p['32'] = 21
    p['51'] = 41
    p['52'] = 33
    p['61'] = 52

#     total_input_step = [0,0,2,0,2,1]   ######## the total input step per stage

#     p['31'] = 1     ### the parent of the 1st input in stage 3 is the output of stage 1
#     p['32'] = 2
#     p['51'] = 4
#     p['52'] = 3
#     p['61'] = 5


    c['1'] = 3   # the child of stage 1 is stage 3
    c['2'] = 3
    c['3'] = 5
    c['4'] = 5
    c['5'] = 6


    #so,partition_num[stageId-1] is the partion number of a stage  partition_num[c[str(stageId)]-1] is the partiton_number of its child
############## the key for each stage  ,   the partition number of its parent : partion_num[int(p[(str(stageId) + str(step_Id))]/10)-1]

    ##### stage type for scheduoling calculation (calculator())
    t['1'] = 'map'
    t['2'] = 'map'
    t['4'] = 'map'

    t['3'] = 'join'
    t['6'] = 'join'
    t['4'] = 'groupby'
    ####### for special ones can have different types

    ############## sender step in each stage, used in cal
    s['1'] = 1
    s['2'] = 1
    s['4'] = 1
    s['3'] = 3
    s['5'] = 2

    ##################### open queues
    queue_opener()

    ######################### create keys for each stage
    key = {}
    # print(task_id)
#    key['taskId'] = 1
    key['appName'] = appName
    #key['names'] = names
    # key['dtypes'] = dtypes
    key['partition_num'] = partition_array
    partition_num = partition_array
    key['rounds'] = rounds
    key['rounds2'] = rounds   ############## this has to be fixed

    key['em'] = em
    key['stageId'] = 2
    key['names'] = []  ### should change after we have the schema
    key['dtypes'] = []
    key['cached'] = switch



#     for i in range(stage_num):
#         stageId = i + 1
#         globals()['key'+str(stageId)] = copy.deepcopy(key)
#         globals()['key'+str(stageId)]['stageId'] = stageId

#         globals()['key'+str(stageId)]['rounds'] =


    key1 = copy.deepcopy(key)    # partion does not affect the map ones, they have k partitions where k = total chunk/round number
    key1['stageId'] = 1
    key1['rounds'] = round_array[0]
#     key1['partition_num'] = 1

    key2 = copy.deepcopy(key)    # for map ones, the name and dtypes are found by the map launcher (by the table name)
    key2['stageId'] = 2
    key2['rounds'] = round_array[1]

    chunks_stage1 = 20
    chunks_stage2 = 5

    key4 = copy.deepcopy(key)
    key4['stageId'] = 4
    key4['rounds'] = round_array[3]

    key3 = copy.deepcopy(key)
    key3['stageId'] = 3
    key3['rounds'] = round_array[0] / chunks_stage1     # inherited from stage 1
    key3['rounds2'] = round_array[1] /chunks_stage2   # inherited from stage 2
    key3['rounds3'] = 2                # this is the number of rounds/chunks of the broadcast table


    key5 = copy.deepcopy(key)
    key5['stageId'] = 5
    key5['rounds'] = round_array[3]     # inherited from stage 4
    key5['rounds2'] = round_array[0]/chunks_stage1   # inherited from stage 3
    key5['rounds3'] = 1                # now just hard code it

    key6 = copy.deepcopy(key)
    key6['stageId'] = 6
    key6['rounds'] = round_array[0] /chunks_stage1 # inherited from stage 5, which is inherited from stage 3, which is inherited from stage 1


        ############# variables to collect performance for each stage
    for i in range(stage_num):
        j = i+1
        globals()['shared_var_'+str(j)] = manager.list([])



    keylist1 = map_stage_launcher1('stage1',key1,'catalog_sales', shared_var_1)
    keylist2 = map_stage_launcher1('stage2',key2,'catalog_returns', shared_var_2)
    keylist4 = map_stage_launcher1('stage4',key4,'customer_address', shared_var_4)

    p1 = Process(target = map_stage_launcher2,  args = ('stage1',keylist1, key1, shared_var_1,))
    p2 = Process(target = map_stage_launcher2,  args = ('stage2',keylist2, key2, shared_var_2,))
    p4 = Process(target = map_stage_launcher2,  args = ('stage4',keylist4, key4, shared_var_4,))
    t_ss = time.time()



    if profiled == 0:    ############# it is a profile run, cannot use jot scheudling, have to run lazily to get information

        start_time = stage_num*[[0]]

        if exe_stage > 0:
            p1.start()
            p1.join()
        if exe_stage > 1:
            p2.start()
            p2.join()
        if exe_stage > 3:
            p4.start()
            p4.join()

                          #         logger.debug("now print data info:")
#         logger.debug(data_info)
        if exe_stage > 2:
            key3['names1'] = data_info['stage1']['names']
            key3['names2'] = data_info['stage2']['names']
            key3['dtypes1'] = data_info['stage1']['dtypes']
            key3['dtypes2'] = data_info['stage2']['dtypes']


            ######### this is for the broadcast table:
            table_name = 'date_dim'
            names = get_name_for_table(table_name)
            dtypes = get_dtypes_for_table(table_name)
            chunks = get_input_locations(table_name, 1, key3['rounds3'])
            key3['loc'] = chunks[0]
            key3['names'] = names
            key3['dtypes'] = dtypes

            p3 = Process(target = stage_launcher_new , args = (key3,shared_var_3, stage3, 'lazy', start_time,))
            p3.start()
            p3.join()
    ######## change the code so it also changes the infor
    ########### both the launcher and the job will change info
    ##### launcher: estimations  job: datatypes.
    ############## note that these info cannot be used in the non-proflied case because they have to wait for the task to finish

        if exe_stage > 4:
            key5['names1'] = data_info['stage4']['names']
            key5['names2'] = data_info['stage3']['names']
            key5['dtypes1'] = data_info['stage4']['dtypes']
            key5['dtypes2'] = data_info['stage3']['dtypes']

                        ######### this is for the broadcast table:
            table_name = 'call_center'
            names = get_name_for_table(table_name)
            dtypes = get_dtypes_for_table(table_name)
            chunks = get_input_locations(table_name, 1, key5['rounds3'])
            key5['loc'] = chunks[0]
            key5['names'] = names
            key5['dtypes'] = dtypes



            p5 = Process(target = stage_launcher_new , args = (key5,shared_var_5, stage5, 'lazy', start_time,))
            p5.start()
            p5.join()

        if exe_stage > 5:
            key6['names'] = data_info['stage5']['names']
            key6['dtypes'] = data_info['stage5']['dtypes']

            p6 = Process(target = stage_launcher_new,  args = (key6,shared_var_6, stage6, 'lazy', start_time,))
            print("now process stage 6")
            p6.start()
            p6.join()


        ################## profile things
#         for i in range(stage_num):
#             stageId = i+1
#             ini_time = lifespan['stage' + str(stageId)]
#             print(ini_time)
#             print(ini_time[0])
#             if len(ini_time[0])>2:
#                 for j in range(len(ini_time)):
#                     total_time[str(stageId)+str(1)][j] = ini_time[j][1] - ini_time[j][0]
#                     total_time[str(stageId)+str(2)][j] = ini_time[j][2] - ini_time[j][1]
#                     print(total_time[str(stageId)+str(1)][j])
#             else:
#                 for j in range(len(ini_time)):
#                     total_time[str(stageId)] = ini_time[j][1] - ini_time[j][0]

    else:      ###########  profiled
        lifespan2 = profile_info['lifespan']
        duration = dict()
        print(lifespan2)
#         for i in range(stage_num):


#         for i in range(exe_stage):   # here I just test the first 6 stage
#             stageId = i+1
#             ini_time = lifespan2['stage' + str(stageId)]

#             if len(ini_time[0])>2:
#                 duration[str(stageId)+str(1)] = len(ini_time)*[-1]
#                 duration[str(stageId)+str(2)] = len(ini_time)*[-1]
#                 for j in range(len(ini_time)):
#                     duration[str(stageId)+str(1)][j] = ini_time[j][1] - ini_time[j][0]
#                     duration[str(stageId)+str(2)][j] = ini_time[j][2] - ini_time[j][1]
#             else:
#                 duration[str(stageId)] = len(ini_time)*[-1]
#                 for j in range(len(ini_time)):
#                     duration[str(stageId)][j] = ini_time[j][1] - ini_time[j][0]


        for i in range(exe_stage):   # here I just test the first 6 stage
            stageId = i+1
            ini_time = lifespan2['stage' + str(stageId)]
    #         print(ini_time)
            if len(ini_time[0]) > 2:   #### this is only for stage 6 for q94
                step_no = len(ini_time[0])-1
                for ind in range(step_no):
                    duration[str(stageId)+str(ind+1)] = len(ini_time)*[-1]
                for j in range(len(ini_time)):
                    for ind in range(step_no):
                        duration[str(stageId)+str(ind+1)][j] = ini_time[j][ind+1] - ini_time[j][ind]
            else:
                duration[str(stageId)] = len(ini_time)*[-1]
                for j in range(len(ini_time)):
                    duration[str(stageId)][j] = ini_time[j][1] - ini_time[j][0]





#         duration = profile_info['total_time']
        print(duration)
        start_time = stage_num*[[]]
        end_time = stage_num*[[]]
        start_time, end_time = calculator()
        print(start_time)
        t_ss = time.time()


        if exe_stage > 2:
            key3['names1'] = profile_info['data_info']['stage1']['names']
            key3['names2'] = profile_info['data_info']['stage2']['names']
            key3['dtypes1'] = profile_info['data_info']['stage1']['dtypes']
            key3['dtypes2'] = profile_info['data_info']['stage2']['dtypes']

            ######### this is for the broadcast table:
            table_name = 'date_dim'
            names = get_name_for_table(table_name)
            dtypes = get_dtypes_for_table(table_name)
            chunks = get_input_locations(table_name, 1, key3['rounds3'])
            key3['loc'] = chunks[0]
            key3['names'] = names
            key3['dtypes'] = dtypes


            p3 = Process(target = stage_launcher_new , args = (key3,shared_var_3, stage3, scheme, start_time,))

        if exe_stage > 4:
            key5['names1'] = profile_info['data_info']['stage4']['names']
            key5['names2'] = profile_info['data_info']['stage3']['names']
            key5['dtypes1'] = profile_info['data_info']['stage4']['dtypes']
            key5['dtypes2'] = profile_info['data_info']['stage3']['dtypes']

                        ######### this is for the broadcast table:
            table_name = 'call_center'
            names = get_name_for_table(table_name)
            dtypes = get_dtypes_for_table(table_name)
            chunks = get_input_locations(table_name, 1, key5['rounds3'])
            key5['loc'] = chunks[0]
            key5['names'] = names
            key5['dtypes'] = dtypes

            p5 = Process(target = stage_launcher_new , args = (key5,shared_var_5, stage5, scheme, start_time,))

        if exe_stage > 5:
            key6['names'] = profile_info['data_info']['stage5']['names']
            key6['dtypes'] = profile_info['data_info']['stage5']['dtypes']

            p6 = Process(target = stage_launcher_new,  args = (key6,shared_var_6, stage6, scheme, start_time,))
            print("now process stage 6")


        for i in range(stage_num):
            if i < exe_stage:
                exec("p"+ str(i+1) +".start()")

        for i in range(stage_num):
            if i < exe_stage:
                exec("p"+ str(i+1) +".join()")



#     print(info["outputs_info"][0]['names'])
#     print(info["outputs_info"][0]['dtypes'])
#     if proflie = 1:
#         proflie_info['total_task_size'] = total_task_size
#         proflie_info['total_cosume_rate'] = total_cosume_rate
#         proflie_info['total_output_rate'] = total_output_rate
#         proflie_info['cal_start_time'] = cal_start_time
#         proflie_info['cal_fin_time'] = cal_fin_time
#         proflie_info['total_time'] = total_time
#         proflie_info['data_info'] = total_time


#         pickle.dump(profile_info, open(profile_filename, 'wb'))


#     proflie_info = pickle.load(open(profile_filename, "rb"))
#     print(proflie_info['data_info']['stage2']['names'])
#     print(proflie_info['data_info']['stage2']['dtypes'])



#     key['names2'] = info2["outputs_info"][0]['names']
#     key['dtypes2'] = info2["outputs_info"][0]['dtypes']

##################### calculate duration and lifespan: lifespan: start end time for each step, duration, the lengh

    exp1_info = {}
#     print(shared_var_2)
    for x in range(exe_stage):
        stageId = x+1
        results = eval(("shared_var_" + str(stageId)))
#         print(results)
#         data_info['stage' + str(stageId)] = results[0][1]
        if stageId == 1 or stageId == 2 or stageId == 4:  # mappers
            results = results[0]



        lifespan['stage' + str(stageId)] = len(results)*[0]
        size_info['stage' + str(stageId)] = len(results)*[0]
        total_output['stage' + str(stageId)] = len(results)*[0]
        change = len(results)*[0]
        change2 = len(results)*[0]
        change3 = len(results)*[[]]

#         if stageId == 2:
#             print(results)
#             print(len(results))

#         if stageId == 3:
#             print(results)
#             print(len(results))

#         if stageId == 8:
#             print(results)
#             print(len(results))
#         if x == 1 or x == 2 or x == 5 or x == 7:  # mappers

#             for i in range(len(results)):   ########### manager.dict cannot change the second level value!
#     #             print(i)
#     #             print(results[i])
# #                 print(results[i][0])
#                 taskId = results[i][2]
#                 change[taskId] = results[i][0]
#         else:
        for i in range(len(results)):
            if stageId == 1 or stageId == 2 or stageId == 4:
                taskId = results[i][2]
                change[taskId] = results[i][0]
                change2[taskId] = results[i][-1]
#                 print(results[i][-1])
                change3[taskId] = len(results[i][-1][0])*[0]
                for rnd in range(len(results[i][-1])):
                    for dst in range(len(results[i][-1][rnd])):
                        change3[taskId][dst] = change3[taskId][dst] + int(results[i][-1][rnd][dst])
            else:
#                 print(results[i][0][-1])
                taskId = results[i][0][2]
                change[taskId] = results[i][0][0]
                change2[taskId] = results[i][0][-3]
                if stageId != 6:
                    change3[taskId] = len(results[i][0][-3][0])*[0]
                    for rnd in range(len(results[i][0][-3])):
                        for dst in range(len(results[i][0][-3][rnd])):
                            change3[taskId][dst] = change3[taskId][dst] + int(results[i][0][-3][rnd][dst])
        output_array = []
        input_array_1 = []
        input_array_2 = []

        for i in range(len(results)):
            if stageId == 1 or stageId == 2 or stageId == 4:
                logger.info("##############################  Printing the output info here: ##########################")
                logger.info(stageId)
                output_array.append(results[i][-1])
            elif stageId == 6:
                logger.info("##############################  Printing the output info here: ##########################")
                logger.info(stageId)
                input_array_1.append(results[i][0][-2])
                input_array_2.append(results[i][0][-1])
            else:
                logger.info("##############################  Printing the output info here: ##########################")
                logger.info(stageId)
                input_array_1.append(results[i][0][-2])
                input_array_2.append(results[i][0][-1])
                output_array.append(results[i][0][-3])

        if stageId == 1 or stageId == 2 or stageId == 4:
            exp1_info[str(stageId) + '-output_array'] = output_array

        elif stageId == 6:
            exp1_info[str(stageId) + '-input_array_1'] = input_array_1
            exp1_info[str(stageId) + '-input_array_2'] = input_array_2
        else:
            exp1_info[str(stageId) + '-input_array_1'] = input_array_1
            exp1_info[str(stageId) + '-input_array_2'] = input_array_2
            exp1_info[str(stageId) + '-output_array'] = output_array

        lifespan['stage' + str(stageId)] = change
        size_info['stage' + str(stageId)] = change2
        total_output['stage' + str(stageId)] = change3

    exp1_info['lifespan'] = lifespan
    exp1_info['size_info'] = size_info
    exp1_info['total_output'] = total_output

#     print(size_info)
#     print(total_output)
    duration2 = dict()
    for i in range(exe_stage):   # here I just test the first 6 stage
        stageId = i+1
        ini_time = lifespan['stage' + str(stageId)]
#         print(ini_time)
        if len(ini_time[0]) > 2:   #### this is only for stage 6 for q94
            step_no = len(ini_time[0])-1
            for ind in range(step_no):
                duration2[str(stageId)+str(ind)] = len(ini_time)*[-1]
            for j in range(len(ini_time)):
                for ind in range(step_no):
                    duration2[str(stageId)+str(ind)][j] = ini_time[j][ind+1] - ini_time[j][ind]
        else:
            duration2[str(stageId)] = len(ini_time)*[-1]
            for j in range(len(ini_time)):
                duration2[str(stageId)][j] = ini_time[j][1] - ini_time[j][0]
    print(duration2)
    exp1_info['duration'] = duration2

#     print(max(duration2['2']))
#     print(min(duration2['2']))
#     print(np.mean(duration2['2']))
#     print(np.std(duration2['2']))


#             if len(ini_time[0]) == 3:   #### this is only for stage 6 for q94
#             duration2[str(stageId)+str(1)] = len(ini_time)*[-1]
#             duration2[str(stageId)+str(2)] = len(ini_time)*[-1]
#             for j in range(len(ini_time)):
#                 duration2[str(stageId)+str(1)][j] = ini_time[j][1] - ini_time[j][0]
#                 duration2[str(stageId)+str(2)][j] = ini_time[j][2] - ini_time[j][1]



############### if not profiled, store some information for later runs
    profile_info = {}
    profile_info['total_task_size'] = total_task_size
    profile_info['total_cosume_rate'] = total_cosume_rate
    profile_info['total_output_rate'] = total_output_rate
    profile_info['cal_start_time'] = cal_start_time
    profile_info['cal_fin_time'] = cal_fin_time
    profile_info['total_time'] = total_time

    profile_info['data_info'] = data_info
    profile_info['lifespan'] = lifespan
    profile_info['total_output'] = total_output
    profile_info_dict = copy.deepcopy(profile_info)
    exp1_info_dict = copy.deepcopy(exp1_info)
    if profiled == 0:
        pickle.dump(profile_info_dict, open(profile_filename, 'wb'))
    else:
        log_name = "log" + appName +  query_name + scheme +  ".pickle"
        pickle.dump(profile_info_dict, open(log_name, 'wb'))
    pickle.dump(exp1_info_dict, open("exp1_tpcds" + query_name +  "-" + str(query_number) + ".pickle", 'wb'))

    print(t_ss)
#     print("done")
#     print(profile_info_dict)
#     print(shared_var_4[0])
#     aggregated = shared_var_4[0][0].groupby(['sr_customer_sk']).agg({'sr_return_amt':'sum'}).reset_index()
#     a = copy.copy(shared_var_4[0][0])
#     print(type(a))
#     debug_filename = "debug" + appName +  query_name + ".pickle"
# #     if os.path.exists(debug_filename):

#     f = open(profile_filename, 'rb')
#     x = pickle.load(f)
# #     y = x['total_task_size']
# #     print(y)
#     f.close


#     print(shared_var_4[0])
#     aggregated = shared_var_3[0][8].groupby(['sr_customer_sk']).agg({'sr_return_amt':'sum'}).reset_index()
#     a = copy.copy(shared_var_3[0][8])
#     b = copy.copy(shared_var_3[0][9])

#     o = copy.copy(shared_var_3[0])
#     filename = "debug_now.pickle"
#     pickle.dump(o, open(filename, 'wb'))

#     print(type(a))
#     left_filename = "left_full.pickle"
#     pickle.dump(a, open(left_filename, 'wb'))
#     right_filename = "right_full.pickle"
#     pickle.dump(b, open(right_filename, 'wb'))
#     f = open(left_filename, 'rb')
#     x = pickle.load(f)
#     print(x)




#     y = x['total_task_size']
#     print(y)
# #         profile_info = pickle.load(open(debug_filename, "rb"))
#     pickle.dump(a, open(debug_filename, 'wb'))
#     f = open(debug_filename, 'rb')
#     x = pickle.load(f)
#     f.close
#     print(x)
