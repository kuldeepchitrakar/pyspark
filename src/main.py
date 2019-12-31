import argparse
import os
from pyspark.sql import SparkSession
import importlib
import sys
import json
import logging
import logging.config
from hdfs3 import HDFileSystem
import datetime

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')


def get_config(path, job):
    file_path = path + '/' + job + '/resources/JobConfig.json'
    with open(file_path, encoding='utf-8') as json_file:
        config = json.loads(json_file.read())
    config['relative_path'] = path
    return config


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='My pyspark job arguments')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help='The name of the spark job you want to run')
    parser.add_argument('--res-path', type=str, required=True, dest='res_path',
                        help='Path to the jobs resources')

    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName(args.job_name)\
        .getOrCreate()

    sc = spark.sparkContext

    #logging

    # get logging config file
    with open('./src/logging.json') as log_json:
        logging_config = json.load(log_json)
    logging.config.dictConfig(logging_config)
    logger_main = logging.getLogger(__name__)

    # Get App Config file
    with open('./src/config.json') as config_json:
        job_config = json.load(config_json)

    # to test various levels
    logger_main.debug('debug message')
    logger_main.info('info message')
    logger_main.warning('warn message')
    logger_main.error('error message')
    logger_main.critical('critical message')

    # initialize log4j for yarn cluster logs
    log4jLogger = spark._jvm.org.apache.log4j
    logger_pyspark = log4jLogger.LogManager.getLogger(__name__)
    logger_pyspark.info("pyspark script logger initialized")

    # push logs to hdfs
    logpath = job_config['logger_config']['path']
    # get the hdfs directory where logs are to be stored
    #hdfs = HDFileSystem(host=job_config['HDFS_host'], port=port)
    current_job_logpath = logpath + f"{datetime.datetime.now():%Y-%m-%d}"
    job_run_path = current_job_logpath + '/' + str(int(f"{datetime.datetime.now():%H}") - 2) + '/'

    # You can uncomment this if you want push the logs to HDFS
    '''
    if hdfs.exists(current_job_logpath):
        pass
    else:
        # create directory for today
        hdfs.mkdir(current_job_logpath)
        hdfs.chmod(current_job_logpath, mode=0o777)

    hdfs.put("info.log", job_run_path + "info.log")
    hdfs.put("errors.log", job_run_path + "errors.log")
    hdfs.put("spark_job_log4j.log", job_run_path + "spark_job_log4j.log")
    '''

    module_name = "jobs."+args.job_name+"."+args.job_name
    job_module = importlib.import_module(module_name)
    res = job_module.run(spark, get_config(args.res_path, args.job_name))



    print('[JOB {job} RESULT]: {result}'.format(job=args.job_name, result=res))

