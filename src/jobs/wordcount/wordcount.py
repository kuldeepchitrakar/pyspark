from operator import add
#from src.shared.context import JobContext
import logging


logMe = logging.getLogger(__name__)
logMe.info("Inside Wordcount")

def run (spark, config):
    logMe.info("Executing Wordcount run")
    lines = spark.read.text(config['relative_path'] + config['input_file_path']).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    return output
