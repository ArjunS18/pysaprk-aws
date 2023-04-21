from pyspark.sql import SparkSession

#import logging
import logging.config
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

#function to create spark object
def get_spark_object(envn, appName):
    if envn == 'TEST':
        master = 'local'
    else:
        master = 'yarn'
    spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    return spark

