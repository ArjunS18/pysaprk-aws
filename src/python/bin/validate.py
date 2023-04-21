#import logging
import pandas as pd
import logging.config
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    try:
        spark_date_df = spark.sql(""" select current_date """)
        print(type(spark_date_df))
        logger.info("Validate the Spark object by printing Current Date - " + str(spark_date_df.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(). " + str(exp))
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("Spark object is validated. Spark Object is ready.")

def df_count(df, dfName):
        try:
            logger.info(f"The DataFrame Validation by count df_count() is started for Dataframe {dfName}...")
            df_count = df.count()
            print(f"The DataFrame count is {df_count}.")
        except Exception as exp:
            logger.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exp))
            raise
        else:
            logger.info(f"The DataFrame Validation by count df_count() is completed.")

def df_top10_rec(df, dfName):
        try:
            logger.info(
                f"The DataFrame Validation by top 10 record df_top10_rec() is started for Dataframe {dfName}...")
            logger.info(f"The DataFrame top 10 records are:.")
            logger.info("================================================")
            df_pandas = df.limit(10).toPandas()
            logger.info('\n \t' + df_pandas.to_string(index=False))
            logger.info("=================================================")
        except Exception as exp:
            logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exp))
            raise
        else:
            logger.info("The DataFrame Validation by top 10 record df_top10_rec() is completed.")

def df_print_schema(df, dfName):
    try:
        logger.info(f"The DataFrame Schema Validation for Dataframe {dfName}...")
        sch = df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is: ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_show_schema(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Schema Validation is completed.")







