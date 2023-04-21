import sys
import os

# Import all the necessary Modules
import get_all_variables as gav
from create_objects import get_spark_object
from validate import *
from prescriber_data_ingest import load_files
from prescriber_data_preprocessing import *
from prescriber_run_data_transform import city_report, top_10_prescribers_report

#import logging
import logging.config
logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("Spark Object is created ...")

        # Validate Spark Object
        get_curr_date(spark)

        # Load the City File
        for file in os.listdir(gav.staging_dim_city):
            print("File is " + file)
            print('--------------')
            file_dir = gav.staging_dim_city + '\\' + file
            logging.info(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,inferSchema=inferSchema)

        # Load the Prescriber Fact File
        for file in os.listdir(gav.staging_fact):
            print("File is " + file)
            file_dir = gav.staging_fact + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema=gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header='NA'
                inferSchema='NA'
        df_prescriber_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)


        ### Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        df_count(df_prescriber_fact, 'df_prescriber_fact')
        df_top10_rec(df_prescriber_fact, 'df_prescriber_fact')

        #Initiate prescriber_data_preprocessing Script
        #Perform data Cleaning Operations for df_city and df_prescriber_fact
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_prescriber_fact)

        # Validation for df_city and df_fact
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        #Initiate prescriber_data_transform Script
        #prepare city report
        df_city_report = city_report(df_city_sel, df_fact_sel)
        print(df_city_report)
        #Validation for city report
        logging.info('--------GENERATING CITY REPORT-------')
        print('--------GENERATING CITY REPORT-------')
        df_top10_rec(df_city_report, 'df_city_report')
        df_print_schema(df_city_report, 'df_city_report')
        logging.info('--------CITY REPORT COMPLETE-------')

        #prepare prescribers report
        df_prescriber_report = top_10_prescribers_report(df_fact_sel)
        print(df_city_report)
        #validatiion for prescribers report
        logging.info('--------GENERATING PRESCRIBER REPORT-------')
        print('--------GENERATING PRESCRIBER REPORT-------')
        df_top10_rec(df_prescriber_report, 'df_prescriber_report')
        df_print_schema(df_prescriber_report, 'df_prescriber_report')
        logging.info('--------PRESCRIBER REPORT COMPLETE-------')

    except Exception as ex:
        logging.info("Error in the main() method. Please check the Stack Trace to go to the respective module and fix it.", str(ex))
        sys.exit(1)

### End of Application Part 1

if __name__ == "__main__" :
    logging.info('Main start ...')
    main()