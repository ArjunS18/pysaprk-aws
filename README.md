# pysaprk-aws
Brief overview of the project:
The client requires a report that displays the total number of transactions for each city and the distinct number of subscribers for that city. However, the report should only include cities with prescribers. And that's not all - you'll also need to create a prescriber report that showcases the top 10 prescribers based on transaction count, with 20 to 50 years of experience.

Input files:
USA_pres_medicare_data_2021.csv
us_cities_dimension.parquet

Output required:
- City report should be in json format
- Prescriber report should be in snappy format
- The reports of the result have to stored in client s3 bucket

Topics covered:
Launching Google cloud platform ubuntu compute and installing hadoop, spark, python3, hive.
Issues:
While doing the setup in ubuntu, I faced version mismatch and lot of other issues. Check course Q&A or forums to fix the issues

Data ingestion - ingest parquet, csv source files
Pre-processing - handle null, data inconsistencies in parquet and csv files
Transformation - Use spark sql functions to aggregate, count, partition, filter data to get the output
Storage - AWS S3 bucket

Program files:
1. run_prescriber_pipeline.py - contains main function and calls to perform data cleaning, data aggreagation and report generation
2. validate.py - contains functions to perform number of objects in dataframe, select top 10 records in df, print schema of the database
3. create_objects.py - contains function to create a spark session and returns spark function object
4. prescriber_ingest.py - contains function to load files in csv and parquet format
5. prescriber_data_preprocessing.py - contains function to perform dataframe cleaning
6. presciber_run_data_transform.py - contains function to generate city report and top 10 prescribers report
7. get_all_variables.py - contains all environment variables used in the program
