from pyspark.sql import SparkSession
import os
from collections import namedtuple

spark = SparkSession.builder.appName("DLG_Assignment").getOrCreate()

def csv_to_parquet(input_dir,output_dir):
    '''
    :param input_dir: location of input csv files
    :param output_dir: location of storing converted parquet files
    :return: list of Namedtuple of (filename, input row count and output row count)
    Iterates over CSV files in input dir and convert them to parquet and create output parquet files in
    foldername derived from csv filename
    '''

    result=namedtuple('Result','filename input_row_count output_row_count')
    result_list=[]
    for filename in os.listdir(input_dir):
        folder_name=filename.split(".")[1]

        #Input CSV DF
        df = spark.read.csv(input_dir+"/"+filename,inferSchema=True,header=True)
        input_row_count=df.count()
        #converting the input file to parquet format
        df.write.parquet(path=output_dir+"/"+folder_name,mode='overwrite')

        #Parquet DF
        output_df=spark.read.parquet(output_dir+"/"+folder_name)
        output_row_count=output_df.count()
        #Test case to compare input rowcount to output rowcount
        result_list.append(result(filename,input_row_count,output_row_count))
    return (result_list)

def query_parquet(dir, query):
    '''
    :param dir: Directory of Parquet Files
    :param query: SQL to be executed
    :return: result dataframe
    '''
    output_files=[]
    for folders in os.listdir(dir):
        output_files.append(dir+"/"+folders)
    df=spark.read.parquet(*output_files)
    df.createOrReplaceTempView('tbl')
    return(spark.sql(query))

if __name__=='__main__':

    base_dir = os.path.dirname(os.path.realpath(__file__))
    input_dir = os.path.dirname(os.path.realpath(__file__)) + "/input"
    output_dir = base_dir + "/output"

    csv_parquet_conversion=csv_to_parquet(input_dir, output_dir)
    for result in csv_parquet_conversion:
        print(result.filename,result.input_row_count,result.output_row_count)

    sql='''
    --Get Maximum Temperature, Observation Date, Region
    SELECT screentemperature,
       ObservationDate,
       region
    FROM   (SELECT screentemperature,
               ObservationDate,
               region,
               Row_number()
                 OVER (
                   ORDER BY screentemperature DESC) AS rk
        FROM   tbl)
    WHERE  rk = 1
    '''
    result_df=query_parquet(output_dir, sql)
    #truncate=False to show full column content in spark df
    result_df.show(truncate=False)