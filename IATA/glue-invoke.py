import json
import boto3

client = boto3.client('glue')

def lambda_handler(event, context):
    
    
    response1 = client.get_crawler_metrics(CrawlerNameList=['sales-csv'])
    status = response1['CrawlerMetricsList'][0]['StillEstimating']
    print((status))
   
    if status == False:
        response = client.start_job_run(JobName = 'csv-to-parquet')
    else:
        client1 = boto3.client('lambda')
        response = client1.invoke(FunctionName = "glue-invoke-1")
        
   