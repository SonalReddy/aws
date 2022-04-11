import json
import boto3

client = boto3.client('glue')

def lambda_handler(event, context):
    
    
   
    response = client.start_crawler(Name='parquet')
    print(json.dumps(response))