import json
import boto3

client = boto3.client('glue')

def lambda_handler(event, context):
    
    
   
    response = client.start_crawler(Name='sales-csv')
    print(json.dumps(response))
    client1 = boto3.client('lambda')
    response1 = client1.invoke(FunctionName = "glue-invoke")
    print(response1)
    