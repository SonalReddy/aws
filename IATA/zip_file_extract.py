import json
import boto3
import zlib
import zipfile
from io import BytesIO


def lambda_handler(event, context):
    
   
    
    
    s3 = boto3.resource('s3', use_ssl=False)
    zip_obj = s3.Object("iata.artha","2m-Sales-Records.zip")
    
   
    buffer = BytesIO(zip_obj.get()["Body"].read())
    print(buffer)
    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        
        file_info = z.getinfo(filename)
    s3.meta.client.upload_fileobj(
        z.open(filename),
        Bucket="iata.artha.target",
        Key="sales-files/"+f'{filename}'
    )
