import requests
import os
import boto3
from datetime import datetime
 
def lambda_handler(event, context):
    
    # Url of imdb website
    URL = os.environ.get('URL')
    
    # user agent used for headers
    user_agent = os.environ.get('user_agent')
    
    # headers for get request
    headers = ({'User-Agent': user_agent})
    
    # getting the response from url
    movie_data = requests.get(URL, headers = headers)
    
    # Converting the response into text
    movie_data_text = movie_data.text
    
    # creating s3 object using boto3
    client = boto3.client('s3')
    
    # Specifying file name
    file_name = "imdb_raw_" + str(datetime.now()) + ".txt"
    
    # save the data in s3
    client.put_object(
        Bucket="imdb-etl-project-atul",
        Key="raw_data/to_process/" + file_name,
        Body= movie_data_text.encode('utf-8')
        )
    
    
