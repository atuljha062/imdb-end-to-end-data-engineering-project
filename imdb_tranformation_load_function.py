import json
import boto3
from bs4 import BeautifulSoup
import pandas as pd
import requests
import os
import numpy as np
from io import StringIO
from datetime import datetime

# Creating all the functions that extracts data from the individual links 
    
def get_movie_ranking(soup):
    try:
        ranking = soup.find("a", attrs={"class":"ipc-link ipc-link--base ipc-link--inherit-color top-rated-link"})
        
        ranking_text = ranking.text.strip().split('#')[1]
        
    except AttributeError:
        ranking_text = ''
        
    return ranking_text

def get_movie_name(soup):
    
    try:
        name = soup.find("span", attrs={"class":"hero__primary-text"})
        
        name_text  = name.text.strip()
        
    except AttributeError:
        
        name_text = ''
        
    return name_text


def get_imdb_rating(soup):
    
    try:
        rating = soup.find("span", attrs={"class":"sc-bde20123-1 cMEQkK"})
        
        rating_text = rating.text.strip()
        
    except AttributeError:
        rating_text = ''
        
    return rating_text

def get_user_review_count(soup):
    try:
        review_count = soup.find("span", attrs={"class":"score"})
        
        review_count_text = review_count.text.strip()
        
    except AttributeError:
        review_count_text = ''
        
    return review_count_text

def get_release_year(soup):
    
    try:
        # Instead of creating 2 variables creating it in the same variable
        year_text = soup.find("ul", attrs={"class":"ipc-inline-list ipc-inline-list--show-dividers sc-d8941411-2 cdJsTz baseAlt"}).find("a", attrs={"class":"ipc-link ipc-link--baseAlt ipc-link--inherit-color"}).text.strip()
        
    except AttributeError:
        year_text = ''
        
    return year_text

def get_movie_duration(soup):
    
    try:
        duration_text = soup.find("ul", attrs={"class":"ipc-inline-list ipc-inline-list--show-dividers sc-d8941411-2 cdJsTz baseAlt"}).find_all("li", attrs={"class":"ipc-inline-list__item"})[-1].text.strip()
        
    except AttributeError:
        
        duration_text = ''
        
    return duration_text

def get_director(soup):
    
    try:
        director_text = soup.find("a", attrs={"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}).text.strip()
        
    except AttributeError:
        director_text = ''
        
    return director_text

def get_genres(soup):
    
    try:
        genre = soup.find("script", attrs={"type":"application/ld+json"}).text
        
        genre_json = json.loads(genre)["genre"][0]
        
    except AttributeError:
        genre_json = ''
        
    return genre_json

def get_language(soup):
    
    try:
        lang = soup.find("script", attrs={"type":"application/ld+json"}).text
        
        lang_json = json.loads(lang)["review"]["inLanguage"]
        
    except AttributeError:
        lang_json = ''
        
    return lang_json

def lambda_handler(event, context):
    
    # Creating s3 object using boto3
    client = boto3.client('s3')
    
    Bucket = "imdb-etl-project-atul"
    Key = "raw_data/to_process/"
    
    # user agent used for headers
    user_agent = os.environ.get('user_agent')
    
    # headers for get request
    headers = ({'User-Agent': user_agent})
    
    # creating key list to use it at the end to copy and delete files
    file_key_list = []
    
    # Iterate over all the files in Key location
    for file in client.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        
        file_key_list.append(file_key)
        
        # Checking if the files in the location is text file or not
        if file_key.split('/')[-1].find('.txt') != -1:
            
            # Retrieving data from S3
            movie_data_text = client.get_object(
                Bucket= Bucket,
                Key = file_key
                )
            
            movie_data = movie_data_text['Body'].read().decode('utf-8')
            
            soup = BeautifulSoup(movie_data, 'html.parser')
            
            # getting the list of all the links tags to individual movies
            movie_links = soup.find_all("a", attrs={"class":"ipc-title-link-wrapper"}, limit=50)
            
            movie_links_list = []
            
            # getting the individual link from tags and storing it
            for link in movie_links:
                movie_links_list.append("https://www.imdb.com" + link.get('href'))
                
            # creating dictionary for storing data 
            movie_dict = {'movie_ranking':[], 'movie_name':[], 'imdb_rating':[], 'user_review_count':[]}
            movie_meta_data_dict = {'movie_name':[], 'release_year':[], 'movie_duration':[], 'director':[], 'genres':[], 'language':[]}
            
            # looping through each and every link and getting the data
            for link in movie_links_list:
                individual_movie_data = requests.get(link, headers=headers)
                
                new_soup = BeautifulSoup(individual_movie_data.content, 'html.parser')
                
                movie_dict['movie_ranking'].append(get_movie_ranking(new_soup))
                movie_dict['movie_name'].append(get_movie_name(new_soup))
                movie_dict['imdb_rating'].append(get_imdb_rating(new_soup))
                movie_dict['user_review_count'].append(get_user_review_count(new_soup))
                
                movie_meta_data_dict['movie_name'].append(get_movie_name(new_soup))
                movie_meta_data_dict['release_year'].append(get_release_year(new_soup))
                movie_meta_data_dict['movie_duration'].append(get_movie_duration(new_soup))
                movie_meta_data_dict['director'].append(get_director(new_soup))
                movie_meta_data_dict['genres'].append(get_genres(new_soup))
                movie_meta_data_dict['language'].append(get_language(new_soup))
            
            # creating data frame from dictionary 
            movie_df = pd.DataFrame.from_dict(movie_dict)
            
            movie_meta_data_df = pd.DataFrame.from_dict(movie_meta_data_dict)
            
            #  Converting rows with empty ranking as nan and dropping them
            movie_df['movie_ranking'].replace('',np.nan, inplace=True)
            movie_df = movie_df.dropna(subset=['movie_ranking'])
            
            movie_meta_data_df['movie_name'].replace('', np.nan, inplace=True)
            movie_meta_data_df = movie_meta_data_df.dropna(subset=['movie_name'])
            
            # Removing duplicate data from dataframe
            movie_df = movie_df.drop_duplicates(subset=['movie_ranking'])
            
            movie_meta_data_df = movie_meta_data_df.drop_duplicates(subset=['movie_name'])
            
            # converting columns from string to int
            movie_df['movie_ranking'] = movie_df['movie_ranking'].astype(int)
            
            movie_df['imdb_rating'] = movie_df['imdb_rating'].astype(float)
            
            movie_df['user_review_count'] = movie_df['user_review_count'].str.replace('K','').astype(float)
            movie_df['user_review_count'] = movie_df['user_review_count']*1000
            
            movie_meta_data_df['release_year'] = movie_meta_data_df['release_year'].astype(int)
            
            
            # To store data into csv file, we need to convert dataframes into string file - For that w use StrinIO
            
            movie_key = "transformed_data/movie_data/movie_transformed_" + str(datetime.now()) + ".csv"
            movie_meta_data_key = "transformed_data/movie_meta_data/movie_meta_data_transformed_" + str(datetime.now()) + ".csv"
            
            # Creating StringIO object
            movie_buffer = StringIO()
            
            # converting the dataframe into string like object
            movie_df.to_csv(movie_buffer, index=False)
            
            # Getting the content from converted movie_buffer string like object
            movie_content = movie_buffer.getvalue()
            
            # Saving this in S3
            client.put_object(
                Bucket = Bucket,
                Key = movie_key,
                Body = movie_content
                )
            
            # Repeating same steps for movie_meta_data
            movie_meta_data_buffer = StringIO()
            
            movie_meta_data_df.to_csv(movie_meta_data_buffer, index = False)
            
            movie_meta_data_content = movie_meta_data_buffer.getvalue()
            
            client.put_object(
                Bucket = Bucket,
                Key = movie_meta_data_key,
                Body = movie_meta_data_content
                )
                
            
    for file_key in file_key_list:
        # Copying data from to_processed to processed
            
        # Creating boto3 resource object - we create this when we wanted to work on resource level
        s3_resource = boto3.resource('s3')
        
        # creating a dictionary variable for the source from where files will be copied
        copy_source = {
            'Bucket' : Bucket,
            'Key' : file_key
        }
        
        # Copying files from copy source to specified bucket and key
        s3_resource.meta.client.copy(copy_source, Bucket, "raw_data/processed/" + file_key.split('/')[-1])
        
        # Deleting the file which we have copied from to_process folder
        s3_resource.Object(Bucket, file_key).delete()
    
    

