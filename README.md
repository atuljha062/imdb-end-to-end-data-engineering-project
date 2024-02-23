# IMDb End-To-End Data Engineering Project

### Introduction
In this project, we've developed a comprehensive data pipeline on Amazon Web Services (AWS) utilizing data extracted from IMDB via web scraping, specifically targeting the top 100 movies. The pipeline efficiently extracts a wide range of movie-related information, including titles, ratings, genres, director, and more, undergoes tailored transformations to ensure data integrity and compatibility, and then seamlessly loads the refined data into an AWS S3. Our focus remains on optimizing data processing while upholding security and accessibility standards within the AWS environment, enabling robust analytics and decision-making capabilities for the top 100 movies dataset and beyond.

### Archirecture
![Architecture Diagram](https://github.com/atuljha062/imdb-end-to-end-data-engineering-project/blob/main/imdb-end-to-end-data-engineering-project-diagram.jpg)

### About Data

I have extracted data from the IMDb website's "Top 250 Movies" chart using the Python programming language, specifically leveraging the **requests** library to send HTTP requests to the webpage and the **BeautifulSoup** library to parse and extract relevant information from the HTML content retrieved. This process allows for the retrieval of various data points such as movie titles, ratings, release years, directors, and genres, providing a comprehensive dataset for further analysis and processing. This extracted data can be utilized for a variety of purposes, including statistical analysis, visualization, and machine learning applications within the domain of film and entertainment.

### Data Model
![Data Model Diagram](https://github.com/atuljha062/imdb-end-to-end-data-engineering-project/blob/main/imdb_data_model.jpg)

### Services Used
1. **AWS S3 (Simple Storage Service):** Amazon S3 (Simple Storage Service) is a highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups and static website files.
  
2. **AWS Lambda:** AWS Lambda is a serverless computing service that let you run your code without managing servers. You can use Lambda to run code in response to events like change in S3, DynamoDB, or other AWS services.
   
3. **AWS CloudWatch:** Amazon CloudWatch is a monitoring service for AWS resources and the applications you run on them. You can use CloudWatch to collect and track metrics, collect and monitor log files, and set alarms.

4. **Glue Crawler:** AWS Glue Crawler is a fully managed service that automatically crawls your data sources, identifies data formats, and infers schemas to create an AWS Glue Data Catalog.

5. **AWS Glue Data Catalog:** AWS Glue Data Catalog is a fully managed metadata repository that makes it easy to discover and manage data in AWS. You can use the Glue Data Catalog with other AWS services, such as Athena.

6. **Amazon Athena:** Amazon Athena is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL. You can use Athena to analyze data in your Glue Data Catalog or in other S3 buckets.


### Installed Packages
```
pip install pandas
pip install numpy
pip install requests
pip install bs4
```

### Project Execution Flow
Lambda Trigger (every week on Friday) -> Run imdb_data_extraction_function code (Extract Data with Python using requests and BeautifulSoup from IMDb website) -> Stores Raw Data in AWS S3 -> Trigger Transform Fuction -> Run imdb_tranformation_load_function code (Transforms the data) -> Stores Transformed Data in AWS S3 -> AWS Glue Crawler runs on the Transformed Data and Generated AWS Glue Data Catalog -> Query the Data using Athena
