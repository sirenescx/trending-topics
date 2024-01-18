# Trending Topics

This repository contains code for the web application showing trending words and phrases from world and Russian news

### Implementation details

#### 1. Regular data fetching
**Tech stack:** Python, Airflow, Hdfs, Spark

The latest news data is taken from rss sources of news sites every hour. 
Fetched fields:
* **source:** the name of the newsletter author (or the host from which the newsletter was taken, if the author's name is not presented)
* **title:** title of the newsletter
* **published:** date of the newsletter publishing
* **summary:** summary of the newsletter content
* **tag:** keywords for the newsletter
Rss files are parsed using feedparser, then pre-processed and uploaded to hdfs in parquet format. Hourly runs are accomplished using Airflow dags.

#### 2. Regular statistics calculation
**Tech stack:** Python, Airflow, Hdfs, Spark

Every hour and 10 minutes (10 minutes is enough time to acquire, record, and process new data), a second dag is run to compute the frequency of occurrence of unigrams, bigrams, and trigrams in the textual news data (title, summary, tag). The processed data (dictionary of the form n-gram - frequency of occurrence) is written to a json file. 

#### 3. UI
**Tech stack:** Python, Flask, WordCloud

User interface is pretty simple – a single page application showing cloud of trending phrases on the news. A Flask application reads json file with statistics computed in [2] and 