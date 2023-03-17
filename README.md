# Flights EDA

This project involved constructing a data pipeline that utilizes PySpark DataFrame API to perform exploratory data analysis. While developing the pipeline, care was taken to ensure that the order of query execution was optimal for performance, and the queries were fine-tuned to achieve the best results.

To manage the massive datasets, including the largest one containing over 10 million rows, AWS S3 was employed for storage, and AWS EMR was used to run Apache Spark and manage the Spark clusters.

## Tasks

The first task was to evaluate Top 3 aircrafts where the manufacturer is "Cessna". The output should be in the form `<model> <count>`.

The second task was to calculate the average flight delay given a year.

And the third task was to evaluate the most popular aircraft types and output the top 5 for each company.

## Files

The whole analysis process can be visualized in the jupyter notebook file - `flights_analysis.ipynb`; which was initially used to experiment on the data and get a good understanding of it.

`src/main.py` contains the final source code.

## Schema

![flights_schema.png](flights_schema.png)
