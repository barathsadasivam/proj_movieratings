# proj_movieratings

This repository contains a Python script for extracting movie ratings data from DAT files and performing data analysis using PySpark.

Description
-----------
The Python script movie_ratings_analysis.py reads movie ratings and movies data from dat files, processes the data, and performs various analyses:

- Validates the schema of the input files against the schema files
- Calculates statistics per MovieID (maximum, minimum, and average ratings)
- Finds top 3 movies per user based on ratings
- Writes the original and processed data to Parquet format for further analysis

File Descriptions
-----------------
- `movie_ratings_analysis.py`: The main Python script responsible for extracting, processing, and analyzing movie ratings data using PySpark. It reads configuration files, performs data operations, and generates output files.
- `config.json`: Configuration file containing file paths for input and output data.
- `ratings_schemafile.json`: JSON file defining the schema for ratings data.
- `movies_schemafile.json`: JSON file defining the schema for movies data.
- `output_files/`: Directory containing output Parquet files.
- `testcases.ipynb`: A Jupyter Notebook containing code for few test cases and their expected results. It validates the functionality and correctness of 'movie_ratings_analysis.py' by comparing expected versus actual outputs.

Execution
---------
To execute the script, use the following command:

# for running Spark in local mode

spark-submit \
--master local[*] \  # Utilize all available cores
--executor-memory 2G \  # Allocate memory per executor
--driver-memory 2G \  # Memory for the driver
movie_ratings_analysis.py

# for running Spark in a cluster environment

spark-submit \
--master yarn \  # Set the cluster manager to YARN
--deploy-mode cluster \  # Deploy mode (cluster or client)
--num-executors 5 \  # Number of executors
--executor-cores 2 \  # Cores per executor
--executor-memory 1G \  # Memory per executor
--driver-memory 2G \  # Memory for the driver
movie_ratings_analysis.py


Make sure to update the config.json file with the appropriate file paths before running the script.