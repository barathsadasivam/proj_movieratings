# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, coalesce, lit
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import json

config_file_path = "/content/config.json"

# Create a SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Function to read CSV file with error handling
def read_csv_with_error_handling(file_path, schema):
    try:
        return spark.read.csv(file_path, sep='::', schema=schema)
    except Exception as e:
        print(f"Error reading file at '{file_path}': {e}")
        raise e  # Raise exception in case of an error

# Read configuration from JSON file
with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)

# Extract parameters from the config
ratings_path = config['file_paths']['ratings_data']
movies_path = config['file_paths']['movies_data']
original_ratings_path = config['file_paths']['original_ratings_targetformat']
original_movies_path = config['file_paths']['original_movies_targetformat']
movie_stats_data_path = config['file_paths']['movie_stats_data']
top_3_per_user_data_path = config['file_paths']['top_3_per_user_data']
ratings_schemafile_path = config['file_paths']['ratings_schemafile']
movies_schemafile_path = config['file_paths']['movies_schemafile']

# Load the schema file from the local path
with open(ratings_schemafile_path, 'r') as ratings_file:
    ratings_schema_data = json.load(ratings_file)

# Convert the schema data to StructType
ratings_schema = StructType.fromJson(ratings_schema_data)

with open(movies_schemafile_path, 'r') as movies_file:
    movies_schema_data = json.load(movies_file)

movies_schema = StructType.fromJson(movies_schema_data)

# Read CSV files with error handling
df_ratings = read_csv_with_error_handling(ratings_path, ratings_schema)
df_movies = read_csv_with_error_handling(movies_path, movies_schema)

# Replace any NULL in "Rating" column to 0
df_ratings = df_ratings.na.fill(0, subset=["Rating"])

# Check if files were successfully read before proceeding
if df_ratings and df_movies:
  
  # Schema validation checks
  if df_ratings.schema == ratings_schema:
      print("Ratings data schema matches the expected schema.")
  else:
      print("Ratings data schema does not match the expected schema.")
      raise Exception("Ratings data schema does not match the expected schema")

  if df_movies.schema == movies_schema:
      print("Movies data schema matches the expected schema.")
  else:
      print("Movies data schema does not match the expected schema.")
      raise Exception("Movies data schema does not match the expected schema")

try:
  # statistics per MovieID
    df_movie_stats = df_ratings.groupBy(df_ratings.MovieID) \
        .agg(
            F.max(df_ratings.Rating).alias("MaxRating"),
            F.min(df_ratings.Rating).alias("MinRating"),
            F.avg(df_ratings.Rating).alias("AvgRating")
        )

    # Left join movie data with aggregated statistics and handle missing values
    df_with_stats = df_movies.alias("movies").join(
      df_movie_stats.alias("stats"),
      col("movies.MovieID") == col("stats.MovieID"),
      "left_outer"
      ).select(
        col("movies.MovieID"),
        col("movies.Title"),
        col("movies.Genres"),
        coalesce(col("stats.MinRating"), lit(0)).alias("MinRating"),
        coalesce(col("stats.MaxRating"), lit(0)).alias("MaxRating"),
        coalesce(col("stats.AvgRating"), lit(0)).alias("AvgRating")
      )

    # window function and assign row number to each row within each partition
    window_spec = Window.partitionBy("UserID").orderBy(col("Rating").desc())
    df_with_row_numbers = df_ratings.withColumn("row_number", row_number().over(window_spec))

    # Select top 3 rows for each UserID ordered by 'row_number'
    df_top_3_per_user = df_with_row_numbers.where(col("row_number") <= 3).orderBy("UserID", "row_number")

    # Join dataframes to get the final desired output
    final_output = df_movies.join(df_top_3_per_user, df_movies.MovieID == df_top_3_per_user.MovieID) \
        .select(df_top_3_per_user.UserID, df_top_3_per_user.MovieID, df_movies.Title, df_top_3_per_user.Rating, df_top_3_per_user.row_number)

    # Write original and final DataFrames to Parquet format
    df_ratings.write.mode('overwrite').parquet(original_ratings_path)
    df_movies.write.mode('overwrite').parquet(original_movies_path)
    df_with_stats.write.mode('overwrite').parquet(movie_stats_data_path)
    final_output.write.mode('overwrite').parquet(top_3_per_user_data_path)
except Exception as e:
    print("Unknown exception occured - {}".format(str(e)))