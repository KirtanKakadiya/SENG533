import argparse
import time
from pyspark.sql import SparkSession

def benchmark_spark_job(input_path, output_path, file_format="csv"):
    """
    Benchmark the execution time of a Spark job reading from either CSV or Parquet format,
    performing a basic transformation, and writing results to output.

    :param input_path: S3 URI to the input dataset (e.g., 's3://bucket/dataset.csv' or '.parquet')
    :param output_path: S3 URI where the output will be saved (e.g., 's3://bucket/output/')
    :param file_format: Input file format, either 'csv' or 'parquet'
    """
    # Record start time (used for script execution benchmarking)
    start_time = time.time()

    # Initialize Spark session
    spark = SparkSession.builder.appName("Spark Performance Benchmark").getOrCreate()

    # Load dataset
    if file_format.lower() == "csv":
        df = spark.read.option("header", "true").csv(input_path)
    elif file_format.lower() == "parquet":
        df = spark.read.parquet(input_path)
    else:
        raise ValueError("Unsupported file format. Please use 'csv' or 'parquet'.")

    # Register as temporary SQL view
    df.createOrReplaceTempView("benchmark_data")

    # Example transformation (can be replaced with real logic)
    result_df = spark.sql("SELECT * FROM benchmark_data")

    # Write results to output path
    result_df.write.option("header", "true").mode("overwrite").csv(output_path)

    # Record end time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Script Execution Time: {execution_time:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Performance Benchmark Script")
    parser.add_argument('--input_path', required=True, help="S3 path to the input dataset.")
    parser.add_argument('--output_path', required=True, help="S3 path for output results.")
    parser.add_argument('--file_format', default='csv', choices=['csv', 'parquet'],
                        help="Format of the input file: 'csv' or 'parquet'.")

    args = parser.parse_args()
    benchmark_spark_job(args.input_path, args.output_path, args.file_format)
