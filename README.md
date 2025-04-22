# Spark Performance Benchmarking on AWS EMR

This project evaluates the performance of Apache Spark when processing large datasets in different file formats (CSV vs. Parquet) using AWS Elastic MapReduce (EMR). It aims to benchmark execution time and system resource utilization as the dataset scales from 1 million to 10 million rows.

## 📁 Project Structure

The script performs the following steps:

1. Reads a dataset in either CSV or Parquet format from S3.
2. Registers it as a temporary SQL view.
3. Performs a basic SQL transformation (can be customized).
4. Writes the result back to S3 in CSV format.
5. Measures and prints the script execution time.

You can then analyze this execution time alongside resource metrics (CPU, memory, idle time) from **AWS CloudWatch** and **EMR logs**.

## 🖥️ Requirements

- Python 3.x
- Apache Spark (tested on EMR)
- boto3 (optional, for interacting with AWS)
- Access to AWS S3 and EMR

## 📦 Usage

```bash
spark-submit benchmark_spark.py \
    --input_path s3://your-bucket/input_data.csv \
    --output_path s3://your-bucket/output_data/ \
    --file_format csv