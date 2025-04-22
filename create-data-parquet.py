import pandas as pd
import numpy as np
import os

def generate_sample_parquet_data(output_dir="sample_parquet_data"):
    """
    Generate large-scale sample Parquet files with synthetic data for performance benchmarking.

    This function creates datasets ranging from 1 million to 10 million rows and saves them
    in Parquet format, which is optimized for analytics workloads such as those run on Apache Spark.

    @param output_dir: Directory where the generated Parquet files will be saved.
                       Default is 'sample_parquet_data'.
    @return: None. Parquet files are written to disk in the specified directory.
    """
    # Create a directory to save files
    os.makedirs(output_dir, exist_ok=True)

    # Define dataset sizes
    row_counts = [1_000_000, 2_000_000, 4_000_000, 6_000_000, 8_000_000, 10_000_000]

    # Generate and save Parquet files
    for num_rows in row_counts:
        print(f"Generating {num_rows} rows...")

        # Create sample data
        df = pd.DataFrame({
            "id": np.arange(1, num_rows + 1),
            "value": np.random.rand(num_rows),
            "category": np.random.choice(['A', 'B', 'C'], size=num_rows)
        })

        # Build file path
        file_name = f"sample_{num_rows // 1_000_000}M_rows.parquet"
        file_path = os.path.join(output_dir, file_name)

        # Save DataFrame to Parquet
        df.to_parquet(file_path, index=False)

        print(f"Saved: {file_path}")

    print("All Parquet files created in:", output_dir)

# Execute function
if __name__ == "__main__":
    generate_sample_parquet_data()
