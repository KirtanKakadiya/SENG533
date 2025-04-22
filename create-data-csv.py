import pandas as pd
import numpy as np
import os

def generate_sample_csv_data(output_dir="sample_csv_data"):
    """
    Generate large-scale sample CSV files with synthetic data for benchmarking purposes.

    This function creates datasets with 1M to 10M rows, simulating a realistic data
    structure for testing performance of data processing systems like Apache Spark.

    @param output_dir: Directory where the generated CSV files will be saved.
                       Default is 'sample_csv_data'.
    @return: None. CSV files are saved directly to the specified directory.
    """
    # Create a directory to save CSV files
    os.makedirs(output_dir, exist_ok=True)

    # List of dataset sizes in rows
    row_sizes = [1_000_000, 2_000_000, 4_000_000, 6_000_000, 8_000_000, 10_000_000]

    # Generate and save CSV files
    for num_rows in row_sizes:
        print(f"Generating {num_rows} rows...")

        # Create synthetic data
        df = pd.DataFrame({
            "id": np.arange(1, num_rows + 1),
            "value": np.random.rand(num_rows),
            "category": np.random.choice(['A', 'B', 'C'], size=num_rows)
        })

        # Build file name and path
        file_name = f"sample_{num_rows // 1_000_000}M_rows.csv"
        file_path = os.path.join(output_dir, file_name)

        # Save DataFrame to CSV
        df.to_csv(file_path, index=False)

        print(f"Saved: {file_path}")

    print(f"All CSV files saved in directory: {output_dir}")

# Call the function
if __name__ == "__main__":
    generate_sample_csv_data()
