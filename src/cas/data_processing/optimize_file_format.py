import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_csv_to_parquet(input_delimited_file, output_parquet_file_location, delimiter=",", header='infer'):
    """
    Convert a CSV file to Parquet format.
    Args:
        input_delimited_file (str): Path to the input CSV file.
        output_parquet_file (str): Path to the output Parquet file.
        delimeter (str): Delimiter used in the CSV file. Default is comma (,).
        header (str): Row number(s) to use as the column names. Default is 'infer'.
    """
    logging.info(f"Converting {input_delimited_file} to Parquet format...")
    try:
        #read the csv file
        df = pd.read_csv(input_delimited_file, delimiter=delimiter, header=header, low_memory=False)
        logging.info(df.head())

        #read the Deal Name from dataframe and for each deal name save a new file in parquet format
        if 'Deal Name' not in df.columns:
            raise ValueError("Deal Name column not found in the CSV file.")
        for deal_name in df['Deal Name'].unique():
            deal_df = df[df['Deal Name'] == deal_name]
            logging.info(f"Processing for deal: {deal_name}")
            #logging.info(f"Column Types: {deal_df.dtypes['Special Eligibility Program']}")
            #logging.info(f"Column values: {print(df['Special Eligibility Program'].unique())}")
            #logging.info(f"Columns: {deal_df.columns}")
            deal_df.to_parquet(f"{output_parquet_file_location}/{deal_name}.parquet", engine='pyarrow')
            logging.info(f"File saved: {output_parquet_file_location}/{deal_name}.parquet")
        logging.info("Deal Files saved successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {input_delimited_file}")
    except ValueError as ve:
        #print full stacktrace and detailed exception message
        import traceback
        logging.error("ValueError occurred", exc_info=True)
        logging.error(f"ValueError: {ve}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    #check if the file is saved

if __name__ == "__main__":
    logging.info("Starting to process cas-data-mcp-service!")
    convert_csv_to_parquet("data/OCT_2024/CAS_Oct24.csv", "data/OCT_2024", '|', header=0)