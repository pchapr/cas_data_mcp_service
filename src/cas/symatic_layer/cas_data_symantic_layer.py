import logging
import pandas as pd
import pandasai as pai
from pandasai.config import Config
import logging
from langchain_community.llms import Ollama
import os
llm = Ollama(model="mistral")

def start_llm_chat(file_path):
    logging.debug(f"Reading CAS file: {file_path}")    
    # Read the parquet file as pandas dataframe
    cas_dataframe = pd.read_parquet(file_path)
    #write cas_dataframe as delimited file using pandas
    cas_dataframe.to_csv("data.csv", sep=",", index=False) 
    ollama_llm = Ollama(model="codellama:latest") 
    print(f"Ollama model loaded {ollama_llm}")
    # config = pai.config.set({"llm": ollama_llm, 
    #                          "code_executor": "python"})
    smart_dataframe = pai.SmartDataframe(cas_dataframe, config={"llm": ollama_llm, "code_executor": "python", "enable_logging": False})
    #smart_dataframe = pai.SmartDataframe(cas_dataframe, config=config)
    response = smart_dataframe.chat("Plot Original Interest Rate distribution") 
    logging.info(response)
    os.remove("data.csv")
    logging.info("Data processing completed successfully.")

if __name__ == "__main__":
    # Configure logging to display INFO level messages on the console
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting cas_data_symantic_layer")
    # Initialize pandas AI with Ollama LLM
    start_llm_chat('data/OCT_2024/CAS 2024 R04 G1.parquet')