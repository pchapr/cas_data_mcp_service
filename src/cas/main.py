from cas.data_processing import convert_csv_to_parquet

def main():
    print("Hello from cas-data-mcp-service!")
    convert_csv_to_parquet(
        "data/OCT_2024/CAS_Oct24.csv",
        "data/OCT_2024",
        delimiter="|",
        header=0
    )


if __name__ == "__main__":
    main()
