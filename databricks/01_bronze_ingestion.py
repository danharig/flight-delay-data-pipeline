# Databricks notebook source
# 1. Confirm Azure Data Lake access

storage_path = "abfss://flight-data@flightdelay.dfs.core.windows.net/"

files = dbutils.fs.ls(storage_path)
display(files)

print("Azure storage connection verified.")

# COMMAND ----------

# 2. Download and extract the May 2026 TranStats file on the Databricks driver


import os
import requests
import zipfile

download_dir = "/tmp/transtats"
extract_dir = os.path.join(download_dir, "2026_05")

os.makedirs(download_dir, exist_ok=True)
os.makedirs(extract_dir, exist_ok=True)

url = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_5.zip"
)

zip_name = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_5.zip"
zip_path = os.path.join(download_dir, zip_name)

print("Downloading TranStats file...")

response = requests.get(url, timeout=120)
response.raise_for_status()

# ZIP files begin with the PK signature.
if not response.content.startswith(b"PK"):
    raise ValueError(
        "TranStats response does not look like a ZIP file. "
        "The site may have returned an HTML/error page."
    )

with open(zip_path, "wb") as f:
    f.write(response.content)

print(f"Downloaded: {zip_path}")
print(f"Size: {os.path.getsize(zip_path):,} bytes")

if not zipfile.is_zipfile(zip_path):
    raise ValueError("Downloaded file is not a valid ZIP archive.")

print("ZIP verified successfully.")

with zipfile.ZipFile(zip_path, "r") as z:
    print("Files inside ZIP:")
    for name in z.namelist():
        print(" -", name)

    z.extractall(extract_dir)

print(f"Extracted to: {extract_dir}")

# COMMAND ----------

# 3. Locate the extracted CSV

csv_files = [
    os.path.abspath(os.path.join(extract_dir, file_name))
    for file_name in os.listdir(extract_dir)
    if file_name.lower().endswith(".csv")
]

if not csv_files:
    raise FileNotFoundError(
        f"No CSV file was found in {extract_dir}"
    )

if len(csv_files) > 1:
    print("Multiple CSV files found. The first one will be used:")
    for path in csv_files:
        print(" -", path)

csv_path = csv_files[0]

print("CSV located:")
print(csv_path)


# COMMAND ----------

# 4. Save the raw CSV directly into Azure Data Lake

raw_dir = (
    "abfss://flight-data@flightdelay.dfs.core.windows.net/"
    "raw/transtats/2026/05"
)

raw_csv_path = (
    raw_dir
    + "/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_5.csv"
)

dbutils.fs.mkdirs(raw_dir)

# Read the local CSV with Python, then write it to ADLS
with open(csv_path, "rb") as src:
    file_bytes = src.read()

dbutils.fs.put(
    raw_csv_path,
    file_bytes.decode("utf-8"),
    overwrite=True
)

print("Raw CSV copied to Azure Data Lake:")
print(raw_csv_path)

display(dbutils.fs.ls(raw_dir))

# COMMAND ----------

# 5. Read the Azure raw file into the Bronze Spark DataFrame

bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_csv_path)
)

print(f"Bronze rows: {bronze_df.count():,}")
print(f"Bronze columns: {len(bronze_df.columns)}")

bronze_df.printSchema()
display(bronze_df.limit(20))

# COMMAND ----------

#Create/Read Bronze Delta Table from the ADLS path.
raw_csv_path = (
    "abfss://flight-data@flightdelay.dfs.core.windows.net/"
    "raw/transtats/2026/05/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_5.csv"
)

bronze_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_csv_path)
)



# COMMAND ----------

# 6. Write the Bronze Delta table to the correct Unity Catalog location

bronze_table = (
    "flight_delay_databricks.flight_delay.bronze_flights"
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze Delta table written successfully: {bronze_table}")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN `flight_delay_databricks`.flight_delay
""").show(truncate=False)

# COMMAND ----------

print(
    spark.table(
        "flight_delay_databricks.flight_delay.bronze_flights"
    ).count()
)

# COMMAND ----------

bronze_df = spark.table(
    "`flight-delay_databricks`.flight_delay.bronze_flights"
)