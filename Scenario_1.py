from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import requests
import os
from datetime import datetime

base_path = os.getcwd()
today_date = datetime.now().strftime("%Y-%m-%d")

class TransactionPipeline:
    def __init__(self, raw_path: str, clean_path: str, quarantine_path: str, webhook_url: str = None):
        self.spark = SparkSession.builder \
            .appName("MyPySparkApplication2") \
            .master("local[*]") \
            .getOrCreate()
        

        self.raw_path = raw_path
        self.clean_path = clean_path
        self.quarantine_path = quarantine_path
        self.webhook_url = webhook_url

    def read_raw_data(self) -> DataFrame:
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("treatEmptyValuesAsNulls", "true") \
            .csv(self.raw_path)

        return df

    def validate_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        valid_df = df.filter(
            col("transaction_amount").isNotNull() &
            col("customer_id").isNotNull()
        ).dropDuplicates(["transaction_id"])
        
        bad_df = df.subtract(valid_df)
        return valid_df, bad_df

    def write_valid_data(self, valid_df: DataFrame):
        valid_df.write.mode("append").format("csv").save(self.clean_path)

    def write_invalid_data(self, bad_df: DataFrame):
        if bad_df.count() > 0:
            bad_df.write.mode("append").format("csv").save(self.quarantine_path)

    def send_alert(self, count: int):
        if self.webhook_url and count > 0:
            payload = {"bad_record_count": count}
            try:
                requests.post(self.webhook_url, json=payload)
                print(f"Alert sent for {count} bad records.")
            except Exception as e:
                print(f"Failed to send alert: {e}")

    def run(self):
        print("📥 Reading raw data...")
        raw_df = self.read_raw_data()

        print("🧹 Validating data...")
        valid_df, bad_df = self.validate_data(raw_df)

        print("✅ Writing valid records...")
        self.write_valid_data(valid_df)

        print("🚫 Writing invalid records...")
        self.write_invalid_data(bad_df)

        print("🔔 Sending alerts if needed...")
        self.send_alert(bad_df.count())

        print("✅ Pipeline completed successfully.")


raw_path = f"{base_path}/data/transactions.csv"
clean_path = f"{base_path}/data/output/{today_date}/transactions_out.csv"
quarantine_path = f"{base_path}/data/bad_data/{today_date}/transactions_invalid.csv"
webhook_url = "https://<your-logic-app-or-slack-webhook>"

pipeline = TransactionPipeline(raw_path, clean_path, quarantine_path, webhook_url)
pipeline.run()
