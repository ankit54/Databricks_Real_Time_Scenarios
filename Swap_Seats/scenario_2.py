from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, collect_list, explode, struct, lit, when
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import udf
import os
from datetime import datetime

base_path = os.getcwd() + "/Scenario_2"
print(base_path)
today_date = datetime.now().strftime("%Y-%m-%d")

spark = SparkSession.builder \
            .appName("Scenario2") \
            .master("local[*]") \
            .getOrCreate()

df = spark.read.csv(f"{base_path}/data.csv", header=True, inferSchema=True)


# Step 1: Add row_number to ensure stable order
window_spec = Window.orderBy("Seat")
df_with_row = df.withColumn("row_num", row_number().over(window_spec))

# Step 2: Create a pairing group
df_with_group = df_with_row.withColumn("group_id", ((col("row_num") - 1) / 2).cast("int"))

# df_with_group.show()

# # Step 3: Collect names and seat numbers by group
df_grouped = df_with_group.groupBy("group_id").agg(collect_list(struct("Seat", "Name")).alias("pairs"))

# Step 4: Swap pairs
def swap_pair(pairs):
    if len(pairs) == 2:
        return [ (pairs[0]["Seat"], pairs[1]["Name"]), (pairs[1]["Seat"], pairs[0]["Name"]) ]
    else:
        # Odd record (no pair), return as is
        return [ (pairs[0]["Seat"], pairs[0]["Name"]) ]



schema = ArrayType(StructType([
    StructField("Seat", IntegerType(), True),
    StructField("Name", StringType(), True),
]))

swap_udf = udf(swap_pair, schema)
swapped_df = df_grouped.withColumn("swapped", swap_udf("pairs")).select(explode("swapped").alias("row"))


# # Step 5: Flatten to final format
final_df = swapped_df.select(col("row.Seat"), col("row.Name")).orderBy("Seat")

final_df.show()
