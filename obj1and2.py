# CS 436 Big Data
# Jimson Huang, Carl Antiado, Evan Lancaster
# Objective 2

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import os

# Find all the data files in the data directory and add it to a list
main_list = []
for subdir in os.scandir("data/main"):
    if not subdir.is_file():
        for file in os.scandir(subdir):
            if file.is_file() and file.name.endswith("txt") and not file.name.startswith("log"):
                # Limit to 100 files due to computational limitations
                if len(main_list) < 100:
                    main_list.append(file.path)

# Find all the size data files in the data directory and add it to a list
size_list = []
for subdir in os.scandir("data/size"):
    if not subdir.is_file():
        for file in os.scandir(subdir):
            if file.is_file() and file.name.endswith("txt") and file.name.startswith("size"):
                # Read input file as rdd
                size_list.append(file.path)

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Read data files as rdd
rdd = spark.read.text(main_list).rdd
# Read size data files as separate rdd
rdd_size = spark.read.text(size_list).rdd


# Split a row by \t into its attributes, and group all related videos into a single array
def split_entry(row):
    # Split line by \t
    columns = row.value.split("\t")
    # Fix for dirty data: if not all of the attributes are present, just return nulls to be discarded
    if len(columns) < 9:
        return [None] * 10
    # Add the first 9 columns into the output list
    out = columns[:9]
    # Store everything past the 9th column as a single list of related videos
    related = columns[9:]
    # Add the list to the end of the list of outputs
    out.append(related)
    return out


# Split rdd using above function
rdd = rdd.map(lambda k: split_entry(k))
rdd_size = rdd_size.map(lambda k: k.value.split("\t"))

# Create df from rdd
df = spark.createDataFrame(data=rdd, schema=["id", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments", "related_ids"]).dropna() # Remove rows with null values
# Change column types to their appropriate type
df = df.withColumn("age", df["age"].cast("int"))
df = df.withColumn("length", df["length"].cast("int"))
df = df.withColumn("views", df["views"].cast("int"))
df = df.withColumn("rate", df["rate"].cast("float"))
df = df.withColumn("ratings", df["ratings"].cast("int"))
df = df.withColumn("comments", df["comments"].cast("int"))

# Do the same thing with the size data rdd
df_size = spark.createDataFrame(data=rdd_size, schema=["id", "length", "size"]).dropna()
df_size = df_size.withColumn("length", df_size["length"].cast("int"))
df_size = df_size.withColumn("size", df_size["size"].cast("int"))

# Create the full df as a full outer join between the data df and the size df
full_df = df.join(df_size, ["id", "length"], "full_outer")

# Use SQL queries to get results needed for the project objective 2
full_df.createOrReplaceTempView("table")

# Give user options to choose from
while True:
    print("Menu:")
    print("1. Get videos from a category within a range of lengths")
    print("2. Get videos from a category within a range of ages")
    print("3. Get all videos within a range of sizes")
    print("4. Get the top k most popular categories")
    print("5. Get the top k of a sorted column")
    print("0. Quit")
    choice = int(input("Please select your intended action: "))

    if choice == 1:
        cat = input("Enter category: ")
        lower = input("Enter lower bound: ")
        upper = input("Enter upper bound: ")

        # Get videos from a category of length within a certain range
        spark.sql("SELECT * FROM table WHERE category = \'{}\' AND length BETWEEN {} AND {}".format(cat, lower, upper)).show()
    elif choice == 2:
        cat = input("Enter category: ")
        lower = input("Enter lower bound: ")
        upper = input("Enter upper bound: ")

        # Get videos from a category of age within a certain range
        spark.sql("SELECT * FROM table WHERE age = \'{}\' AND length BETWEEN {} AND {}".format(cat, lower, upper)).show()
    elif choice == 3:
        lower = input("Enter lower bound: ")
        upper = input("Enter upper bound: ")

        # Get videos of size within a certain range
        spark.sql("SELECT * FROM table WHERE size BETWEEN {} AND {}".format(lower, upper)).show()
    elif choice == 4:
        k = int(input("Enter k: "))

        # Count how many videos are in each category, sort by that
        full_df.groupby("category").count().sort(desc("count")).dropna().show(k)
    elif choice == 5:
        cat = input("Enter a column name: ")
        k = int(input("Enter k: "))

        # Sort by a category then show the top k
        df.sort(desc(cat)).dropna().show(k)
    else:
        break



