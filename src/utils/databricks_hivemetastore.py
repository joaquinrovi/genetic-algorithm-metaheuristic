import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class DatabricksHive:
    """
    This class provides methods for creating, reading, updating and deleting tables in Databricks' HiveMetastore.
    It works with pandas DataFrames by converting them to Spark DataFrames first.

    Usage:
    from utils.databricks_hivemetastore import DatabricksHive

    spark = SparkSession.builder.getOrCreate()
    db_hive = DatabricksHive(spark)
    db_hive.create(df=my_dataframe, db_name="my_database", table_name="my_table")
    df = db_hive.read(db_name="my_database", table_name="my_table")
    db_hive.update(df=my_updated_dataframe, db_name="my_database", table_name="my_table")
    db_hive.delete(db_name="my_database", table_name="my_table")
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark


    def create(self, df: pd.DataFrame, db_name: str, table_name: str, mode: str = "overwrite"):
        """
        This function saves a pandas DataFrame in a specific database as a table.

        :param df: The pandas DataFrame to be saved.
        :type df: pd.DataFrame
        :param db_name: The name of the database where the table will be saved.
        :type db_name: str
        :param table_name: The name of the table to be created.
        :type table_name: str
        :param mode: This defines the saving method of the table. It can be "overwrite" to overwrite the existing table,
                     "append" to add data to the existing table or "ignore" to keep the original table if it already exists.
                     The default is "overwrite".
        :type mode: str
        :return: None
        """

        # Convert pandas DataFrame to Spark DataFrame
        data_dict = df.to_dict("records")
        rdd = self.spark.sparkContext.parallelize(data_dict)

        schema = StructType([StructField(col, StringType(), True) for col in df.columns])
        spark_df = self.spark.createDataFrame(rdd, schema)

        # Create table
        self.spark.sql(f"USE {db_name}")
        spark_df.write.mode(mode).saveAsTable(table_name)

        print(f"The table {table_name} has been successfully created in the database {db_name}.")


    def read(self, db_name: str, table_name: str) -> pd.DataFrame:
        """
        This function reads a table from a specified database into a pandas DataFrame.

        :param db_name: The name of the database from which the table will be read.
        :type db_name: str
        :param table_name: The name of the table to be read.
        :type table_name: str
        :return: The DataFrame containing the table's data.
        :rtype: pd.DataFrame
        """
        self.spark.sql(f"USE {db_name}")
        spark_df = self.spark.read.table(table_name)

        # Convert Spark DataFrame to pandas DataFrame
        df = spark_df.toPandas()

        return df

    def delete(self, db_name: str, table_name: str):
        """
        This function deletes a table from a specified database.

        :param db_name: The name of the database from which the table will be deleted.
        :type db_name: str
        :param table_name: The name of the table to be deleted.
        :type table_name: str
        :return: None
        """
        # spark.sql(f"USE {db_name}")

        if self.spark.catalog.tableExists(f"{db_name}.{table_name}"):
            self.spark.sql(f"DROP TABLE {db_name}.{table_name}")
            print(f"The table {table_name} has been dropped from the database {db_name}.")
        else:
            print(f"The table {table_name} does not exist in the database {db_name}.")
