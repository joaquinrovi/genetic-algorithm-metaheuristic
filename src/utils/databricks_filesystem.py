import os
from shutil import copyfile

import pandas as pd

class DatabricksFileSystem:
    """This is a conceptual class representation of Databricks' filesystem interaction using pandas.

    :Usage:

    from utils.databricks_filesystem import DatabricksFileSystem

    dbfs = DatabricksFileSystem()
    dbfs.create(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx', df=my_dataframe)
    df = dbfs.read(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx')

    """

    @staticmethod
    def create(dbfs_path: str, df: pd.DataFrame):
        """Writes a DataFrame to a specified path in Databricks' filesystem.

        :param dbfs_path: The path in Databricks' filesystem to write the DataFrame.
        :type dbfs_path: str
        :param df: The DataFrame to write.
        :type df: pd.DataFrame

        :Usage:

        dbfs.create(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx', df=my_dataframe)
        """

        tmp_dbfs_path = f"/local_disk0/tmp/{dbfs_path.split('/')[-1]}"
        df.to_excel(tmp_dbfs_path, index=False)

        copyfile(tmp_dbfs_path, dbfs_path)

    @staticmethod
    def read(dbfs_path: str):
        """Reads a file from a specified path in Databricks' filesystem into a DataFrame.

        :param dbfs_path: The path in Databricks' filesystem from which to read the file.
        :type dbfs_path: str
        :return: The DataFrame containing the file's data.
        :rtype: pd.DataFrame

        :Usage:

        df = dbfs.read(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx')
        """
        return pd.read_excel(dbfs_path)

    @staticmethod
    def update(dbfs_path: str, df: pd.DataFrame):
        """Overwrites an existing file in Databricks' filesystem with a DataFrame.

        :param dbfs_path: The path in Databricks' filesystem of the file to overwrite.
        :type dbfs_path: str
        :param df: The DataFrame to write.
        :type df: pd.DataFrame

        :Usage:

        dbfs.update(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx', df=my_updated_dataframe)
        """
        tmp_dbfs_path = f"/local_disk0/tmp/{dbfs_path.split('/')[-1]}"
        df.to_excel(tmp_dbfs_path, index=False)

        copyfile(tmp_dbfs_path, dbfs_path)

    @staticmethod
    def delete(dbfs_path: str):
        """Deletes a file from a specified path in Databricks' filesystem.

        :param dbfs_path: The path in Databricks' filesystem of the file to delete.
        :type dbfs_path: str

        :Usage:

        dbfs.delete(dbfs_path='/dbfs/FileStore/my_dataframe.xlsx')
        """
        if os.path.isfile(dbfs_path):
            os.remove(dbfs_path)
        else:
            print("Error: %s file not found" % dbfs_path)
