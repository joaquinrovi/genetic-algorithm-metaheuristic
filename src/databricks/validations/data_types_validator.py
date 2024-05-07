import pandas as pd
import plotly.graph_objects as go

# !pip install attrs --upgrade  , Install in databricks first

class DataTypesValidator:
    """
    The DataTypesValidator class provides methods to validate data types of Spark tables based on a given configuration.

    Usage:
    validator = DataTypesValidator()
    tables_results, discrepancies_list = validator.validate_tables_with_config(tables_config)
    """

    @staticmethod
    def get_spark_types(db_name: str, table_name: str) -> dict:
        """
        Retrieves data types of all columns in a Spark table.

        :param db_name: Name of the database.
        :type db_name: str
        :param table_name: Name of the table.
        :type table_name: str
        :return: Dictionary with column names as keys and their data types as values.
        :rtype: dict
        """
        df_spark = spark.sql(f"SELECT * FROM {db_name}.{table_name}")
        return {column.name: str(column.dataType) for column in df_spark.schema}

    @staticmethod
    def compare_data_types(spark_types: dict, config_types: dict) -> dict:
        """
        Compares the data types of columns from a Spark table with a given configuration.

        :param spark_types: Dictionary containing column names and their data types from Spark.
        :type spark_types: dict
        :param config_types: Dictionary containing column names and their expected data types.
        :type config_types: dict
        :return: Dictionary with columns that don't match the expected data type.
                 Each entry has the column name as key and a tuple (expected data type, actual data type) as value.
        :rtype: dict
        """
        discrepancies = {}
        for column, spark_type in config_types.items():
            if column in spark_types:
                if spark_type != spark_types[column]:
                    discrepancies[column] = (spark_type, spark_types[column])
        return discrepancies

    def process_tables(self, tables_config_section, config_types):
        """
        Processes tables based on the provided configuration section.

        :param tables_config_section: Configuration section containing table setups.
        :type tables_config_section: dict
        :param config_types: Dictionary containing column names and their expected data types.
        :type config_types: dict
        :return: Tuple containing a list of validation results and a list of discrepancies.
        :rtype: (list, list)
        """
        results = []
        discrepancies_list = []

        for _, config_value in tables_config_section.items():
            for setup in config_value["setup"]:
                db_name = setup["db_name"]
                table_name = setup["table_name"]

                spark_types = self.get_spark_types(db_name, table_name)
                discrepancies = self.compare_data_types(spark_types, config_types)

                total_columns = len(spark_types)
                non_matching_columns = len(discrepancies)
                match_percentage = ((total_columns - non_matching_columns) / total_columns) * 100

                results.append({
                    "Table": table_name,
                    "match_percentage": match_percentage,
                    "total_columns": total_columns,
                    "non_matching_columns": non_matching_columns
                })

                for column, (config_type, actual_type) in discrepancies.items():
                    discrepancies_list.append({
                        "Table": table_name,
                        "Column": column,
                        "Config Type": config_type,
                        "Actual Type": actual_type
                    })

        return results, discrepancies_list

    def validate_tables_with_config(self, tables_config: dict) -> (list, list):
        """
        Validates data types of tables based on a provided configuration.

        :param tables_config: Configuration dictionary containing paths to type definitions and table setups.
        :type tables_config: dict
        :return: Tuple containing a list of validation results and a list of discrepancies.
        :rtype: (list, list)
        """
        data_types_config = tables_config["data_types_config"]
        df_config_type = pd.read_excel(data_types_config["dbfs_path"])
        config_types = df_config_type.set_index('COLUMNAS')['SPARK_TYPE'].to_dict()

        all_results = []
        all_discrepancies_list = []

        # Process raw_files_read_config
        results, discrepancies_list = self.process_tables(tables_config["raw_files_read_config"], config_types)
        all_results.extend(results)
        all_discrepancies_list.extend(discrepancies_list)

        # Process metastore_tables_config
        results, discrepancies_list = self.process_tables(tables_config["metastore_tables_config"], config_types)
        all_results.extend(results)
        all_discrepancies_list.extend(discrepancies_list)

        return all_results, all_discrepancies_list


def visualize_data_match(tables_results: list):
    """
    This function visualizes the percentage of matched data types for each table.

    The visualization displays a bar chart where each bar corresponds to a table.
    The height of the bar indicates the percentage of matched data types,
    and text inside the bar provides additional information about the match and mismatch count.

    :param tables_results: A list of dictionaries. Each dictionary should have keys:
                           "Table", "match_percentage", "total_columns", and "non_matching_columns".
    :type tables_results: list

    Usage:
    visualize_data_match(tables_results)
    """

    table_names = [res["Table"] for res in tables_results]
    match_percentages = [res["match_percentage"] for res in tables_results]
    total_columns = [res.get("total_columns", 0) for res in tables_results]
    non_matching_columns = [res.get("non_matching_columns", 0) for res in tables_results]

    text = [f"{mp:.2f}%\nMatch: {total - non_match}\nMismatch: {non_match}"
            for mp, total, non_match in zip(match_percentages, total_columns, non_matching_columns)]

    fig = go.Figure(data=[
        go.Bar(
            x=table_names,
            y=match_percentages,
            text=text,
            textposition='inside',
            textangle=-45,
            textfont=dict(size=12, color='yellow'),
            insidetextanchor='middle',
            marker_color='darkgreen'
        )
    ])
    fig.update_layout(
        title_text='Percentage of Correct Data Types per Table',
        title_font=dict(color='darkgreen'),
        plot_bgcolor='#ccffcc',
        paper_bgcolor='#ccffcc'
    )
    fig.show()


def visualize_discrepancies(discrepancies_list: list):
    """
    This function visualizes the discrepancies between the expected and actual data types of columns for various tables.

    The visualization displays a table where each row corresponds to a discrepancy.
    The columns of this table display the table name, column name, expected data type, and the actual data type.

    :param discrepancies_list: A list of dictionaries. Each dictionary should have keys:
                               "Table", "Column", "Config Type", and "Actual Type".
    :type discrepancies_list: list

    Usage:
    visualize_discrepancies(discrepancies_list)
    """

    table_names = [res["Table"] for res in discrepancies_list]
    columns = [res["Column"] for res in discrepancies_list]
    config_types = [res["Config Type"] for res in discrepancies_list]
    actual_types = [res["Actual Type"] for res in discrepancies_list]

    fig = go.Figure(data=[
        go.Table(
            header=dict(
                values=["Table", "Column", "Config Type", "Actual Type"],
                fill_color="darkgreen",
                font=dict(color="yellow", size=13)
            ),
            cells=dict(
                values=[table_names, columns, config_types, actual_types],
                fill_color=[["#f5f5f5", "white"] * len(discrepancies_list)],
                font=dict(color="darkgreen", size=12)
            )
        )
    ])
    fig.show()


validator = DataTypesValidator()
tables_results, discrepancies_list = validator.validate_tables_with_config(tables_config)
visualize_data_match(tables_results)
if discrepancies_list:
    visualize_discrepancies(discrepancies_list)
