import subprocess
import sys

def install_package(package_name):
    subprocess.call(["pip", "install", package_name])

install_package("unidecode")
install_package("numpy")
install_package("pygad")

import warnings

warnings.simplefilter(action="ignore", category=Warning)

import json
import os
import csv
from shutil import copyfile
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)
from pyspark.sql.functions import when, lit, col as spark_col
import numpy as np
import pandas as pd
import unidecode

from model.model import Model
from axis.axis import Axis
from output.values_goals import ValuesGoals
from data_processing.flujo_down import DownBase
from data_processing.flujo_mid import FlujoMid
from data_processing.flujo_union import DataProcessing
from data_processing.flujo_caja_utils import union_flujos, union_flujos_2, get_vpn_calculado
from utils.databricks_hivemetastore import DatabricksHive
from utils.logger import Logger
from utils.general_utils import read_json, auto_cast_df, get_configuration, get_configuration_model, try_float, check_units, get_run_params, spark_cast, save_table_real, save_capex_debt, get_colombian_time, import_model_results, save_types_union, save_db_name_csv

logger = Logger(__name__).get_logger()

main_config = read_json("config/main_config.json")
data_source_config = read_json("config/data_source_config.json")
model_config = get_configuration_model(main_config["config_path"], "config/model_config.json")
total_caja_config = get_configuration(main_config["config_path"], "config/fuente_config.json")

# spark = SparkSession.builder.appName("App Portafolio Model").getOrCreate()
spark = SparkSession.builder \
    .appName("App Modelo Portafolio") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:<alguna version de delta lake aqui>") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql(f"CREATE DATABASE IF NOT EXISTS {main_config['db_name']}")
db_hive = DatabricksHive(spark)


def measure_time(task_name, func, *args):
    """
    Executes a function, measures its execution time, and logs the start, end, and elapsed time.

    :param task_name: Name or description of the task being measured.
    :type task_name: str
    :param func: The function that needs its execution time to be measured.
    :type func: function
    :param *args: Variable length argument list of the function's parameters.
    :type *args: tuple
    :return: Returns what the input function returns.
    :rtype: varies based on the function
    """
    logger.info(f"{task_name} successfully started")
    start_time = time.time()

    result = func(*args)

    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    logger.info(f"{task_name} successfully finished, {elapsed_time:.2f} minutes")

    return result


def process_fc_up_be_isa_down(data_processing):
    """
    Processes the given data and resets its index.

    :param data_processing: An instance of a class with the fc_up_be_isa_down() method.
    :type data_processing: Object
    :return: DataFrame with reset index.
    :rtype: pd.DataFrame
    """
    df = data_processing.fc_up_be_isa_down()
    return df.reset_index(drop=True)


def run_model(model_config, params, df_union, df_down, df_mid, initial=False):
    """
    Runs a machine learning model using the provided configurations and data.

    :param model_config: Configuration parameters for the Model.
    :type model_config: dict
    :param df_union: DataFrame containing union data.
    :type df_union: pd.DataFrame
    :param df_base: DataFrame containing base data.
    :type df_base: pd.DataFrame
    :param params: Additional parameters for the model.
    :type params: dict
    :return: Results and parameters of the run model.
    :rtype: tuple(pd.DataFrame, pd.DataFrame)
    """
    model = Model(model_config, params, df_union, df_down, df_mid, initial)
    return model.run_generations()


def get_down_base(df_model_results, config, api_config):
    """
    Generates a base DataFrame based on model results and configuration.

    :param df_model_results: DataFrame containing results of a model run.
    :type df_model_results: pd.DataFrame
    :param config: Configuration parameters for DownBase.
    :type config: dict
    :return: DataFrame representing the down base with set column values.
    :rtype: pd.DataFrame
    """
    base = DownBase(df_model_results, config, api_config)
    df = base.get_base(False)
    df.loc[:, "TIPO_FLUJO_CAJA_FLAG"] = "REAL"
    return df


def get_mid_base(df_model_results, config, api_config):
    """
    Generates a base DataFrame based on model results and configuration.

    :param df_model_results: DataFrame containing results of a model run.
    :type df_model_results: pd.DataFrame
    :param config: Configuration parameters for DownBase.
    :type config: dict
    :return: DataFrame representing the mid base with set column values.
    :rtype: pd.DataFrame
    """
    base = FlujoMid(df_model_results, config, api_config)
    df = base.calculate_mid(False)
    df.loc[:, "TIPO_FLUJO_CAJA_FLAG"] = "REAL"
    return df


def get_values_goals(df_union, df_model_results, config, api_config):
    """
    Retrieves values and goals based on provided data and configuration.

    :param df_union: DataFrame containing union data.
    :type df_union: pd.DataFrame
    :param df_real: DataFrame containing real data.
    :type df_real: pd.DataFrame
    :param df_model_results: DataFrame containing results of a model run.
    :type df_model_results: pd.DataFrame
    :param config: Configuration parameters for ValuesGoals.
    :type config: dict
    :return: DataFrames representing values and goals.
    :rtype: tuple(pd.DataFrame, pd.DataFrame)
    """
    model_values_goals = ValuesGoals(df_union, df_model_results, config, api_config)
    return model_values_goals.get_output()


def get_axis(df_union, df_model_results, api_config, model_config):
    """
    Retrieves the axis based on union and real data.

    :param df_union: DataFrame containing union data.
    :type df_union: pd.DataFrame
    :param df_real: DataFrame containing real data.
    :type df_real: pd.DataFrame
    :return: DataFrame representing the axis.
    :rtype: pd.DataFrame
    """
    model_axis = Axis(df_union, api_config, model_config)
    return model_axis.get_outputs(df_model_results)


def get_base():
    """
    Retrieves flujo_down and flujo_mid base values

    :return: DataFrames representing flujo_down and flujo_mid base.
    :rtype: tuple(pd.DataFrame, pd.DataFrame)
    """
    df_flujo_caja_down_base = db_hive.read(main_config['db_name'], "flujo_down")
    df_flujo_caja_down_base = df_flujo_caja_down_base[df_flujo_caja_down_base['TIPO_FLUJO_CAJA_FLAG'] == 'BASE']

    df_flujo_caja_mid_base = db_hive.read(main_config['db_name'], "flujo_mid")
    df_flujo_caja_mid_base = df_flujo_caja_mid_base[df_flujo_caja_mid_base['TIPO_FLUJO_CAJA_FLAG'] == 'BASE']

    logger.info(f"df_flujo_caja_down_base rows: {len(df_flujo_caja_down_base)}")
    logger.info(
        f"Unique values in df_flujo_caja_down_base TIPO_FLUJO_CAJA_FLAG: {df_flujo_caja_down_base.TIPO_FLUJO_CAJA_FLAG.unique()}")
    return df_flujo_caja_down_base, df_flujo_caja_mid_base

def save_tables_momento1(dfs_dict_p_result, job_run_params):
    """
    Saves tables based on dataframes created in momento_1 execution

    :param dfs_dict_p_result: Dictionary containing DataFrames created in momento_1.
    :type dfs_dict_p_result: Dictionary(key: DataFrame)
    :param job_run_params: Dictionary containing params entered by the powerBI
    :type job_run_params: Dictionary
    :return: None.
    """
    logger.info(f"saving")
    save_types_union(main_config["types_path"])


    dfs_dict_s_result = dict()

    for df_name, df in dfs_dict_p_result.items():
        dfs_dict_p_result[df_name] = auto_cast_df(df)
        dfs_dict_s_result[df_name] = spark.createDataFrame(dfs_dict_p_result[df_name])

    type_mappings = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "FloatType": FloatType(),
    }

    config_types_spark = {
        "df_model_results": "portfolio_model",
        "df_model_fitness": "fitness_model",
        "df_values": "values_model",
        "df_axis_port": "axis_port",
        "df_goals": "goals_model",
        "df_axis_metrics": "axis_metrics_model",
        "df_flujo_caja_down_real": "job_down",
        "df_flujo_caja_mid_real": "job_mid",
        "df_capex_deuda": "job_cd"
    }

    for df_name, data_sheet_type_name in config_types_spark.items():
        df_spark = spark_cast(dfs_dict_s_result[df_name], data_sheet_type_name, main_config["types_path"], type_mappings)
        dfs_dict_s_result[df_name] = df_spark

    db_name = main_config["db_name"]
    spark.sql(f"USE {db_name}")

    logger.info(f"Create tables successfully started")
    tables_start_time = time.time()

    dfs_dict_s_result["df_model_results"].write.mode("overwrite").saveAsTable("portfolio_model")
    dfs_dict_s_result["df_axis_port"].write.mode("overwrite").saveAsTable("axis_port")
    dfs_dict_s_result["df_values"].write.mode("overwrite").saveAsTable("values_model")
    dfs_dict_s_result["df_goals"].write.mode("overwrite").saveAsTable("goals_model")
    dfs_dict_s_result["df_axis_metrics"].write.mode("overwrite").saveAsTable("axis_metrics_model")
    dfs_dict_s_result["df_model_fitness"].write.mode("overwrite").saveAsTable("fitness_model")
    dfs_dict_s_result["df_capex_deuda"].write.mode("overwrite").saveAsTable("capex_deuda_params")

    save_table_real(dfs_dict_s_result["df_flujo_caja_down_real"], "flujo_down", main_config['db_name'], spark)
    save_table_real(dfs_dict_s_result["df_flujo_caja_mid_real"], "flujo_mid", main_config['db_name'], spark)


    formatted_date = get_colombian_time()
    job_run_params["tiempo_final"] = formatted_date
    job_run_params["tiempo_momento_2"] = "No aplica"
    job_run_params = auto_cast_df(job_run_params)
    job_run_params = spark.createDataFrame(job_run_params)
    job_run_params = spark_cast(job_run_params, 'job_params', main_config["types_path"], type_mappings)
    job_run_params.write.mode("overwrite").saveAsTable("params_model")

    tables_end_time = time.time()
    tables_elapsed_time = tables_end_time - tables_start_time

    logger.info(f"Create tables successfully finished, {tables_elapsed_time: .2f} minutes")

    dbutils.notebook.exit("Operación completada")

def save_tables_momento2(dfs_dict_p_result):
    """
    Saves tables based on dataframes created in momento_2 execution

    :param dfs_dict_p_result: Dictionary containing DataFrames created in momento_2.
    :type dfs_dict_p_result: Dictionary(key: DataFrame)
    :return: None.
    """
    logger.info(f"saving")
    save_types_union(main_config["types_path"])
    dfs_dict_s_result = dict()

    for df_name, df in dfs_dict_p_result.items():
        dfs_dict_p_result[df_name] = auto_cast_df(df)
        dfs_dict_s_result[df_name] = spark.createDataFrame(dfs_dict_p_result[df_name])

    type_mappings = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "FloatType": FloatType(),
    }

    config_types_spark = {
        "df_model_results": "portfolio_model",
        "df_values": "values_model",
        "df_axis_port": "axis_port",
        "df_goals": "goals_model",
        "df_axis_metrics": "axis_metrics_model",
        "df_flujo_caja_down_real": "job_down",
    }

    for df_name, data_sheet_type_name in config_types_spark.items():
        df_spark = spark_cast(dfs_dict_s_result[df_name], data_sheet_type_name, main_config["types_path"],
                                    type_mappings)
        dfs_dict_s_result[df_name] = df_spark

    db_name = main_config["db_name"]
    mode = "overwrite"

    spark.sql(f"USE {db_name}")

    logger.info(f"Create tables successfully started")
    tables_start_time = time.time()

    dfs_dict_s_result["df_model_results"].write.mode("overwrite").saveAsTable("portfolio_model")
    dfs_dict_s_result["df_axis_port"].write.mode("overwrite").saveAsTable("axis_port")
    dfs_dict_s_result["df_values"].write.mode("overwrite").saveAsTable("values_model")
    dfs_dict_s_result["df_goals"].write.mode("overwrite").saveAsTable("goals_model")
    dfs_dict_s_result["df_axis_metrics"].write.mode("overwrite").saveAsTable("axis_metrics_model")
    save_table_real(dfs_dict_s_result["df_flujo_caja_down_real"], "flujo_down", main_config['db_name'], spark)
    job_run_params = db_hive.read(db_name, "params_model")
    formatted_date = get_colombian_time()
    job_run_params["tiempo_momento_2"] = formatted_date
    job_run_params = auto_cast_df(job_run_params)
    job_run_params = spark.createDataFrame(job_run_params)
    job_run_params = spark_cast(job_run_params, 'job_params', main_config["types_path"], type_mappings)
    job_run_params.write.mode("overwrite").saveAsTable("params_model")

    tables_end_time = time.time()
    tables_elapsed_time = tables_end_time - tables_start_time

    logger.info(f"Create tables successfully finished, {tables_elapsed_time: .2f} minutes")

    dbutils.notebook.exit("Operación completada")

def momento_1(df_flujo_caja_down_base, df_flujo_caja_mid_base):
    """
    Function that contains the logic for momento1, in here we create several dataFrames for their future storage in
    tables

    :param df_flujo_caja_down_base: DataFrame containing down_base
    :type df_flujo_caja_down_base: DataFrame
    :param df_flujo_caja_mid_base: DataFrame containing mid_base
    :type df_flujo_caja_mid_base: DataFrame

    :return: dfs_dict_result: Dict containing dataframes which will be saved into tables
    :rtype dfs_dict_result: Dictionary(key: DataFrame)
    """
    logger.info(f"momento1")
    dfs_dict_result = {}
    data_processing = DataProcessing(total_caja_config, params_to_float)

    dfs_dict_result["df_flujo_caja_union"] = measure_time(
        "fc_up_be_isa_down", process_fc_up_be_isa_down, data_processing
    )

    dfs_dict_result["df_flujo_caja_union"].reset_index(drop=True, inplace=True)
    #
    dfs_dict_result["df_flujo_caja_union"].to_csv('/dbfs/FileStore/testing/union2.csv', index=False)

    df_union = dfs_dict_result["df_flujo_caja_union"].copy()
    df_union = get_vpn_calculado(df_union)

    (
        dfs_dict_result["df_model_results"],
        dfs_dict_result["df_model_fitness"]
    ) = measure_time(
        "Model",
        run_model,
        model_config,
        params_to_float,
        df_union,
        df_flujo_caja_down_base,
        df_flujo_caja_mid_base
    )
    dfs_dict_result["df_model_results"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_model_fitness"].reset_index(drop=True, inplace=True)

    dfs_dict_result["df_flujo_caja_down_real"] = measure_time(
        "DownBase",
        get_down_base,
        dfs_dict_result["df_model_results"],
        total_caja_config,
        params_to_float
    )
    dfs_dict_result["df_flujo_caja_down_real"].reset_index(drop=True, inplace=True)

    dfs_dict_result["df_flujo_caja_mid_real"] = measure_time(
        "MidBase",
        get_mid_base,
        dfs_dict_result["df_model_results"],
        total_caja_config,
        params_to_float
    )
    dfs_dict_result["df_flujo_caja_mid_real"].reset_index(drop=True, inplace=True)

    dfs_dict_result["df_union_portafolios"] = union_flujos(dfs_dict_result["df_flujo_caja_union"],
                                                           dfs_dict_result["df_flujo_caja_mid_real"],
                                                           dfs_dict_result["df_flujo_caja_down_real"])
    dfs_dict_result["df_values"], dfs_dict_result["df_goals"], dfs_dict_result["df_axis_port"] = measure_time(
        "ValuesGoals",
        get_values_goals,
        dfs_dict_result["df_union_portafolios"],
        dfs_dict_result["df_model_results"],
        model_config,
        params_to_float
    )

    dfs_dict_result["df_values"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_goals"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_axis_port"].reset_index(drop=True, inplace=True)

    (dfs_dict_result["df_axis_metrics"]) = measure_time(
        "Axis",
        get_axis,
        dfs_dict_result["df_union_portafolios"],
        dfs_dict_result["df_model_results"],
        params_to_float,
        model_config
    )
    dfs_dict_result["df_axis_metrics"].reset_index(drop=True, inplace=True)

    dfs_dict_result["df_union_portafolios"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_union_portafolios"].to_csv('/dbfs/FileStore/ESMModel/bronze/union_scala.csv', index=False)

    dfs_dict_result["df_capex_deuda"] = save_capex_debt(model_config, total_caja_config)
    return dfs_dict_result

def momento_2():
    """
    Function that contains the logic for momento2, here several tables are updated based on
    a xlsx with model results

    :return: dfs_dict_result: Dict containing dataframes which will be saved into tables
    :rtype dfs_dict_result: Dictionary(key: DataFrame)
    """
    logger.info(f"momento2")
    dfs_dict_result = {}
    df_union_tables = db_hive.read(main_config['db_name'], "flujo_union_portafolios")
    dfs_dict_result["df_flujo_caja_union"] = df_union_tables.loc[(df_union_tables["NOMBRE_PROYECTO"] != 'Operacion refineria barranca') &
                                                                (df_union_tables["NOMBRE_PROYECTO"] != 'Operacion refineria reficar')]
    dfs_dict_result["df_flujo_caja_union"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_model_results"] = pd.read_excel(main_config["results_momento_2"], engine='openpyxl')

    if 'Unnamed: 0' in dfs_dict_result["df_model_results"]:
        dfs_dict_result["df_model_results"] = dfs_dict_result["df_model_results"].drop("Unnamed: 0", axis='columns')

    import_model_results(dfs_dict_result["df_model_results"])
    dfs_dict_result["df_model_results"].reset_index(drop=True, inplace=True)
    logger.info(f"Leido")

    dfs_dict_result["df_flujo_caja_down_real"] = measure_time(
        "DownBase",
        get_down_base,
        dfs_dict_result["df_model_results"],
        total_caja_config,
        params_to_float
    )
    dfs_dict_result["df_flujo_caja_down_real"].reset_index(drop=True, inplace=True)

    dfs_dict_result["df_union_portafolios"] = union_flujos_2(dfs_dict_result["df_flujo_caja_union"],
                                                           dfs_dict_result["df_flujo_caja_down_real"])

    dfs_dict_result["df_values"], dfs_dict_result["df_goals"], dfs_dict_result["df_axis_port"] = measure_time(
        "ValuesGoals",
        get_values_goals,
        dfs_dict_result["df_union_portafolios"],
        dfs_dict_result["df_model_results"],
        model_config,
        params_to_float
    )
    dfs_dict_result["df_values"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_goals"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_axis_port"].reset_index(drop=True, inplace=True)

    (dfs_dict_result["df_axis_metrics"]) = measure_time(
        "Axis",
        get_axis,
        dfs_dict_result["df_union_portafolios"],
        dfs_dict_result["df_model_results"],
        params_to_float,
        model_config
    )
    dfs_dict_result["df_axis_metrics"].reset_index(drop=True, inplace=True)
    dfs_dict_result["df_union_portafolios"].to_csv('/dbfs/FileStore/ESMModel/bronze/union_scala.csv', index=False)

    return dfs_dict_result

def get_initial_base():
    """
    Function that generates tables flujo_down_base and flujo_mid_base when there aren't any tables in the DB

    :return: None
    """
    dfs_dict_result = {}
    data_processing = DataProcessing(total_caja_config, params_to_float)

    flujo_union = measure_time(
        "fc_up_be_isa_down", process_fc_up_be_isa_down, data_processing
    )
    flujo_union = get_vpn_calculado(flujo_union)

    (
        model_results,
        model_fitness
    ) = measure_time(
        "Model",
        run_model,
        model_config,
        params_to_float,
        flujo_union,
        None,
        None,
        True
    )

    flujo_down_real = measure_time(
        "DownBase",
        get_down_base,
        model_results,
        total_caja_config,
        params_to_float
    )
    dfs_dict_result["df_flujo_caja_down_base"] = flujo_down_real.loc[flujo_down_real["PORTAFOLIO"] == "portafolio_4"]
    dfs_dict_result["df_flujo_caja_down_base"].loc[:, "TIPO_FLUJO_CAJA_FLAG"] = "BASE"
    flujo_down_real.reset_index(drop=True, inplace=True)


    flujo_mid_real = measure_time(
        "MidBase",
        get_mid_base,
        model_results,
        total_caja_config,
        params_to_float
    )
    flujo_mid_real.to_csv('/dbfs/FileStore/testing/mid_0.csv', index=False)

    dfs_dict_result["df_flujo_caja_mid_base"] = flujo_mid_real.loc[flujo_mid_real["PORTAFOLIO"] == "PORTAFOLIO_CASO_BASE"]
    dfs_dict_result["df_flujo_caja_mid_base"].loc[:, "TIPO_FLUJO_CAJA_FLAG"] = "BASE"
    flujo_mid_real.reset_index(drop=True, inplace=True)

    save_initial(dfs_dict_result)

def save_initial(dfs_dict_p_result):
    """
    Function that saves in tables the initial calculations for flujo_down_base and flujo_mid_base

    :param dfs_dict_p_result: Dictionary containing the dataframes of flujo_down_base and flujo_mid_base
    :type dfs_dict_p_result: Dictionary(key: DataFrame)

    :return: None
    """
    dfs_dict_s_result = dict()

    for df_name, df in dfs_dict_p_result.items():
        dfs_dict_p_result[df_name] = auto_cast_df(df)
        dfs_dict_s_result[df_name] = spark.createDataFrame(dfs_dict_p_result[df_name])

    type_mappings = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "FloatType": FloatType(),
    }

    config_types_spark = {
        "df_flujo_caja_down_base": "job_down",
        "df_flujo_caja_mid_base": "job_mid",
    }
    for df_name, data_sheet_type_name in config_types_spark.items():
        df_spark = spark_cast(dfs_dict_s_result[df_name], data_sheet_type_name, main_config["types_path"], type_mappings)
        dfs_dict_s_result[df_name] = df_spark

    db_name = main_config["db_name"]

    spark.sql(f"USE {db_name}")

    logger.info(f"Create tables successfully started")

    dfs_dict_s_result["df_flujo_caja_down_base"].write.mode("overwrite").saveAsTable("flujo_down")
    dfs_dict_s_result["df_flujo_caja_mid_base"].write.mode("overwrite").saveAsTable("flujo_mid")


def main(params_to_float: dict):
    """
    Main function to run data processing and machine learning model
    depending on the configurations specified in config.json.

    :param params_to_float: Dictionary containing the parameters entered from the powerBI
    :type params_to_float: Dictionary
    """
    save_db_name_csv(main_config["db_name"])
    if ("flujo_down" not in sqlContext.tableNames(main_config['db_name']) or
        "flujo_mid" not in sqlContext.tableNames(main_config['db_name'])):
        logger.info(f"Inicializando casos base")
        get_initial_base()
    df_flujo_caja_down_base, df_flujo_caja_mid_base = get_base()
    if params_to_float["momento"] == "momento1":
        job_run_params = get_run_params(params_to_float)
        job_run_params["tiempo_inicio"] = formatted_date
        dfs_dict_result = momento_1(df_flujo_caja_down_base, df_flujo_caja_mid_base)
        save_tables_momento1(dfs_dict_result, job_run_params)
    elif params_to_float["momento"] == "momento2":
        dfs_dict_result = momento_2()
        save_tables_momento2(dfs_dict_result)
    else:
        logger.info(f"Revise el momento iniciado, este debe ser momento1 o momento2")

if __name__ == "__main__":
    """
    Here is where the execution of the main.py starts.
    the params are read assuming they are passed in the same order as in the params list.
    
    Note that for momento_1 all the params present in the list have to be present, while in momento_2
    only the momento param has to be present

    After reading the params the execution of the main function starts
    """
    formatted_date = get_colombian_time()
    if len(sys.argv) == 2:
        params_momento_1 = db_hive.read(main_config['db_name'], "params_model")
        params_to_float = params_momento_1.to_dict(orient='list')
        for key, value in params_to_float.items():
            params_to_float[key] = try_float(value[0])
        params_to_float["momento"] = sys.argv[1]
    else:
        params = [
            "vpn_weight",
            "vpn_goal",
            "deuda_bruta_weight",
            "deuda_bruta_ratio",
            "trans_nacion_weight",
            "trans_nacion_min",
            "ebitda_weight",
            "ebitda_min",
            "low_emissions_weight",
            "low_emissions_min",
            "emisiones_netas_co2_alcance_1_y_2_weight",
            "emisiones_netas_co2_alcance_1_y_2_actual",
            "neutralidad_agua_weight",
            "neutralidad_agua_actual",
            "mwh_weight",
            "mwh_goal",
            "not_oil_jobs_weight",
            "not_oil_jobs_goal",
            "students_weight",
            "students_goal",
            "natural_gas_weight",
            "natural_gas_actual",
            "agua_potable_weight",
            "agua_potable_actual",
            "km_weight",
            "km_actual",
            "cti_weight",
            "cti_min",
            "val_curva_crudo",
            "val_sensi_precio_crudo",
            "val_sensi_precio_gas",
            "val_sensi_precio_Hidro",
            "val_delta_energia",
            "val_sensi_precio_co2",
            "generations",
            "sol_per_pop",
            "mutation_ind",
            "mutation_gen",
            "vpn_zero_restriction",
            "be_restriction",
            "phase_restriction",
            "cash_flow_restriction",
            "gas_demanda_nacion",
            "transporte_offshore",
            "momento"]
        params_to_float = {params[i]: try_float(sys.argv[i + 1]) for i in range(len(params))}
        check_units(params_to_float)
    main(params_to_float)


