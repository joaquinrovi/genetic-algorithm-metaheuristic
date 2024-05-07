import json

import pandas as pd
import numpy as np
from datetime import datetime
import time
import pytz
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)
from pyspark.sql.functions import when, lit, col as spark_col
from delta.tables import DeltaTable

def read_json(path: str):
    """
    function that reads a json file

    :param path: location or name of the .json file
    :return: Json read by the function
    """
    f = open(path)
    return json.load(f)

def auto_cast_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function that casts a dataFrames columns type fot its future conversion to a Spark DataFrame

    :param df: DataFrame containing the  info to cast
    :type df: DataFrame


    :return: df: DataFrame with cast columns
    :rtype df: DataFrame
    """
    for column in df.columns:
        if df[column].isna().all():
            continue
        non_nan_values = df[column].dropna()

        # If any value is string, cast the column to string
        if any(isinstance(val, str) for val in non_nan_values):
            df[column] = df[column].astype(str)
        # If any value is datetime or timedelta, continue without casting
        elif any(isinstance(val, (pd.Timestamp, pd.Timedelta, pd.Period)) for val in non_nan_values):
            continue
        # If any value is float, cast the entire column to float
        elif any(isinstance(val, (float, np.float64)) for val in non_nan_values):
            df[column] = df[column].apply(lambda x: float(x) if pd.notna(x) else x)
        # Otherwise, cast to int but handle NaN values properly
        else:
            df[column] = df[column].apply(lambda x: int(x) if pd.notna(x) else None)

    return df

def get_configuration(xlsx_path, json_path):
    """
    Function that gets configuration for flujo_union by merging the xlsx config file and the api config input

    :param xlsx_path: String containing path to xlsx config
    :type xlsx_path: String
    :param json_path: String containing path to json config
    :type json_path: String


    :return: Dictionary containing all flujo_caja configs
    :rtype Dictionary
    """
    xls = pd.ExcelFile(xlsx_path)
    df1 = pd.read_excel(xls, 'Parametros')
    df2 = pd.read_excel(xls, 'Curva_crudo')
    df3 = pd.read_excel(xls, 'Datos_sobretasa_depreciacion')
    df4 = pd.read_excel(xls, 'Rango_anio')
    df5 = pd.read_excel(xls, 'Historico_precios')
    df6 = pd.read_excel(xls, 'Down_refinerias')

    dictionary_ones = df3.to_dict(orient='list')

    fuente = read_json(json_path)
    for key, value in dictionary_ones.items():
        dictionary_ones[key] = value[0]

    params = {}
    params.update(df1.to_dict(orient='list'))
    params.update(df2.to_dict(orient='list'))
    params.update(df4.to_dict(orient='list'))
    params.update(df5.to_dict(orient='list'))
    params.update(df6.to_dict(orient='list'))
    params.update(dictionary_ones)
    return {
        "parametros": params,
        "fuente": fuente
    }


def get_configuration_model(xlsx_path, json_path):
    """
    Function that gets configuration for model by merging the xlsx config file and the api config input

    :param xlsx_path: String containing path to xlsx config
    :type xlsx_path: String
    :param json_path: String containing path to json config
    :type json_path: String


    :return: params: Dictionary containing all model configs
    :rtype Dictionary
    """
    xls = pd.ExcelFile(xlsx_path)
    df1 = pd.read_excel(xls, 'Model_params')
    df2 = pd.read_excel(xls, 'Curva_crudo')

    fuente = read_json(json_path)
    params = {}
    params.update(df1.to_dict(orient='list'))
    params.update(df2.to_dict(orient='list'))
    params.update(fuente)
    params["capex_yearly_budget"] = np.array(params["CAPEX"])
    params["debt"] = np.array(params["DEBT"])
    return params


def try_float(value):
    """
    Function that check the type of the data received from the powerBI, here it tries to find any float values

    :param value: String containing a certain value to check
    :type value: String

    :return: value: Variable containing cast variable
    :rtype int-float-String
    """
    if '.' in value:
        try:
            return float(value)
        except ValueError:
            return value
    try:
        return int(value)
    except ValueError:
        return value


def check_units(params):
    """
    Function that converts the units of the parameters received from the powerBI, this is done to give them to the model
    int the units it expects

    :param params: Dictionary with the params received from the powerBI
    :type value: Dictionary

    :return: None
    """
    params["vpn_goal"] = params["vpn_goal"] * 1000
    params["deuda_bruta_ratio"] = params["deuda_bruta_ratio"]
    params["trans_nacion_min"] = params["trans_nacion_min"] * 1000
    params["ebitda_min"] = params["ebitda_min"] * 1000
    params["low_emissions_min"] = params["low_emissions_min"] * 0.01
    params["emisiones_netas_co2_alcance_1_y_2_actual"] = params["emisiones_netas_co2_alcance_1_y_2_actual"]
    params["neutralidad_agua_actual"] = params["neutralidad_agua_actual"] * 1000
    params["mwh_goal"] = params["mwh_goal"]
    params["not_oil_jobs_goal"] = params["not_oil_jobs_goal"] * 1000
    params["students_goal"] = params["students_goal"] * 1000000
    params["natural_gas_actual"] = params["natural_gas_actual"]
    params["agua_potable_actual"] = params["agua_potable_actual"]
    params["km_actual"] = params["km_actual"]
    params["cti_min"] = params["cti_min"] * 1000
    params["val_sensi_precio_crudo"] = params["val_sensi_precio_crudo"] / 100
    params["val_sensi_precio_gas"] = params["val_sensi_precio_gas"] / 100
    params["val_sensi_precio_Hidro"] = params["val_sensi_precio_Hidro"] / 100
    params["val_sensi_precio_co2"] = params["val_sensi_precio_co2"] / 100
    params["val_delta_energia"] = params["val_delta_energia"] / 100

def get_run_params(params):
    """
    Function that converts every params received by the powerBI into a list of one element for its future storage in a
    DataFrame

    :param params: Dictionary with the params received from the powerBI
    :type value: Dictionary

    :return: Dataframe containing all params received from the BI
    :rtype DataFrame
    """
    params = {
        "vpn_weight": [params["vpn_weight"]],
        "vpn_goal": [params["vpn_goal"]],
        "deuda_bruta_weight":[params["deuda_bruta_weight"]],
        "deuda_bruta_ratio":[params["deuda_bruta_ratio"]],
        "trans_nacion_weight":[params["trans_nacion_weight"]],
        "trans_nacion_min":[params["trans_nacion_min"]],
        "ebitda_weight":[params["ebitda_weight"]],
        "ebitda_min":[params["ebitda_min"]],
        "low_emissions_weight":[params["low_emissions_weight"]],
        "low_emissions_min":[params["low_emissions_min"]],
        "emisiones_netas_co2_alcance_1_y_2_weight":[params["emisiones_netas_co2_alcance_1_y_2_weight"]],
        "emisiones_netas_co2_alcance_1_y_2_actual":[params["emisiones_netas_co2_alcance_1_y_2_actual"]],
        "neutralidad_agua_weight":[params["neutralidad_agua_weight"]],
        "neutralidad_agua_actual":[params["neutralidad_agua_actual"]],
        "mwh_weight":[params["mwh_weight"]],
        "mwh_goal":[params["mwh_goal"]],
        "not_oil_jobs_weight":[params["not_oil_jobs_weight"]],
        "not_oil_jobs_goal":[params["not_oil_jobs_goal"]],
        "students_weight":[params["students_weight"]],
        "students_goal":[params["students_goal"]],
        "natural_gas_weight":[params["natural_gas_weight"]],
        "natural_gas_actual":[params["natural_gas_actual"]],
        "agua_potable_weight":[params["agua_potable_weight"]],
        "agua_potable_actual":[params["agua_potable_actual"]],
        "km_weight":[params["km_weight"]],
        "km_actual":[params["km_actual"]],
        "cti_weight":[params["cti_weight"]],
        "cti_min":[params["cti_min"]],
        "val_curva_crudo":[params["val_curva_crudo"]],
        "val_sensi_precio_crudo":[params["val_sensi_precio_crudo"]],
        "val_sensi_precio_gas":[params["val_sensi_precio_gas"]],
        "val_sensi_precio_Hidro":[params["val_sensi_precio_Hidro"]],
        "val_delta_energia":[params["val_delta_energia"]],
        "val_sensi_precio_co2":[params["val_sensi_precio_co2"]],
        "generations":[params["generations"]],
        "sol_per_pop":[params["sol_per_pop"]],
        "mutation_ind":[params["mutation_ind"]],
        "mutation_gen":[params["mutation_gen"]],
        "vpn_zero_restriction":[params["vpn_zero_restriction"]],
        "phase_restriction":[params["phase_restriction"]],
        "cash_flow_restriction":[params["cash_flow_restriction"]],
        "gas_demanda_nacion":[params["gas_demanda_nacion"]],
        "transporte_offshore":[params["transporte_offshore"]],
        "momento":[params["momento"]]
    }

    return pd.DataFrame(params)


def save_types_union(types_path):
    """
    Helper function that saves the Types union sheet of the types_excel file in a separate csv file

    :param types_path: String containing the types excel path
    :type types_path: String

    Returns: None

    """
    df_union_data_type = pd.read_excel(
        types_path,
        sheet_name="job_union_down_mid",
        engine="openpyxl",
    )
    df_union_data_type.to_csv('/dbfs/FileStore/testing/union_data_type.csv', index=False)


def spark_cast(df, data_sheet_type_name, types_path, type_mappings):
    """
    Function that casts columns of a certain df based on the types present in the ESM_TYPE excel config (check path
    in main_config)

    :param df: DataFrame to be cast
    :type df: Spark DataFrame
    :param data_sheet_type_name: String containing the name of the Sheet of the Excel where the types of the current
    df are
    :type data_sheet_type_name: String
    :param types_path: String containing the path to the Excel which has the types
    :type types_path: String
    :param type_mappings: Dictionary containing the types for spark
    :type type_mappings: Dictionary

    :return: Dataframe with corresponding spark cast
    :rtype Spark DataFrame
    """
    df_data_type = pd.read_excel(
        types_path,
        sheet_name=data_sheet_type_name,
        engine="openpyxl",
    )

    df_spark = df

    for index, row in df_data_type.iterrows():
        column_name = row["COLUMNAS"]
        spark_type_str = row["SPARK_TYPE"].replace("()", "")
        spark_type = type_mappings[spark_type_str]

        if column_name in df_spark.columns:
            df_spark = df_spark.withColumn(
                column_name, df_spark[column_name].cast(spark_type)
            )

    for spark_column in df_spark.columns:
        df_spark = df_spark.withColumn(
            spark_column,
            when(spark_col(spark_column) == lit("nan"), lit(None)).otherwise(
                spark_col(spark_column)
            ),
        )

    df_spark = df_spark.coalesce(1)

    return df_spark

def import_model_results(df):
    """
    Function that changes the names of all columns in the df read from the Excel uploaded for momento_2, this is
    to let the Excel be exported with different column names and later do all calculations with standard names

    :param df: results DataFrame raed from the dbfs (check path in main_config)
    :type df: DataFrame

    :return: None
    """
    df.rename(columns={
        'Matricula': 'MATRICULA_DIGITAL',
        'Proyecto': 'NOMBRE_PROYECTO',
        'Activo': 'ACTIVO',
        'Empresa': 'EMPRESA',
        'Tipo Inversión': 'TIPO_DE_INVERSION',
        'Fase': 'FASE_EN_CURSO',
        'Segmento': 'SEGMENTO',
        'Esc 1': 'PORTAFOLIO_1',
        'Esc 2': 'PORTAFOLIO_2',
        'Esc 3': 'PORTAFOLIO_3',
        'Esc 4': 'PORTAFOLIO_4',
        'Esc 5': 'PORTAFOLIO_5',
        'Esc 6': 'PORTAFOLIO_6',
    }
        , inplace=True)


def save_table_real(df, table_name, database, spark):
    """
    Function that only changes the table where TUPO_FLUJO_CAJA_FLAG = REAL, this is done specifically with
    flujo_mid and flujo_down tables

    :param df: DataFrame which has the data to be updated onto the table
    :type df: DataFrame
    :param table_name: String containing the name of the table to update
    :type table_name: String
    :param database: String containing the name of the current database
    :type database: String
    :param spark: Current Spark Session
    :type spark: Spark Session

    :return: None
    """
    # Cargar la tabla Delta
    deltaTable = DeltaTable.forPath(spark, f"/user/hive/warehouse/{database}.db/{table_name}")
    # Eliminar registros con MOMENTO = "MOMENTO2"
    deltaTable.delete(f"TIPO_FLUJO_CAJA_FLAG = 'REAL'")

    df.write.format("delta").mode("append").save(f"/user/hive/warehouse/{database}.db/{table_name}")

def save_capex_debt(config, total_caja_config):
    """
    Function that creates a dataframe to store the capex and debt params entered from the excel config, followed by the
    corresponding year of that param

    :param config: Dictionary containing all model params
    :type config: Dictionary
    :param total_caja_config: Dictionary containing all flujo_caja params
    :type total_caja_config: Dictionary

    :return: DataFrame containing the capex, deuda and year columns
    :rtype DataFrame
    """
    anio_inicial = total_caja_config['parametros']['rango_anio'][0]
    anio_final = total_caja_config['parametros']['rango_anio'][1]
    anios = [i for i in range(anio_inicial, anio_final + 1)]
    df_cd = pd.DataFrame({"CAPEX": config["capex_yearly_budget"], "DEUDA": config["debt"], "RANGO_ANIO": anios})
    df_cd.reset_index(drop=True, inplace=True)
    return df_cd

def get_colombian_time():
    """
    Helper Function that gets the current colombian time

    :return: String containing the current colombian time
    :rtype String
    """
    utc = pytz.timezone('UTC')
    now = utc.localize(datetime.utcnow())

    col = pytz.timezone('America/Bogota')
    col_time = now.astimezone(col)

    return col_time.strftime('%Y-%m-%d %H:%M:%S')


def save_db_name_csv(db_name):
    """
    Helper function to save db_name in a csv, this is done to let scala task know the db_name

    :param db_name: String with db_name
    :type db_name: String

    :return: None

    """
    df = pd.DataFrame({"db_name": [db_name]})
    df.to_csv("/dbfs/FileStore/ESMModel/bronze/db_scala.csv")