def transform_dataframe_config1(input_df: pd.DataFrame) -> pd.DataFrame:
    anio_list = []
    codigo_list = []
    nombre_list = []
    valor_list = []

    # Procesar la configuración 1
    temp_anio = input_df["Código"].tolist()[1:]
    temp_codigo = input_df.columns[1:].tolist()
    temp_nombre = input_df.iloc[0, 1:].tolist()

    for col in input_df.columns[1:]:
        anio_list.extend(temp_anio)
        codigo_list.extend([col] * len(temp_anio))
        nombre_list.extend([temp_nombre[input_df.columns.get_loc(col) - 1]] * len(temp_anio))
        valor_list.extend(input_df[col].tolist()[1:])

    final_df = pd.DataFrame({
        'ANIO': anio_list,
        'CODIGO': codigo_list,
        'NOMBRE': nombre_list,
        'VALOR': valor_list,
    })

    return final_df


def transform_dataframe_config2(input_df: pd.DataFrame) -> pd.DataFrame:
    anio_list = []
    nombre_list = []
    valor_list = []

    # Procesar la configuración 2
    temp_anio = input_df["Product"].tolist()
    temp_nombre = input_df.columns[1:].tolist()

    for col in input_df.columns[1:]:
        anio_list.extend(temp_anio)
        nombre_list.extend([col] * len(temp_anio))
        valor_list.extend(input_df[col].tolist())

    final_df = pd.DataFrame({
        'ANIO': anio_list,
        'NOMBRE': nombre_list,
        'VALOR': valor_list,
    })

    return final_df


def convert_column_type(df, col_name, type_to_transform):
    try:
        if df[col_name].dtype == 'object':
            df[col_name] = df[col_name].str.replace(',', '.')

        df[col_name] = df[col_name].astype(type_to_transform)
    except Exception as e:
        print(f"Error al convertir la columna {col_name} a {type_to_transform}. Detalle del error: {e}")

    return df


import pandas as pd
import string
import random
from itertools import chain


def generate_random_alphanumeric(length=8):
    alphanumeric_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_characters) for _ in range(length))


def fill_alphanumeric_for_nan_or_empty(value):
    if pd.isna(value) or str(value).strip() == '':
        return generate_random_alphanumeric()
    else:
        return value


def exclude_columns_by_regex(df: pd.DataFrame, regex_list: list) -> pd.DataFrame:
    total_columns_to_exclude = list(
        chain.from_iterable(
            df.columns[df.columns.str.match(regex)] for regex in regex_list
        )
    )
    return df.drop(columns=total_columns_to_exclude)


def generate_transponed_df(df, id_v, var_n, new_column_name, pattern):
    df_melted = df.melt(
        id_vars=[id_v],
        value_vars=df.columns[1:],
        value_name=new_column_name,
        var_name=var_n
    )
    df_melted[var_n] = df_melted[var_n].str.extract(pattern).astype(int)
    return df_melted


def expand_row(row, start_value, end_value):
    return pd.DataFrame({**row, config["params"]["var_n"]: range(start_value, end_value)})


def format_year(year):
    if len(str(year)) < 4:
        return 2000 + int(year)
    return int(year)

tables_config = {
    "data_types_config": {
        "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/ESM_Datatype.xlsx"
    },
    "metastore_tables_config": {
        "esm_model_tables": {
            "setup": [
                {"db_name": "esm_model", "table_name": "flujo_caja"},
                {"db_name": "esm_model", "table_name": "esm_optimizacion"},
            ]
        }
    },
    "raw_files_read_config": {
        "plp": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/PLP.xlsx",
            "setup": [
                {
                    "sheet_name": "Upstream",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "plp_upstream",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "plp_upstream",
                    "read_for_flujo_caja": [False, "df_plp_upstream"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "plp_upstream",
                    },
                },
                {
                    "sheet_name": "Upstream",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "plp_upstream_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "plp_upstream_v2",
                    "read_for_flujo_caja": [True, "df_plp_upstream_v2"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "plp_upstream_v2",
                    },
                    "transposed_config": {
                        "params": {"id_v": "Matrícula Digital", "var_n": "RANGO_ANIO"},
                        "patterns": {
                            "capex": {
                                "column_pattern": "CapEx \d+",
                                "extract_pattern": "(\d{2})$",
                                "new_column_name": "CAPEX",
                            },
                            "crudo_risked": {
                                "column_pattern": "Prod. Crudo \d+ Risked",
                                "extract_pattern": "(\d{2}) Risked$",
                                "new_column_name": "PRODUCCION_CRUDO",
                            },
                            "gas_risked": {
                                "column_pattern": "Prod. Gas \d+ Risked",
                                "extract_pattern": "(\d{2}) Risked$",
                                "new_column_name": "PRODUCCION_GAS",
                            },
                            "blancos_risked": {
                                "column_pattern": "Blancos \d+ Risked",
                                "extract_pattern": "(\d{2}) Risked$",
                                "new_column_name": "PRODUCCION_BLANCOS",
                            },
                            "prodequiv_risked": {
                                "column_pattern": "Prod. Equiv. \d+ Risked",
                                "extract_pattern": "(\d{2}) Risked$",
                                "new_column_name": "PRODUCCION_EQUIVALENTE",
                            },
                            "emisiones_netas_co2": {
                                "column_pattern": "Emisiones Netas Co2 Alcance 1 y 2 \d+",
                                "extract_pattern": "(\d{4})$",
                                "new_column_name": "EMISIONES_NETAS_CO2",
                            },
                            "pir_1p": {
                                "column_pattern": "PIR \d+ 1P",
                                "extract_pattern": "(\d{2}) 1P$",
                                "new_column_name": "PIR",
                            },
                            "capex_exploratorio": {
                                "column_pattern": "CapEx Exploratorio \d+",
                                "extract_pattern": "(\d{2})$",
                                "new_column_name": "CAPEX_EXPLORATORIO",
                            },
                        },
                    },
                },
                {
                    "sheet_name": "Activo Upstream",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "plp_activo_upstream",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "plp_activo_upstream",
                    "read_for_flujo_caja": [True, "df_plp_activo_upstream"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "plp_activo_upstream",
                    },
                },
            ],
        },
        "formato_peep_crudos_portafolio": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/Formato_PEEP_Crudos_Portafolio_2023.xlsx",
            "setup": [
                {
                    "sheet_name": "Offset Calidad",
                    "header": 3,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_oc",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc",
                    "rows_to_delete": ["0-19"],
                    "change_dtype": [["Nombre", "int"]],
                    "order_by": {"columns": ["Nombre"], "order": ["asc"]},
                },
                {
                    "sheet_name": "Offset Calidad",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_oc_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc_v2",
                    "config_type": 1,
                    "rows_to_delete": ["1-20"],
                    "order_by": {"columns": ["Código"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_crudos_portafolio_oc"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "crudos_portafolio_oc_v2",
                    },
                },
                {
                    "sheet_name": "Offset Transporte tarifa costo",
                    "header": 3,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_ot",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_ot",
                    "rows_to_delete": ["0-19"],
                    "change_dtype": [["Nombre", "int"]],
                    "order_by": {"columns": ["Nombre"], "order": ["asc"]},
                },
                {
                    "sheet_name": "Offset Transporte tarifa costo",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_ot_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc_v2",
                    "config_type": 1,
                    "rows_to_delete": ["1-20"],
                    "order_by": {"columns": ["Código"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_crudos_portafolio_ot"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "crudos_portafolio_ot_v2",
                    },
                },
                {
                    "sheet_name": "Pas. mol. dil. tarifa costo",
                    "header": 3,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_pm",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_pm",
                    "rows_to_delete": ["0-19"],
                    "change_dtype": [["Nombre", "int"]],
                    "order_by": {"columns": ["Nombre"], "order": ["asc"]},
                },
                {
                    "sheet_name": "Pas. mol. dil. tarifa costo",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_pm_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc_v2",
                    "config_type": 1,
                    "rows_to_delete": ["1-20"],
                    "order_by": {"columns": ["Código"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_crudos_portafolio_pm"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "crudos_portafolio_pm_v2",
                    },
                },
                {
                    "sheet_name": "Precio neto (con tarifa costo)",
                    "header": 3,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_pn",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_pn",
                    "rows_to_delete": ["0-19"],
                    "change_dtype": [["Nombre", "int"]],
                    "order_by": {"columns": ["Nombre"], "order": ["asc"]},
                },
                {
                    "sheet_name": "Precio neto (con tarifa costo)",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "crudos_portafolio_pn_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc_v2",
                    "config_type": 1,
                    "rows_to_delete": ["1-20"],
                    "order_by": {"columns": ["Código"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_crudos_portafolio_pn"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "crudos_portafolio_pn_v2",
                    },
                },
            ],
        },
        "factores_de_dilucion": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/Factores_de_dilución.xlsx",
            "setup": [
                {
                    "sheet_name": "Factor de dilución",
                    "header": 0,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "factores_dilucion_fd",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "factores_dilucion_fd",
                    "read_for_flujo_caja": [True, "df_factores_dilucion_fd"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "factores_dilucion_fd",
                    },
                }
            ],
        },
        "intensidad_emisiones_co2e": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/INTENSIDAD_DE_EMISIONES_DE_CO2E.xlsx",
            "setup": [
                {
                    "sheet_name": "upstream",
                    "header": 0,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "intensidad_emisiones_up",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "intensidad_emisiones_up",
                    "transform_column_dtype": [["intensidad", "float"]],
                    "read_for_flujo_caja": [True, "df_intensidad_emisiones_up"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "intensidad_emisiones_up",
                    },
                }
            ],
        },
        "formato_peep_gas_portafolio_ecp": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/Formato_PEEP_GAS_Portafolio_2023_ECP.xlsx",
            "setup": [
                {
                    "sheet_name": "Precio Neto Básica",
                    "header": 12,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "gas_portafolio_pnb",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "gas_portafolio_pnb",
                    "rows_to_delete": ["0-8"],
                    "change_dtype": [["Product", "int"]],
                    "order_by": {"columns": ["Product"], "order": ["asc"]},
                },
                {
                    "sheet_name": "Precio Neto Básica",
                    "header": 12,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "gas_portafolio_pnb_v2",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "gas_portafolio_pnb_v2",
                    "config_type": 2,
                    "rows_to_delete": ["0-8"],
                    "order_by": {"columns": ["Product"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_gas_portafolio_pnb"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "gas_portafolio_pnb_v2",
                    },
                },
            ],
        },
        "formato_peep_ngl_portafolio": {
            "dbfs_path": "/dbfs/FileStore/ESMModel/bronze/Formato_PEEP_NGL_Portafolio_2023.xlsx",
            "setup": [
                {
                    "sheet_name": "Precio neto tarifa costo",
                    "header": 2,
                    "engine": "openpyxl",
                    "db_name": "esm_model",
                    "table_name": "ngl_portafolio",
                    "delete_unnamed_cols_if_full_nan": True,
                    "sheet_name_data_types": "crudos_portafolio_oc_v2",
                    "config_type": 1,
                    "rows_to_delete": ["1-20"],
                    "order_by": {"columns": ["Código"], "order": ["asc"]},
                    "read_for_flujo_caja": [True, "df_ngl_portafolio"],
                    "data_lake": {
                        "save_as_parquet": True,
                        "dbfs_path": "dbfs:/FileStore/ESMModel/silver/",
                        "file_name": "ngl_portafolio",
                    },
                },
            ],
        },
    },
}

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import when, lit, col as spark_col


def auto_cast_df(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if df[column].isna().all():
            continue

        non_nan_values = df[column].dropna()

        # If any value is string, cast the column to string
        if any(isinstance(val, str) for val in non_nan_values):
            df[column] = df[column].astype(str)
        # If any value is float, cast the entire column to float
        elif any(isinstance(val, (float, np.float64)) for val in non_nan_values):
            df[column] = df[column].apply(lambda x: float(x) if x is not None else x)
        # Otherwise, cast to int
        else:
            df[column] = df[column].apply(lambda x: int(x) if x is not None else x)

    return df


logger = Logger("DataProcessing").get_logger()

spark = SparkSession.builder.getOrCreate()

logger.info("SparkSession successfully created")

db_hive = DatabricksHive(spark)
logger.info("DatabricksHive initialized")

type_mappings = {"IntegerType": IntegerType(), "StringType": StringType(), "FloatType": FloatType()}

for file_key, file_config in tables_config["raw_files_read_config"].items():
    dbfs_path = file_config["dbfs_path"]
    logger.info(f"Processing file: {file_key} at path {dbfs_path}")

    for sheet_config in file_config["setup"]:
        sheet_name = sheet_config["sheet_name"]
        logger.info(f"Reading raw sheet: {sheet_name}")

        # Read the primary file
        df = pd.read_excel(
            dbfs_path,
            sheet_name=sheet_name,
            header=sheet_config["header"],
            engine=sheet_config["engine"]
        )

        logger.debug(f"Loaded DataFrame for {sheet_name} with shape {df.shape}")

        # Read the data type configuration file
        data_types_config_path = tables_config["data_types_config"]["dbfs_path"]
        sheet_name_data_types = sheet_config["sheet_name_data_types"]

        df_data_type = pd.read_excel(
            data_types_config_path,
            sheet_name=sheet_name_data_types,
            engine="openpyxl",
        )
        logger.debug(f"Loaded data types config for {sheet_name_data_types} with shape {df_data_type.shape}")

        logger.debug(f"{df.shape[0]} rows read from {sheet_name}")

        # Check for row deletions
        if "rows_to_delete" in sheet_config:
            rows_to_delete = sheet_config["rows_to_delete"]
            for row in rows_to_delete:
                if isinstance(row, str) and "-" in row:
                    start, end = map(int, row.split("-"))
                    df.drop(df.index[start: end], inplace=True)
                    logger.debug(f"Dropped rows from {start} to {end}")
                else:
                    df.drop(row, inplace=True)
                    logger.debug(f"Dropped row {row}")

        # Remove columns if they start with the word Unnamed and are 100% null values
        if sheet_config.get("delete_unnamed_cols_if_full_nan", False):
            cols_to_drop = [col for col in df.columns if
                            str(col).startswith(("Unnamed", "UNNAMED")) and df[col].isna().all()]
            df.drop(columns=cols_to_drop, inplace=True)
            logger.debug(f"Dropped columns: {cols_to_drop}")

        # Change column type if it is specified in the config
        if "change_dtype" in sheet_config:
            for change in sheet_config["change_dtype"]:
                col_name, new_dtype = change
                if col_name in df.columns:
                    try:
                        df[col_name] = df[col_name].astype(new_dtype)
                        logger.debug(f"Changed dtype of column {col_name} to {new_dtype}")
                    except Exception as e:
                        logger.error(f"Error while changing dtype of column {col_name} to {new_dtype}: {str(e)}")

        # Sort the DataFrame based on the specified order by
        if "order_by" in sheet_config:
            try:
                order_config = sheet_config["order_by"]
                sort_cols = order_config["columns"]
                sort_ascending = [True if order == "asc" else False for order in order_config["order"]]
                df = df.sort_values(by=sort_cols, ascending=sort_ascending)
                logger.debug(f"Ordered dataframe by {sort_cols} in order {order_config['order']}")
            except Exception as e:
                logger.error(f"Error while ordering dataframe: {str(e)}")

        if "config_type" in sheet_config:
            if sheet_config["config_type"] == 1:
                df = transform_dataframe_config1(df)
                logger.debug(f"Applied transform_dataframe_config1 for {sheet_name}")
            elif sheet_config["config_type"] == 2:
                df = transform_dataframe_config2(df)
                logger.debug(f"Applied transform_dataframe_config2 for {sheet_name}")

        if "transform_column_dtype" in sheet_config:
            col_name = sheet_config["transform_column_dtype"][0][0]
            type_to_transform = sheet_config["transform_column_dtype"][0][1]
            df = convert_column_type(df, col_name, type_to_transform)

        if "transposed_config" in sheet_config:
            config = sheet_config["transposed_config"]

            df['Matrícula Digital'] = df['Matrícula Digital'].apply(fill_alphanumeric_for_nan_or_empty)

            dfs = {}
            for key, value in config["patterns"].items():
                columns_for_melt = [config["params"]["id_v"]]
                columns_for_melt.extend(df.columns[df.columns.str.match(value["column_pattern"])])
                df_temp = df[columns_for_melt]
                dfs[key] = generate_transponed_df(df_temp, config["params"]["id_v"], config["params"]["var_n"],
                                                  value["new_column_name"], value["extract_pattern"])

            for key, df_in_dfs in dfs.items():
                dfs[key]['RANGO_ANIO'] = df_in_dfs['RANGO_ANIO'].apply(format_year)

            regex_list = [pattern["column_pattern"] for pattern in config["patterns"].values()]
            df_selected = exclude_columns_by_regex(df, regex_list)

            annio_inicio = 2023
            annio_fin = 2040 + 1
            df_selected = pd.concat(df_selected.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(),
                                    ignore_index=True)

            result = df_selected.copy()
            for key, df in dfs.items():
                result = result.merge(
                    df[['Matrícula Digital', 'RANGO_ANIO', config["patterns"][key]["new_column_name"]]],
                    on=['Matrícula Digital', 'RANGO_ANIO'],
                    how='left'
                )

                result[config["patterns"][key]["new_column_name"]] = result[
                    config["patterns"][key]["new_column_name"]].fillna(0)

            df = result

        seen_columns = {}
        normalized_columns = []

        for col in df.columns:
            norm_col = normalize_column_name(col)
            if norm_col in seen_columns:
                norm_col = f"{norm_col}_DUPLICATE"
            seen_columns[norm_col] = True
            normalized_columns.append(norm_col)

        df.columns = normalized_columns
        logger.debug(f"Normalized column names: {normalized_columns}")

        db_name = sheet_config["db_name"]
        table_name = sheet_config["table_name"]

        df = auto_cast_df(df)
        df_spark = spark.createDataFrame(df)

        for index, row in df_data_type.iterrows():
            column_name = row["COLUMNAS"]
            spark_type_str = row["SPARK_TYPE"].replace("()", "")
            spark_type = type_mappings[spark_type_str]
            df_spark = df_spark.withColumn(column_name, df_spark[column_name].cast(spark_type))

        for spark_column in df_spark.columns:
            df_spark = df_spark.withColumn(spark_column,
                                           when(spark_col(spark_column) == lit("nan"), lit(None)).otherwise(
                                               spark_col(spark_column)))

        df_spark = df_spark.coalesce(1)

        if "data_lake" in sheet_config and sheet_config["data_lake"]["save_as_parquet"] is True:
            dbfs_path_dl = sheet_config["data_lake"]["dbfs_path"] + sheet_config["data_lake"]["file_name"]

            df_spark.write.option("compression", "snappy").mode("overwrite").parquet(dbfs_path_dl)

            logger.info(f"Saving as parquet: {dbfs_path_dl}")

        db_hive.delete(db_name, table_name)
        logger.debug(f"Deleted existing table {table_name} in database {db_name}")

        db_hive.create(df_spark, db_name, table_name)
        logger.info(f"Table created in database: {db_name}, table: {table_name}")

logger.info("Processing successfully completed")

