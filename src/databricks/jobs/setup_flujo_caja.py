# Esto deberia venir desde un excel del cliente y luego una tabla en Hive
annio_inicio = 2023
annio_fin = 2040 + 1

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import when, lit, col as spark_col, udf, row_number, coalesce
import string, random

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

from pyspark.sql import SparkSession

db_hive = DatabricksHive(spark)
logger = Logger(__name__).get_logger()

class DataProcessing:
    def __init__(self, spark: SparkSession, data_source_config: dict):
        self.db_hive = DatabricksHive(spark)
        self.data_source_config = data_source_config

    def read_data(self):
        """
        Reads data from the configured data sources, and returns a dictionary of pandas DataFrames.

        :return: A dictionary containing pandas DataFrames indexed by the custom name specified in the 'read_for_flujo_caja' configuration.
        :rtype: dict
        """
        data_frames = {}

        for file_key, file_config in self.data_source_config["raw_files_read_config"].items():
            for sheet_config in file_config["setup"]:
                if "read_for_flujo_caja" in sheet_config and sheet_config["read_for_flujo_caja"][0] is True:
                    db_name = sheet_config["db_name"]
                    table_name = sheet_config["table_name"]

                    df = self.db_hive.read(db_name, table_name)

                    data_frames[sheet_config["read_for_flujo_caja"][1]] = df

        logger.info(f"Dataframes generated with the following keys: {data_frames.keys()}")

        return data_frames


spark = SparkSession.builder.getOrCreate()

data_source_config = tables_config

data_processor = DataProcessing(spark, data_source_config)
data_frames = data_processor.read_data()

df_plp_upstream_v1 = data_frames.get("df_plp_upstream")
df_plp_upstream = data_frames.get("df_plp_upstream_v2")

df_plp_activo_upstream = data_frames.get("df_plp_activo_upstream")

df_crudos_portafolio_oc = data_frames.get("df_crudos_portafolio_oc")
df_crudos_portafolio_ot = data_frames.get("df_crudos_portafolio_ot")
df_crudos_portafolio_pm = data_frames.get("df_crudos_portafolio_pm")
df_crudos_portafolio_pn = data_frames.get("df_crudos_portafolio_pn")
df_factores_dilucion_fd = data_frames.get("df_factores_dilucion_fd")
df_intensidad_emisiones_up = data_frames.get("df_intensidad_emisiones_up")
df_gas_portafolio_pnb = data_frames.get("df_gas_portafolio_pnb")
df_ngl_portafolio = data_frames.get("df_ngl_portafolio")

from pyspark.sql.functions import col, monotonically_increasing_id, upper

selected_columns = [
    col("MATRICULA_DIGITAL"),
    col("LINEA_DE_NEGOCIO"),
    col("OPCION_ESTRATEGICA"),
    col("CATEGORIA_GAS_GLP"),
    col("SUBCATEGORIA_GAS_GLP"),
    col("EMPRESA"),
    col("SEGMENTO"),
    col("SUBSEGMENTO"),
    col("UN_NEGREGIONAL").alias("UN_NEG_REGIONAL"),
    col("GERENCIA"),
    col("ACTIVO"),
    upper(col("CAMPO")).alias("CAMPO").cast("string"),
    col("DEPARTAMENTO"),
    col("CUENCA_SEDIMENTARIA"),
    col("VENTURE"),
    col("TIPO_DE_INVERSION"),
    col("NOMBRE_PROYECTO"),
    col("NOMBRE_PEEP"),
    col("HIDROCARBURO"),
    col("GAS").alias("PORCENTAJE_GAS"),
    col("WI"),
    col("REGALIAS"),
    col("RECOBRO"),
    col("TECNOLOGIA"),
    col("CAPEX__LT_23").alias("CAPEX_MENOR_ANIO"),
    col("CAPEX_TOTAL"),
    col("CAPEX_EXPLORATORIO_GT_40").alias("CAPEX_MAYOR_ANIO"),
    col("CAPEX_EXP_ESPERADO_TOTAL"),
    col("VPN"),
    col("E_VPN").alias("EVPN"),
    col("VPI"),
    col("EFI"),
    col("TIR"),
    col("PAY_BACK"),
    col("BE_CRUDO").alias("B_E_CRUDO"),
    col("BE_GAS").alias("B_E_GAS"),
    col("VP_CAPEX_CASO_FALLA"),
    col("OPEX_UNITARIO"),
    col("CAPEX_UNITARIO"),
    col("FD_COST").alias("F&D_COST"),
    col("FIRST_OIL_OR_GAS"),
    col("TIME_TO_MARKET"),
    col("PROB_DESARROLLO"),
    col("PEPC").alias("PE_PC"),
    col("PG"),
    col("PX"),
    col("VOLUMENES_1P"),
    col("VOLUMENES_P2"),
    col("VOLUMENES_P3"),
    col("RECURSOS_CONTINGENTES"),
    col("REC_DESC_X_DELIMITARVERIFICAR_MEAN").alias("REC_DESC_X_DELIMITAR_VERIFICAR_MEAN"),
    col("REC_PROSP_MEAN").alias("REC_PROP_MEAN"),
    col("REC_TOT_E"),
    col("INVERSION_TOTAL_ASOCIADA_A_CTI").alias("INVERSION_ASOCIADA_CT_I"),
    col("APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI").alias("APORTE_EBITDA_ACUM_"),
    col("INV_SOCIAL_ESTRA_2426").alias("INV_SOCIAL_ESTRA_24_26"),
    col("INV_SOCIAL_ESTRA_TOTAL"),
    col("INV_SOCIAL_OBLIG_TOTAL"),
    col("INV_AMBIENTAL_OBLIG_TOTAL"),
    col("INV_AMBIENTAL_ESTRAT_TOTAL"),
    col("GESTION_INTEGRAL_DEL_AGUA").alias("INV_GESTION_INTEGRAL_AGUA_TOTAL"),
    col("RANGO_ANIO"),
    col("CAPEX"),
    col("PRODUCCION_CRUDO"),
    col("PRODUCCION_GAS"),
    col("PRODUCCION_BLANCOS"),
    col("PRODUCCION_EQUIVALENTE"),
    col("EMISIONES_NETAS_CO2"),
    col("PIR"),
    col("CAPEX_EXPLORATORIO")
]

df_data_sheet = df_plp_upstream.select(selected_columns)

def generate_random_alphanumeric_udf(length=8):
    alphanumeric_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_characters) for _ in range(length))

random_alphanumeric_udf = udf(generate_random_alphanumeric_udf, StringType())

df_data_sheet = df_data_sheet.withColumn(
    'MATRICULA_DIGITAL',
    when(col('MATRICULA_DIGITAL').isNull() | (col('MATRICULA_DIGITAL') == ""), random_alphanumeric_udf()).otherwise(col('MATRICULA_DIGITAL'))
)

# ELIMINACION DE NOMBRES REPETIDOS, ES TEMPORAL MIENTRAS SOLUCIONAN LOS DATOS DE PEEP

windowSpec = Window.partitionBy("NOMBRE").orderBy("CODIGO")

df_with_row_number = df_crudos_portafolio_oc.withColumn("row_num", row_number().over(windowSpec))
df_filtered = df_with_row_number.filter(col("row_num") <= 18).drop("row_num")
df_crudos_portafolio_oc = df_filtered

df_with_row_number_ot = df_crudos_portafolio_ot.withColumn("row_num", row_number().over(windowSpec))
df_filtered_ot = df_with_row_number_ot.filter(col("row_num") <= 18).drop("row_num")
df_crudos_portafolio_ot = df_filtered_ot

df_with_row_number_pm = df_crudos_portafolio_pm.withColumn("row_num", row_number().over(windowSpec))
df_filtered_pm = df_with_row_number_pm.filter(col("row_num") <= 18).drop("row_num")
df_crudos_portafolio_pm = df_filtered_pm

df_with_row_number_pm = df_ngl_portafolio.withColumn("row_num", row_number().over(windowSpec))
df_filtered_pm = df_with_row_number_pm.filter(col("row_num") <= 18).drop("row_num")
df_ngl_portafolio = df_filtered_pm

windowSpec_gas = Window.partitionBy("NOMBRE").orderBy("ANIO")
df_with_row_number_pm = df_gas_portafolio_pnb.withColumn("row_num", row_number().over(windowSpec_gas))
df_filtered_pm = df_with_row_number_pm.filter(col("row_num") <= 18).drop("row_num")
df_gas_portafolio_pnb = df_filtered_pm

# display(df_gas_portafolio_pnb.groupBy("NOMBRE").count().where("count > 17"))

from pyspark.sql.functions import coalesce, when, isnull


def peep_update_values_in_sabana_datos(df_data_sheet, df_crudos_portafolio_oc, annio_inicio, annio_fin,
                                       new_column_name):
    df_crudos_portafolio_oc = df_crudos_portafolio_oc.filter(
        df_crudos_portafolio_oc.ANIO.between(annio_inicio, annio_fin))

    df_temp = df_crudos_portafolio_oc.drop("CODIGO") \
        .withColumnRenamed("NOMBRE", "CAMPO_TEMP") \
        .withColumnRenamed("ANIO", "RANGO_ANIO_TEMP") \
        .withColumnRenamed("VALOR", "DESCUENTO_TEMP_CAMPO")

    df_temp2 = df_crudos_portafolio_oc.drop("CODIGO") \
        .withColumnRenamed("NOMBRE", "ACTIVO_TEMP") \
        .withColumnRenamed("ANIO", "RANGO_ANIO_TEMP2") \
        .withColumnRenamed("VALOR", "DESCUENTO_TEMP_ACTIVO")

    df_data_sheet = df_data_sheet.join(df_temp,
                                       (df_data_sheet.CAMPO == df_temp.CAMPO_TEMP) &
                                       (df_data_sheet.RANGO_ANIO == df_temp.RANGO_ANIO_TEMP),
                                       'left_outer') \
        .join(df_temp2,
              (df_data_sheet.ACTIVO == df_temp2.ACTIVO_TEMP) &
              (df_data_sheet.RANGO_ANIO == df_temp2.RANGO_ANIO_TEMP2),
              'left_outer')

    df_data_sheet = df_data_sheet.withColumn(new_column_name,
                                             coalesce(df_data_sheet.DESCUENTO_TEMP_CAMPO,
                                                      df_data_sheet.DESCUENTO_TEMP_ACTIVO)) \
        .drop("CAMPO_TEMP", "RANGO_ANIO_TEMP", "DESCUENTO_TEMP_CAMPO",
              "ACTIVO_TEMP", "RANGO_ANIO_TEMP2", "DESCUENTO_TEMP_ACTIVO")

    df_data_sheet = df_data_sheet.withColumn(new_column_name,
                                             when(isnull(df_data_sheet[new_column_name]), 0).otherwise(
                                                 df_data_sheet[new_column_name]))

    return df_data_sheet


df_data_sheet = peep_update_values_in_sabana_datos(df_data_sheet, df_crudos_portafolio_oc, annio_inicio, annio_fin,
                                                   "DESCUENTO_CALIDAD")
df_data_sheet = peep_update_values_in_sabana_datos(df_data_sheet, df_crudos_portafolio_ot, annio_inicio, annio_fin,
                                                   "PRECIO_TRANSPORTE")
df_data_sheet = peep_update_values_in_sabana_datos(df_data_sheet, df_crudos_portafolio_pm, annio_inicio, annio_fin,
                                                   "COSTO_DILUYENTE")
df_data_sheet = peep_update_values_in_sabana_datos(df_data_sheet, df_ngl_portafolio, annio_inicio, annio_fin,
                                                   "PRECIO_BLANCOS")
df_data_sheet = peep_update_values_in_sabana_datos(df_data_sheet, df_gas_portafolio_pnb, annio_inicio, annio_fin,
                                                   "PRECIO_GAS")

from pyspark.sql.functions import max, coalesce, col


def calculate_factor_dilucion(df, df_dilucion):
    original_order = df.columns

    max_dilucion_campo = df_dilucion.groupby('CAMPO').agg(max('FACTOR_DE_DILUCION').alias('dilucion_max'))

    df = df.join(max_dilucion_campo, on='CAMPO', how='left')

    max_dilucion_activo = df_dilucion.groupby('CAMPO').agg(max('FACTOR_DE_DILUCION').alias('dilucion_max_activo'))

    df = df.join(max_dilucion_activo.withColumnRenamed("CAMPO", "ACTIVO"), on='ACTIVO', how='left')

    df = df.withColumn("FACTOR_DILUCION", coalesce(col("dilucion_max"), col("dilucion_max_activo"))).drop(
        "dilucion_max", "dilucion_max_activo")
    df = df.select(*original_order, "FACTOR_DILUCION")
    df = df.fillna({'FACTOR_DILUCION': 0})

    return df

df_data_sheet = calculate_factor_dilucion(df_data_sheet, df_factores_dilucion_fd)

maximos = df_plp_activo_upstream.groupBy("ACTIVO").agg(max("INTENSIDAD_EMISIONES").alias("INTENSIDAD_CO2"))
df_data_sheet = df_data_sheet.join(maximos, on="ACTIVO", how="left")
df_data_sheet = df_data_sheet.fillna({'INTENSIDAD_CO2': 0})
