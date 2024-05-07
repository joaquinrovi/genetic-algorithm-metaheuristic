import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import unidecode
import string

from utils.databricks_hivemetastore import DatabricksHive
from utils.logger import Logger
from data_processing.flujo_caja_utils import (
   asignar_valor_up,
   expand_row,
   peep_update_values_in_sabana_datos,
   peep_update_values_in_sabana_datos_hocol,
   peep_update_values_in_sabana_datos_codigo,
   peep_update_values_in_sabana_datos_hocol_codigo,
   peep_gas_values_in_sabana_datos,
   plpl_add_new_column_in_sabana_datos,
   calculate_factor_dilucion,
   calcular_sobretasa,
   obtener_cantidad_dias,
   calcular_sobretasa_row,
   calcular_ingresos_crudo,
   calcular_ppe,
   procesar_grupo,
   calcular_impuestos,
   calcular_duracion_proyectos,
   plpl_add_new_column_in_sabana_datos_bajas_emisiones,
   calcular_depreciacion_hidrogeno,
   calcular_depreciacion_ccus,
   calcular_depreciacion_energia,
   asignar_valor_be,
   plpl_add_new_column_in_sabana_transmision,
   plpl_add_new_column_in_sabana_datos_icos_downstream,
   calcular_depreciacion_icos_downstream,
   calcular_impuestos_icos,
   fill_alphanumeric_for_nan_or_empty,
   asignar_valor_mid,
   plpl_add_new_column_in_df_sabana_midstream,
   calcular_depreciacion,
   asignar_valor_aacorp,
   plpl_add_new_column_in_sabana_aacorp,
   calcular_depreciacion_up,
   calcular_depreciacion_be,
   calcular_depreciacion_down,
   calcular_depreciacion_mid
)

logger = Logger(__name__).get_logger()

class DataProcessing:
    """
    This class provides methods for reading and processing data using pandas DataFrames.
    It is designed to work with specific configurations for data sources.

    Usage:
    data_source_config = {...} # Json config
    data_processor = DataProcessing(spark, data_source_config)
    data_frames = data_processor.read_data()
    processed_data = data_processor.process_data(data_frames)

    :param data_source_config: Configuration dictionary for data sources.
    :type data_source_config: dict
    """

    def __init__(self, data_source_config: dict, api_config: dict):
        self.flujo_caja_config = data_source_config
        self.api_config = api_config
        val_curva_crudo = str(api_config['val_curva_crudo'])
        val_curva_crudo = val_curva_crudo[0]
        self.flujo_caja_config["parametros"]["precio_crudo"] = [i * api_config["val_sensi_precio_crudo"] for i in
                                                self.flujo_caja_config["parametros"]
                                                [f"precio_crudo_{val_curva_crudo}"]]

        self.flujo_caja_config["parametros"]["precio_co2"] = [i * api_config["val_sensi_precio_co2"] for i in
                                              self.flujo_caja_config["parametros"]["precio_co2"]]


    def fc_up_be_isa_down(self):
        """
        Processes the given data frames and modifies the "df_gas_portafolio_pnb" DataFrame by capitalizing its columns.
        """
        
        #PLP
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_upstream_sn = self.flujo_caja_config["fuente"]["plp"]["upstream_config"]["upstream"]
        df_plp_upstream = pd.read_excel(file_path_up, sheet_name=plp_upstream_sn, header= 2, engine='openpyxl')

        #crudos portafolio
        file_path = self.flujo_caja_config["fuente"]["crudos_portafolio"]["dbfs_path_crudos"]
        precios_calidad = self.flujo_caja_config["fuente"]["crudos_portafolio"]["precios_config"]["sheet_name1"]
        precios_transporte = self.flujo_caja_config["fuente"]["crudos_portafolio"]["precios_config"]["sheet_name2"]
        precios_paseo_molecular = self.flujo_caja_config["fuente"]["crudos_portafolio"]["precios_config"]["sheet_name3"]


        df_peep_oc = pd.read_excel(file_path, sheet_name=precios_calidad, header= 3, engine='openpyxl')
        df_peep_oc_codigo = pd.read_excel(file_path, sheet_name=precios_calidad, header= 2, engine='openpyxl')
        df_peep_oc.columns = [col.capitalize() for col in df_peep_oc.columns]

        df_peep_ot = pd.read_excel(file_path, sheet_name=precios_transporte, header= 3, engine= 'openpyxl')
        df_peep_ot_codigo = pd.read_excel(file_path, sheet_name=precios_transporte, header= 2, engine= 'openpyxl')
        df_peep_ot.columns = [col.capitalize() for col in df_peep_ot.columns]

        df_peep_pm = pd.read_excel(file_path, sheet_name=precios_paseo_molecular, header= 3, engine= 'openpyxl')
        df_peep_pm.columns = [col.capitalize() for col in df_peep_pm.columns]

        #Blancos 
        file_path = self.flujo_caja_config["fuente"]["blancos"]["dbfs_path_blancos"]
        precios_blancos = self.flujo_caja_config["fuente"]["blancos"]["precios_config"]["sheet_name1"]
        df_peep_blanc = pd.read_excel(file_path, sheet_name=precios_blancos, header= 3, engine= 'openpyxl')
        df_peep_blanc_codigo = pd.read_excel(file_path, sheet_name=precios_blancos, header= 2, engine= 'openpyxl')
        df_peep_blanc.columns = [col.capitalize() for col in df_peep_blanc.columns]

        #Factores de dilucion
        file_path = self.flujo_caja_config ["fuente"]["factores_de_dilucion"]["dbfs_path_dilucion"]
        factores_dilucion = self.flujo_caja_config["fuente"]["factores_de_dilucion"]["dilucion_config"]["sheet_name1"]
        df_factor_dilucion = pd.read_excel(file_path, sheet_name=factores_dilucion, header= 0, engine='openpyxl')


        #Intensidad de emisiones
        file_path = self.flujo_caja_config ["fuente"]["intensidad"]["dbfs_path_intensidad"]
        precios_intensidad = self.flujo_caja_config["fuente"]["intensidad"]["intensidad_config"]["sheet_name1"]
        df_intensidad = pd.read_excel(file_path, sheet_name=precios_intensidad, header= 0, engine='openpyxl')

        #Precio del gas
        file_path =self.flujo_caja_config ["fuente"]["crudos_gas"]["dbfs_path_crudos_gas"]
        precios_gas = self.flujo_caja_config ["fuente"]["crudos_gas"]["precios_config"]["sheet"]
        df_gas = pd.read_excel(file_path, sheet_name=precios_gas, header= 12, engine='openpyxl')
        df_gas.columns = [col.capitalize() for col in df_gas.columns]


        #precios crudo hocol
        file_path =self.flujo_caja_config ["fuente"]["crudos_portafolio_hocol"]["dbfs_path_crudos_hocol"]
        precios_calidad = self.flujo_caja_config["fuente"]["crudos_portafolio_hocol"]["precios_config"]["sheet_name1"]
        precios_transporte_hocol = self.flujo_caja_config["fuente"]["crudos_portafolio"]["precios_config"]["sheet_name3"]


        df_peep_oc_hocol = pd.read_excel(file_path, sheet_name=precios_calidad, header= 3, engine='openpyxl')
        df_peep_oc_hocol_codigo = pd.read_excel(file_path, sheet_name=precios_calidad, header= 2, engine='openpyxl')
        df_peep_oc_hocol.columns = [col.capitalize() for col in df_peep_oc_hocol.columns]

        df_peep_ot_hocol = pd.read_excel(file_path, sheet_name=precios_transporte, header= 3, engine= 'openpyxl')
        df_peep_ot_hocol_codigo = pd.read_excel(file_path, sheet_name=precios_transporte, header= 2, engine= 'openpyxl')
        df_peep_ot_hocol.columns = [col.capitalize() for col in df_peep_ot_hocol.columns]


        #precios internacionales
        file_path =self.flujo_caja_config ["fuente"]["precios_internacionales"]["dbfs_path_crudos_internacionales"]
        precios = self.flujo_caja_config ["fuente"]["precios_internacionales"]["precios_config"]["sheet"]

        df_peep_precios_internacionales = pd.read_excel(file_path, sheet_name=precios, header= 3, engine='openpyxl')
        df_peep_precios_internacionales_codigo = pd.read_excel(file_path, sheet_name=precios, header= 2, engine='openpyxl')
        df_peep_precios_internacionales.columns = [col.capitalize() for col in df_peep_precios_internacionales.columns]


        #precios gas portafolio Hocol

        # df_activo_upstream = pd.DataFrame()
        # df_activo_upstream.loc[:, "ACTIVO"] = df_plp_activo_upstream.loc[:, "Activo"]
        # df_activo_upstream.loc[:, "INTENSIDAD_EMISIONES"] = df_plp_activo_upstream.loc[:, "Intensidad emisiones"]

        if not df_plp_upstream.empty: 
            
            df_sabana_datos = pd.DataFrame()
            df_sabana_datos.loc[:, "MATRICULA_DIGITAL"] = df_plp_upstream.loc[:, "Matrícula Digital"]
            df_sabana_datos.loc[:, "LINEA_DE_NEGOCIO"] = df_plp_upstream.loc[:, "Linea de Negocio"]
            df_sabana_datos.loc[:, "OPCION_ESTRATEGICA"] = df_plp_upstream.loc[:, "Opción estratégica"]
            df_sabana_datos.loc[:, "EMPRESA"] = df_plp_upstream.loc[:, "Empresa"]
            df_sabana_datos.loc[:, "SEGMENTO"] = df_plp_upstream.loc[:, "Segmento"].str.title()
            df_sabana_datos.loc[:, "SUBSEGMENTO"] = df_plp_upstream.loc[:, "Subsegmento"].str.title()
            df_sabana_datos.loc[:, "UN_NEG_REGIONAL"] = df_plp_upstream.loc[:, "Un. Neg./Regional"]
            df_sabana_datos.loc[:, "GERENCIA"] = df_plp_upstream.loc[:, "Gerencia"]
            df_sabana_datos.loc[:, "ACTIVO"] = df_plp_upstream.loc[:, "Activo"].str.capitalize()
            df_sabana_datos.loc[:, "CAMPO"] = df_plp_upstream.loc[:, "Campo "].str.capitalize()
            df_sabana_datos.loc[:, "TIPO_DE_INVERSION"] = df_plp_upstream.loc[:, "Tipo de inversión"]
            df_sabana_datos.loc[:, "NOMBRE_PROYECTO"] = df_plp_upstream.loc[:, "Nombre Proyecto"]
            df_sabana_datos.loc[:, "HIDROCARBURO"] = df_plp_upstream.loc[:, "Hidrocarburo"]
            df_sabana_datos.loc[:, "REGALIAS"] = df_plp_upstream.loc[:, "Regalías"].fillna(0)
            df_sabana_datos.loc[:, "FASE_EN_CURSO"] = df_plp_upstream.loc[:, "Fase en curso"]
            df_sabana_datos.loc[:, "CAPEX_MENOR_ANIO"] = df_plp_upstream.loc[:, "CapEx  < 23"].fillna(0)
            df_sabana_datos.loc[:, "CAPEX_TOTAL"] = df_plp_upstream.loc[:, "CapEx Total"].fillna(0)
            df_sabana_datos.loc[:, "CAPEX_MAYOR_ANIO"] = df_plp_upstream.loc[:, "CapEx Exploratorio > 40"].fillna(0) 
            df_sabana_datos.loc[:, "CAPEX_EXP_ESPERADO_TOTAL"] = df_plp_upstream.loc[:, "Capex Exp. Esperado Total"].fillna(0)
            df_sabana_datos.loc[:, "VPN"] = df_plp_upstream.loc[:, "VPN"].fillna(0)
            df_sabana_datos.loc[:, "EVPN"] = df_plp_upstream.loc[:, "(E) VPN"].fillna(0)
            df_sabana_datos.loc[:, "VPI"] =df_plp_upstream.loc[:, "VPI"].fillna(0)
            df_sabana_datos.loc[:, "EFI"] =df_plp_upstream.loc[:, "EFI"].fillna(0)  
            df_sabana_datos.loc[:, "TIR"] =df_plp_upstream.loc[:, "TIR"].fillna(0) 
            df_sabana_datos.loc[:, "PAY_BACK"] =df_plp_upstream.loc[:, "Pay Back"].fillna(0)   
            df_sabana_datos.loc[:, "B_E_CRUDO"] =df_plp_upstream.loc[:, "B.E Crudo"].fillna(0)  
            df_sabana_datos.loc[:, "B_E_GAS"] =df_plp_upstream.loc[:, "B.E Gas"].fillna(0)   
            df_sabana_datos.loc[:, "OPEX_UNITARIO"] =df_plp_upstream.loc[:, "Opex Unitario"].fillna(0)   
            df_sabana_datos.loc[:, "CAPEX_UNITARIO"] =df_plp_upstream.loc[:, "CapEx Unitario"].fillna(0)    
            df_sabana_datos.loc[:, "F&D_COST"] =df_plp_upstream.loc[:, "F&D Cost"].fillna(0) 
            df_sabana_datos.loc[:, "PROB_DESARROLLO"] =df_plp_upstream.loc[:, "Prob. Desarrollo"].fillna(0)  
            df_sabana_datos.loc[:, "PE_PC"] =df_plp_upstream.loc[:, "Pe/Pc"].fillna(0)  
            df_sabana_datos.loc[:, "PG"] =df_plp_upstream.loc[:, "Pg"].fillna(0)  
            df_sabana_datos.loc[:, "PX"] =df_plp_upstream.loc[:, "Px"].fillna(0)   
            df_sabana_datos.loc[:, "APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] =df_plp_upstream.loc[:, "Aporte EBITDA Acum 2040 proyectos asociados a CT+i"].fillna(0)  
            df_sabana_datos.loc[:, "CATEGORIA_TESG_PALANCAS"] =df_plp_upstream.loc[:, "Categoria TESG (Palancas)"]
            df_sabana_datos.loc[:, "COD_PRECIO_CRUDO"] =df_plp_upstream.loc[:, "Cod Precio Crudo"].fillna(0) 
            df_sabana_datos.loc[:, "COD_PRECIO_INT"] =df_plp_upstream.loc[:, "Cod Precio Int"].fillna(0) 
            df_sabana_datos.loc[:, "COD_PRECIO_HOCOL"] =df_plp_upstream.loc[:, "Cod Precio HOCOL"].fillna(0) 
            df_sabana_datos.loc[:, "COD_PRECIO_GAS"] = df_plp_upstream.loc[:, "Cod Precio Gas"].str.capitalize().fillna(0)
            df_sabana_datos.loc[:, "COD_PRECIO_BLANCOS"] = df_plp_upstream.loc[:, "Cod Precio Blancos"].fillna(0)  	  	

            df_sabana_datos.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_datos) + 1))
            
            #----------funcion_asignar_valor_up

            # Aplicar la función a la columna 'columna_sin_asignar' y asignar el resultado a una nueva columna
            df_sabana_datos['MATRICULA_DIGITAL'] = df_sabana_datos.apply(asignar_valor_up, axis=1)

            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            #-----------------------------------
            
            df_sabana_datos = pd.concat(df_sabana_datos.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

            df_sabana_datos.loc[:, "DESCUENTO_CALIDAD"] = np.nan
            df_sabana_datos.loc[:, "PRECIO_TRANSPORTE"] = np.nan
            df_sabana_datos.loc[:, "PRECIO_DILUYENTE"] = np.nan
            df_sabana_datos.loc[:, "PRECIO_CRUDO_INTERNACIONAL"] = np.nan


            peep_update_values_in_sabana_datos(
                peep_columns=df_peep_oc.columns, 
                df_sabana_datos=df_sabana_datos, 
                df_peep=df_peep_oc, 
                new_column_name="DESCUENTO_CALIDAD", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            peep_update_values_in_sabana_datos(
                peep_columns=df_peep_ot.columns, 
                df_sabana_datos=df_sabana_datos, 
                df_peep=df_peep_ot, 
                new_column_name="PRECIO_TRANSPORTE", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            peep_update_values_in_sabana_datos(
                peep_columns=df_peep_pm.columns, 
                df_sabana_datos=df_sabana_datos, 
                df_peep=df_peep_pm, 
                new_column_name="COSTO_DILUYENTE", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            peep_update_values_in_sabana_datos(
                peep_columns=df_peep_blanc.columns, 
                df_sabana_datos=df_sabana_datos, 
                df_peep=df_peep_blanc, 
                new_column_name="PRECIO_BLANCOS", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )


            df_sabana_datos["DESCUENTO_CALIDAD"] = df_sabana_datos["DESCUENTO_CALIDAD"].fillna(0) 
            df_sabana_datos["PRECIO_TRANSPORTE"] = df_sabana_datos["PRECIO_TRANSPORTE"].fillna(0) 
            df_sabana_datos["PRECIO_DILUYENTE"] = df_sabana_datos["PRECIO_DILUYENTE"].fillna(0)
            df_sabana_datos["PRECIO_BLANCOS"] = df_sabana_datos["PRECIO_BLANCOS"].fillna(0)
            df_sabana_datos["PRECIO_CRUDO_INTERNACIONAL"] = df_sabana_datos["PRECIO_CRUDO_INTERNACIONAL"].fillna(0)

            #--------------funcion

            df_sabana_datos = peep_update_values_in_sabana_datos_hocol(
                peep_columns = df_peep_oc_hocol.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_oc_hocol, 
                column_name_update = "DESCUENTO_CALIDAD", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos = peep_update_values_in_sabana_datos_hocol(
                peep_columns = df_peep_ot_hocol.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_ot_hocol, 
                column_name_update = "PRECIO_TRANSPORTE", 
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos["DESCUENTO_CALIDAD"] = df_sabana_datos["DESCUENTO_CALIDAD"].fillna(0) 
            df_sabana_datos["PRECIO_TRANSPORTE"] = df_sabana_datos["PRECIO_TRANSPORTE"].fillna(0) 

            #-------------------------------------------------------------------------------------------------------  

            

            df_sabana_datos = peep_update_values_in_sabana_datos_codigo(
                peep_columns = df_peep_oc_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_oc_codigo, 
                column_name_update = "DESCUENTO_CALIDAD", 
                column_revisar= "COD_PRECIO_CRUDO",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos = peep_update_values_in_sabana_datos_codigo(
                peep_columns = df_peep_ot_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_ot_codigo, 
                column_name_update = "PRECIO_TRANSPORTE",
                column_revisar= "COD_PRECIO_CRUDO",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos = peep_update_values_in_sabana_datos_codigo(
                peep_columns = df_peep_blanc_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_blanc_codigo, 
                column_name_update = "PRECIO_BLANCOS",
                column_revisar= "COD_PRECIO_BLANCOS",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos = peep_update_values_in_sabana_datos_codigo(
                peep_columns = df_peep_precios_internacionales_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_precios_internacionales_codigo, 
                column_name_update = "PRECIO_CRUDO_INTERNACIONAL",
                column_revisar= "COD_PRECIO_INT",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            #----------------funcion
                
            df_sabana_datos = peep_update_values_in_sabana_datos_hocol_codigo(
                peep_columns = df_peep_ot_hocol_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_oc_hocol_codigo, 
                column_name_update = "DESCUENTO_CALIDAD", 
                column_revisar= "COD_PRECIO_HOCOL",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos = peep_update_values_in_sabana_datos_hocol_codigo(
                peep_columns = df_peep_ot_hocol_codigo.columns, 
                df_sabana_datos = df_sabana_datos, 
                df_peep=df_peep_ot_hocol_codigo, 
                column_name_update = "PRECIO_TRANSPORTE", 
                column_revisar= "COD_PRECIO_HOCOL",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            #--------------funcion

            peep_gas_values_in_sabana_datos(
                peep_columns=df_gas.columns, 
                df_sabana_datos=df_sabana_datos, 
                df_peep=df_gas, 
                new_column_name="PRECIO_GAS",
                annio_inicio=annio_inicio, 
                annio_fin=annio_fin
                )

            df_sabana_datos["PRECIO_GAS"] = df_sabana_datos["PRECIO_GAS"].fillna(0)
            df_sabana_datos["PRECIO_GAS"] = df_sabana_datos["PRECIO_GAS"] * self.api_config["val_sensi_precio_gas"]

            #---------------funcion

            #capex 
            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']
            # df_plp_upstream.columns[df_plp_upstream.columns.str.match("CapEx \d+")]
            nombre_columna_capex = 'CAPEX'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)
            df_sabana_datos["CAPEX"] = df_sabana_datos["CAPEX"].fillna(0)

            #crudo-----------------------------------------------
            columnas_crudo = ['Prod. Crudo 24 Risked', 'Prod. Crudo 25 Risked', 'Prod. Crudo 26 Risked', 
                                        'Prod. Crudo 27 Risked', 'Prod. Crudo 28 Risked', 'Prod. Crudo 29 Risked', 'Prod. Crudo 30 Risked', 'Prod. Crudo 31 Risked', 'Prod. Crudo 32 Risked', 'Prod. Crudo 33 Risked',
                                        'Prod. Crudo 34 Risked', 'Prod. Crudo 35 Risked', 'Prod. Crudo 36 Risked', 'Prod. Crudo 37 Risked', 'Prod. Crudo 38 Risked', 'Prod. Crudo 39 Risked', 'Prod. Crudo 40 Risked']


            nueva_columna_crudo = 'PROD_CRUDO_RISKED'

            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_crudo, nueva_columna_crudo, annio_inicio, annio_fin)
            df_sabana_datos["PROD_CRUDO_RISKED"] = df_sabana_datos["PROD_CRUDO_RISKED"].fillna(0)

            #GAS ------------------------------------------
            columnas_gas = ['Prod. Gas 24 Risked', 'Prod. Gas 25 Risked', 'Prod. Gas 26 Risked', 'Prod. Gas 27 Risked', 
                                'Prod. Gas 28 Risked', 'Prod. Gas 29 Risked','Prod. Gas 30 Risked', 'Prod. Gas 31 Risked', 'Prod. Gas 32 Risked', 'Prod. Gas 33 Risked', 'Prod. Gas 34 Risked',
                                'Prod. Gas 35 Risked', 'Prod. Gas 36 Risked', 'Prod. Gas 37 Risked', 'Prod. Gas 38 Risked', 'Prod. Gas 39 Risked', 'Prod. Gas 40 Risked']

            nueva_columna_gas = 'PROD_GAS_RISKED'

            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_gas, nueva_columna_gas, annio_inicio, annio_fin)
            df_sabana_datos["PROD_GAS_RISKED"] = df_sabana_datos["PROD_GAS_RISKED"].fillna(0)

            # BLANCOS ----------------------------------------------------

            columnas_blancos = ['Blancos 24 Risked', 'Blancos 25 Risked', 'Blancos 26 Risked', 'Blancos 27 Risked', 'Blancos 28 Risked', 'Blancos 29 Risked', 
                                'Blancos 30 Risked', 'Blancos 31 Risked', 'Blancos 32 Risked', 'Blancos 33 Risked', 'Blancos 34 Risked', 'Blancos 35 Risked', 'Blancos 36 Risked', 
                                'Blancos 37 Risked', 'Blancos 38 Risked', 'Blancos 39 Risked', 'Blancos 40 Risked']


            nueva_columna_blancos = 'BLANCOS_RISKED'

            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_blancos, nueva_columna_blancos, annio_inicio, annio_fin)
            df_sabana_datos["BLANCOS_RISKED"] = df_sabana_datos["BLANCOS_RISKED"].fillna(0)

            # PROD EQUIVALENTE ------------------------------------------------------
            columnas_prod_equi = ['Prod. Equiv. 24 Risked','Prod. Equiv. 25 Risked','Prod. Equiv. 26 Risked','Prod. Equiv. 27 Risked','Prod. Equiv. 28 Risked',
                                'Prod. Equiv. 29 Risked', 'Prod. Equiv. 30 Risked','Prod. Equiv. 31 Risked', 'Prod. Equiv. 32 Risked', 'Prod. Equiv. 33 Risked', 'Prod. Equiv. 34 Risked',
                                'Prod. Equiv. 35 Risked', 'Prod. Equiv. 36 Risked', 'Prod. Equiv. 37 Risked', 'Prod. Equiv. 38 Risked','Prod. Equiv. 39 Risked','Prod. Equiv. 40 Risked']

            nueva_columna_prod_equi = 'PROD_EQUIV_RISKED'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_prod_equi, nueva_columna_prod_equi, annio_inicio, annio_fin)
            df_sabana_datos["PROD_EQUIV_RISKED"] = df_sabana_datos["PROD_EQUIV_RISKED"].fillna(0)

            #-----------------------------eMISONES NETAS
            #Emisiones Netas 
            columnas_EMISIONES_NETAS_CO2_ALCANCE_1_2 = ['Emisiones Netas Co2 Alcance 1 y 2 2024','Emisiones Netas Co2 Alcance 1 y 2 2025','Emisiones Netas Co2 Alcance 1 y 2 2026','Emisiones Netas Co2 Alcance 1 y 2 2027',
                                        'Emisiones Netas Co2 Alcance 1 y 2 2028','Emisiones Netas Co2 Alcance 1 y 2 2029','Emisiones Netas Co2 Alcance 1 y 2 2030','Emisiones Netas Co2 Alcance 1 y 2 2031','Emisiones Netas Co2 Alcance 1 y 2 2032',
                                        'Emisiones Netas Co2 Alcance 1 y 2 2033','Emisiones Netas Co2 Alcance 1 y 2 2034','Emisiones Netas Co2 Alcance 1 y 2 2035','Emisiones Netas Co2 Alcance 1 y 2 2036','Emisiones Netas Co2 Alcance 1 y 2 2037', 
                                        'Emisiones Netas Co2 Alcance 1 y 2 2038', 'Emisiones Netas Co2 Alcance 1 y 2 2039','Emisiones Netas Co2 Alcance 1 y 2 2040']

            nueva_columna_EMISIONES_NETAS_CO2_ALCANCE_1_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos,columnas_EMISIONES_NETAS_CO2_ALCANCE_1_2, nueva_columna_EMISIONES_NETAS_CO2_ALCANCE_1_2, annio_inicio, annio_fin)
            df_sabana_datos["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_sabana_datos["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)


            #-----------------------------CAPEX EXPLORATORIO

            columnas_Capex_exploratorio = ['CapEx Exploratorio 24','CapEx Exploratorio 25', 'CapEx Exploratorio 26', 'CapEx Exploratorio 27', 'CapEx Exploratorio 28', 'CapEx Exploratorio 29', 'CapEx Exploratorio 30','CapEx Exploratorio 31',
                                            'CapEx Exploratorio 32', 'CapEx Exploratorio 33', 'CapEx Exploratorio 34', 'CapEx Exploratorio 35', 'CapEx Exploratorio 36', 'CapEx Exploratorio 37', 'CapEx Exploratorio 38', 'CapEx Exploratorio 39', 'CapEx Exploratorio 40']

            nueva_columna_capex_exploratorio = 'CAPEX_EXPLORATORIO'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_Capex_exploratorio, nueva_columna_capex_exploratorio, annio_inicio, annio_fin)
            df_sabana_datos["CAPEX_EXPLORATORIO"] = df_sabana_datos["CAPEX_EXPLORATORIO"].fillna(0)

            #-----------------------------AGUA NEUTRALIDAD

            columnas_agua_neutralidad = ['Agua Neutralidad 2024', 	 
                                        'Agua Neutralidad 2025', 'Agua Neutralidad 2026',	
                                        'Agua Neutralidad 2027', 'Agua Neutralidad 2028', 	
                                        'Agua Neutralidad 2029', 'Agua Neutralidad 2030',
                                        'Agua Neutralidad 2031', 'Agua Neutralidad 2032',
                                        'Agua Neutralidad 2033', 'Agua Neutralidad 2034',
                                        'Agua Neutralidad 2035', 'Agua Neutralidad 2036',	 
                                        'Agua Neutralidad 2037', 'Agua Neutralidad 2038',	 
                                        'Agua Neutralidad 2039', 'Agua Neutralidad 2040' ]

            nueva_columna_agua_neutralidad = 'AGUA_NEUTRALIDAD'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_agua_neutralidad, nueva_columna_agua_neutralidad, annio_inicio, annio_fin)
            df_sabana_datos["AGUA_NEUTRALIDAD"] = df_sabana_datos["AGUA_NEUTRALIDAD"].fillna(0)

            #-----------------------------Empleos NP 

            columnas_empleon_np= ['Empleos NP 2024','Empleos NP 2025',	
                                    'Empleos NP 2026','Empleos NP 2027','Empleos NP 2028',
                                    'Empleos NP 2029','Empleos NP 2030','Empleos NP 2031',
                                    'Empleos NP 2032','Empleos NP 2033','Empleos NP 2034',
                                    'Empleos NP 2035','Empleos NP 2036','Empleos NP 2037',
                                    'Empleos NP 2038','Empleos NP 2039','Empleos NP 2040' ]

            nueva_columna_empleon = 'EMPLEOS_NP'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_empleon_np, nueva_columna_empleon, annio_inicio, annio_fin)
            df_sabana_datos["EMPLEOS_NP"] = df_sabana_datos["EMPLEOS_NP"].fillna(0)

            #----------------------------------------------Estudiantes

            columnas= ['Estudiantes 2024', 'Estudiantes 2025',
                                    'Estudiantes 2026', 'Estudiantes 2027', 'Estudiantes 2028',
                                    'Estudiantes 2029', 'Estudiantes 2030', 'Estudiantes 2031', 
                                    'Estudiantes 2032', 'Estudiantes 2033', 'Estudiantes 2034',	
                                    'Estudiantes 2035', 'Estudiantes 2036', 'Estudiantes 2037',
                                    'Estudiantes 2038',	'Estudiantes 2039',	'Estudiantes 2040' ]

            nueva_columna= 'ESTUDIANTES'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas, nueva_columna, annio_inicio, annio_fin)
            df_sabana_datos["ESTUDIANTES"] = df_sabana_datos["ESTUDIANTES"].fillna(0)

            #----------------------------------ACCESO AGUA POTABLE
            columnas_acceso_potable= ['Acceso agua potable 2024', 'Acceso agua potable 2025',	 'Acceso agua potable 2026','Acceso agua potable 2027',	'Acceso agua potable 2028',	'Acceso agua potable 2029',	'Acceso agua potable 2030',	'Acceso agua potable 2031',	'Acceso agua potable 2032',	'Acceso agua potable 2033',	'Acceso agua potable 2034',	'Acceso agua potable 2035',	'Acceso agua potable 2036',	'Acceso agua potable 2037',	'Acceso agua potable 2038',	'Acceso agua potable 2039','Acceso agua potable 2040']

            nueva_columna_acceso_agua_potable = 'ACCESO_AGUA_POTABLE'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_acceso_potable, nueva_columna_acceso_agua_potable, annio_inicio, annio_fin)
            df_sabana_datos["ACCESO_AGUA_POTABLE"] = df_sabana_datos["ACCESO_AGUA_POTABLE"].fillna(0)


            #-----------------ACCESO A GAS
            columnas_acceso_gas= ['Acceso Gas 2024','Acceso Gas 2025', 'Acceso Gas 2026','Acceso Gas 2027','Acceso Gas 2028','Acceso Gas 2029','Acceso Gas 2030','Acceso Gas 2031','Acceso Gas 2032',	'Acceso Gas 2033','Acceso Gas 2034','Acceso Gas 2035','Acceso Gas 2036','Acceso Gas 2037','Acceso Gas 2038',	'Acceso Gas 2039','Acceso Gas 2040']

            nueva_columna_acceso_gas= 'ACCESO_GAS'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_datos["ACCESO_GAS"] = df_sabana_datos["ACCESO_GAS"].fillna(0)
                    
            #-----------------KM
            columnas_acceso_km= ['Km 2024', 'Km 2025', 'Km 2026', 'Km 2027', 'Km 2028', 'Km 2029', 'Km 2030', 'Km 2031', 'Km 2032', 'Km 2033', 'Km 2034', 'Km 2035', 'Km 2036', 'Km 2037', 'Km 2038', 'Km 2039	Km 2040']

            nueva_columna_acceso_gas= 'KM'
            df_sabana_datos = plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_datos["KM"] = df_sabana_datos["KM"].fillna(0)


            df_sabana_datos = df_sabana_datos.loc[:, ~df_sabana_datos.columns.duplicated()]

            #adicionar factores dilucion 

            df_dilucion = df_factor_dilucion.drop(["Unnamed: 0","Código"],axis=1)
            df_dilucion.rename(columns={"Campo":"campo"}, inplace=True)
            df_dilucion.rename(columns={"Factor de dilución":"dilucion"}, inplace=True)

            df_dilucion['campo'] = df_dilucion['campo'].str.lower()
            df_sabana_datos['CAMPO'] = df_sabana_datos['CAMPO'].str.lower()
            df_sabana_datos['ACTIVO'] = df_sabana_datos['ACTIVO'].str.lower()

            df_sabana_datos['FACTOR_DILUCION'] = None 

            #---------------funcion
            
            df_sabana_datos = calculate_factor_dilucion(df_sabana_datos, df_dilucion)

            # #ADICIONAR INTENSIDAD DE LA HOJA DE ACTIVOS 
            # df_activo_upstream['ACTIVO'] = df_activo_upstream['ACTIVO'].str.lower()
            # df_sabana_datos['CAMPO'] = df_sabana_datos['CAMPO'].str.lower()
            # df_sabana_datos['ACTIVO'] = df_sabana_datos['ACTIVO'].str.lower()

            # df_sabana_datos.loc[:, 'INTENSIDAD_CO2'] = None 


            # maximos = df_activo_upstream.groupby('ACTIVO')['INTENSIDAD_EMISIONES'].max()
            # df_sabana_datos['INTENSIDAD_CO2'] = df_sabana_datos['ACTIVO'].map(maximos)

            # df_sabana_datos['INTENSIDAD_CO2'] = df_sabana_datos['INTENSIDAD_CO2'].fillna(0)

            #---------------funcion_percentiles
            #---------------funcion_percentil.exc
            #---------------funcion_sobretasa    
            
            
                
            # Adiciono las columnas precio_crudo, precio_co2, Cantiad_dias_anio
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]
            precio_C02 =  self.flujo_caja_config["parametros"]["precio_co2"]
            factor_conversion = self.flujo_caja_config["parametros"]["factor_conversion"]
            payout =  self.flujo_caja_config["parametros"]["payout"] 
            impuesto_renta =  self.flujo_caja_config["parametros"]["impuesto_renta"]
            impuesto_renta_20=  self.flujo_caja_config["parametros"]["impuesto_renta_20"]
            impuesto_internacional = self.flujo_caja_config["parametros"]["impuesto_internacional"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios= list(range(annio_inicio, annio_fin))

            lista_anios_dias = []
            for anio in range(annio_inicio, annio_fin):
                cantidad_dias = obtener_cantidad_dias(anio)
                lista_anios_dias.append((cantidad_dias))

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios ,
                    'PRECIO_CRUDO_BRENT': precio_crudo,
                    'PRECIO_CO2': precio_C02, 
                    'CANT_DIAS_ANIO':lista_anios_dias, 
                    'IMPUESTO_RENTA': impuesto_renta,
                    'PAYOUT':payout,
                    'FACTOR_CONVERSION': factor_conversion,
                    'IMPUESTO_RENTA_20': impuesto_renta_20,
                    'IMPUESTO_INTERNACIONAL': impuesto_internacional
                    }
            data_anio = {'RANGO_ANIO': anios , 'PRECIO_CRUDO_BRENT': precio_crudo}
            df_anio_crudo= pd.DataFrame(data)

            df_sabana_datos = df_sabana_datos.merge(df_anio_crudo, on='RANGO_ANIO', how='left')
            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]

            #Hallar la sobretasa 
            #Consultar paramentros 
            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anios = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Replicar registros de df1 para que coincida con la longitud de df2
            replicated_df1 = pd.concat([df_sobretasa] * (len(df_sabana_datos) // len(df_sobretasa)), ignore_index=True)

            # Asegurarse de que la longitud de replicated_df1 coincida con la longitud de df2
            replicated_df1 = replicated_df1.iloc[:len(df_sabana_datos)]

            df_sabana_datos['SOBRETASA'] = replicated_df1['SOBRETASA']

            #---------------funcion

            df_sabana_datos['SOBRETASA'] = df_sabana_datos.apply(calcular_sobretasa_row, axis=1)

            #llenar las columnas que estan con NaN en cero 
            df_sabana_datos['PROD_CRUDO_RISKED'] = df_sabana_datos['PROD_CRUDO_RISKED'].fillna(0)
            df_sabana_datos['PROD_GAS_RISKED'] = df_sabana_datos['PROD_GAS_RISKED'].fillna(0)
            df_sabana_datos['BLANCOS_RISKED'] = df_sabana_datos['BLANCOS_RISKED'].fillna(0)
            df_sabana_datos['CAPEX'] =  df_sabana_datos['CAPEX'].fillna(0)
            df_sabana_datos['REGALIAS'] =  df_sabana_datos['REGALIAS'].fillna(0)
            df_sabana_datos['PRECIO_CO2'] = df_sabana_datos['PRECIO_CO2'].fillna(0)
            df_sabana_datos['PROD_EQUIV_RISKED'] = df_sabana_datos['PROD_EQUIV_RISKED'].fillna(0)
            df_sabana_datos['OPEX_UNITARIO'] =  df_sabana_datos['OPEX_UNITARIO'].fillna(0)
            df_sabana_datos['PRECIO_TRANSPORTE'] = df_sabana_datos['PRECIO_TRANSPORTE'].fillna(0)
            df_sabana_datos['FACTOR_DILUCION'] = df_sabana_datos['FACTOR_DILUCION'].fillna(0)
            df_sabana_datos['PRECIO_DILUYENTE'] = df_sabana_datos['PRECIO_DILUYENTE'].fillna(0)
            df_sabana_datos['VPN'] = df_sabana_datos['VPN'].fillna(0)
            df_sabana_datos['EVPN'] = df_sabana_datos['EVPN'].fillna(0)


            # convertir columnas a valores float
            df_sabana_datos['REGALIAS'] = pd.to_numeric(df_sabana_datos['REGALIAS'], errors='coerce')
            df_sabana_datos['PROD_CRUDO_RISKED'] = pd.to_numeric(df_sabana_datos['PROD_CRUDO_RISKED'], errors='coerce')
            df_sabana_datos['PROD_GAS_RISKED'] = pd.to_numeric(df_sabana_datos['PROD_GAS_RISKED'], errors='coerce')
            df_sabana_datos['BLANCOS_RISKED'] = pd.to_numeric(df_sabana_datos['BLANCOS_RISKED'], errors='coerce')
            df_sabana_datos['CAPEX'] = pd.to_numeric(df_sabana_datos['CAPEX'], errors='coerce')
            df_sabana_datos['PRECIO_CO2'] = pd.to_numeric(df_sabana_datos['PRECIO_CO2'], errors='coerce')
            df_sabana_datos['PRECIO_TRANSPORTE'] = pd.to_numeric(df_sabana_datos['PRECIO_TRANSPORTE'], errors='coerce')
            df_sabana_datos['PROD_EQUIV_RISKED'] = pd.to_numeric(df_sabana_datos['PROD_EQUIV_RISKED'], errors='coerce')


            df_sabana_datos['VOLUMENES_PROD_CRUDO_RISKED_ANTES_R'] =  (df_sabana_datos['PROD_CRUDO_RISKED']  * df_sabana_datos['CANT_DIAS_ANIO']) / 1000
            df_sabana_datos['VOLUMENES_PROD_GAS_RISKED_ANTES_R'] =  (df_sabana_datos['PROD_GAS_RISKED']  * df_sabana_datos['CANT_DIAS_ANIO']) / 1000
            df_sabana_datos['VOLUMENES_BLANCOS_RISKED_ANTES_R'] = (df_sabana_datos['BLANCOS_RISKED'] * df_sabana_datos['CANT_DIAS_ANIO']) / 1000
            df_sabana_datos['TOTAL_VOLUMENES_PROD_ANTES_R'] = df_sabana_datos['VOLUMENES_PROD_CRUDO_RISKED_ANTES_R'] + df_sabana_datos['VOLUMENES_PROD_GAS_RISKED_ANTES_R'] +  df_sabana_datos['VOLUMENES_BLANCOS_RISKED_ANTES_R']

            df_sabana_datos['TOTAL_VOLUMENES_PROD_ANTES_R'] = pd.to_numeric(df_sabana_datos['TOTAL_VOLUMENES_PROD_ANTES_R'], errors='coerce')



            #Produccion Neta ECP antes de R	 ----------------------------------------------------------------------
            df_sabana_datos['PROD_GAS_RISKED_FACTOR'] = df_sabana_datos['PROD_GAS_RISKED'] * df_sabana_datos['FACTOR_CONVERSION']
            df_sabana_datos['TOTAL_PROD_NETA_ANTES_REGALIAS'] = df_sabana_datos['PROD_CRUDO_RISKED'] + df_sabana_datos['PROD_GAS_RISKED_FACTOR'] + df_sabana_datos['BLANCOS_RISKED'] 
            df_sabana_datos['TOTAL_PROD_NETA_ANTES_REGALIAS'] = pd.to_numeric(df_sabana_datos['TOTAL_PROD_NETA_ANTES_REGALIAS'], errors='coerce')


            #Regalias ----------------------------------------------------------------------------------------------------------------	
            df_sabana_datos['PCRUDO_REGALIAS'] = df_sabana_datos['REGALIAS'] * df_sabana_datos['PROD_CRUDO_RISKED']
            df_sabana_datos['PGAS_REGALIAS'] = df_sabana_datos['REGALIAS'] * df_sabana_datos['PROD_GAS_RISKED']
            df_sabana_datos['PGAS_REGALIAS_FACTOR'] = df_sabana_datos['PGAS_REGALIAS'] * df_sabana_datos['FACTOR_CONVERSION']  
            df_sabana_datos['PBLANCOS_REGALIAS'] = df_sabana_datos['REGALIAS'] * df_sabana_datos['BLANCOS_RISKED']
            df_sabana_datos['TOTAL_REGALIAS'] = df_sabana_datos['PCRUDO_REGALIAS'] +  df_sabana_datos['PGAS_REGALIAS'] + df_sabana_datos['PBLANCOS_REGALIAS']


            # Calculos Produccion Neta ECP despues de R (PRODUCCION - REGALIAS ) ------------------------------------------------------
            df_sabana_datos['CRUDO_DESPUES_R'] = df_sabana_datos['PROD_CRUDO_RISKED'] - df_sabana_datos['PCRUDO_REGALIAS']
            df_sabana_datos['GAS_DESPUES_R']  =  df_sabana_datos['PROD_GAS_RISKED'] - df_sabana_datos['PGAS_REGALIAS']                      
            df_sabana_datos['GAS_DESPUES_R_FACTOR'] = df_sabana_datos['GAS_DESPUES_R'] * df_sabana_datos['FACTOR_CONVERSION']  
            df_sabana_datos['BLANCOS_DESPUES_R'] = df_sabana_datos['BLANCOS_RISKED'] - df_sabana_datos['PBLANCOS_REGALIAS']	
            df_sabana_datos['TOTAL_PROD_DESPUES_R'] = df_sabana_datos['CRUDO_DESPUES_R'] + df_sabana_datos['GAS_DESPUES_R'] + df_sabana_datos['BLANCOS_DESPUES_R'] 


            #VolumEN produccion  despues de R --------------------------------------------------------------------------------------------			
            df_sabana_datos['VOLUMEN_CRUDO_DESPUES_R'] = (df_sabana_datos['CRUDO_DESPUES_R'] * df_sabana_datos['CANT_DIAS_ANIO'])/ 1000
            df_sabana_datos['VOLUMEN_GAS_DESPUES_R'] = (df_sabana_datos['GAS_DESPUES_R']  * df_sabana_datos['CANT_DIAS_ANIO']) / 1000
            df_sabana_datos['VOLUMEN_GAS_FACTOR'] = df_sabana_datos['VOLUMEN_GAS_DESPUES_R'] * df_sabana_datos['FACTOR_CONVERSION'] 
            df_sabana_datos['VOLUMEN_BLANCOS_DESPUES_R'] = (df_sabana_datos['BLANCOS_DESPUES_R'] * df_sabana_datos['CANT_DIAS_ANIO']) / 1000
            df_sabana_datos['TOTAL_VOLUMENES_PROD_DESPUES_R'] = df_sabana_datos['VOLUMEN_CRUDO_DESPUES_R']+  df_sabana_datos['VOLUMEN_GAS_DESPUES_R']  +  df_sabana_datos['VOLUMEN_BLANCOS_DESPUES_R'] 

            df_sabana_datos['TOTAL_VOLUMENES_PROD_DESPUES_R'] = pd.to_numeric(df_sabana_datos['TOTAL_VOLUMENES_PROD_DESPUES_R'], errors='coerce')

            #-------------------------------------------------------------------------------------------------------------------------------------
            
            #-----------funcion


            # INGRESOS  
            df_sabana_datos['INGRESOS_CRUDO'] = df_sabana_datos.apply(calcular_ingresos_crudo, axis=1)
            # df_sabana_datos["INGRESOS_CRUDO"] = (df_sabana_datos["PRECIO_CRUDO_BRENT"] + df_sabana_datos["DESCUENTO_CALIDAD"]) * df_sabana_datos["VOLUMEN_CRUDO_DESPUES_R"]  
            df_sabana_datos["INGRESOS_CRUDO"] = df_sabana_datos["INGRESOS_CRUDO"].fillna(0)

            df_sabana_datos['INGRESOS_GAS'] = df_sabana_datos['PRECIO_GAS'] * df_sabana_datos['VOLUMEN_GAS_FACTOR']
            df_sabana_datos["INGRESOS_GAS"] = df_sabana_datos["INGRESOS_GAS"].fillna(0) 

            df_sabana_datos['INGRESOS_BLANCOS'] =  (df_sabana_datos['PRECIO_BLANCOS'] * df_sabana_datos['VOLUMEN_BLANCOS_DESPUES_R'])
            df_sabana_datos["INGRESOS_BLANCOS"] = df_sabana_datos["INGRESOS_BLANCOS"].fillna(0)

            df_sabana_datos['TOTAL_INGRESOS'] = df_sabana_datos['INGRESOS_CRUDO'] + df_sabana_datos['INGRESOS_GAS']  + df_sabana_datos['INGRESOS_BLANCOS'] 
            df_sabana_datos['TOTAL_INGRESOS'] = df_sabana_datos["TOTAL_INGRESOS"].fillna(0) 

            #----------------------------------------
            #Costos CO2		 
            df_sabana_datos['COSTO_INTERNO_CO2'] = df_sabana_datos['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_datos['PRECIO_CO2']

            df_sabana_datos['OPEX_UNITARIO'] = pd.to_numeric(df_sabana_datos['OPEX_UNITARIO'], errors='coerce')

            #Transporte	
            #Tarifa Trasnporte 	
            df_sabana_datos['COSTO_TRANSPORTE'] =  (df_sabana_datos['PRECIO_TRANSPORTE'] * -1 ) * df_sabana_datos['VOLUMEN_CRUDO_DESPUES_R']

            #Dilucion		
            #FACTOR_DILUCION	--> campo armado en la sabana df_sabana_datos['FACTOR_DILUCION']
            #Costo diluyente 	
            df_sabana_datos['COSTO_DILUCION'] = df_sabana_datos['FACTOR_DILUCION'] * df_sabana_datos['PRECIO_DILUYENTE']
            # Costo Dilución 

            df_sabana_datos['OPEX_UNITARIO_TOTAL'] = (df_sabana_datos['TOTAL_VOLUMENES_PROD_DESPUES_R'] *  df_sabana_datos['OPEX_UNITARIO']) 
            df_sabana_datos['TOTAL_COSTOS'] = (df_sabana_datos['TOTAL_VOLUMENES_PROD_DESPUES_R'] *  df_sabana_datos['OPEX_UNITARIO']) + df_sabana_datos['COSTO_TRANSPORTE'] + df_sabana_datos['COSTO_INTERNO_CO2'] 

            #Valor de las Regalias para la nación	
            df_sabana_datos['CRUDO_R_NACION']= (df_sabana_datos['PCRUDO_REGALIAS'] * (df_sabana_datos["PRECIO_CRUDO_BRENT"] + df_sabana_datos["DESCUENTO_CALIDAD"]) *  df_sabana_datos['CANT_DIAS_ANIO'])/ 1000
            df_sabana_datos['GAS_R_NACION'] = (df_sabana_datos['PGAS_REGALIAS_FACTOR'] * df_sabana_datos["PRECIO_GAS"] * df_sabana_datos['CANT_DIAS_ANIO']) /1000
            df_sabana_datos['BLANCOS_R_NACION'] =  (df_sabana_datos['PBLANCOS_REGALIAS'] *  df_sabana_datos["PRECIO_BLANCOS"] * df_sabana_datos['CANT_DIAS_ANIO']) /1000
            df_sabana_datos['TOTAL_R_NACION'] = df_sabana_datos['CRUDO_R_NACION'] + df_sabana_datos['GAS_R_NACION']  +  df_sabana_datos['BLANCOS_R_NACION']

            #--------------funcion_prefijo
            #---------------funcion_procesar_amortizacion_depreciacion
            #--------------funcion_ppe
            #----------------funcion_procesar_grupo       


            df_factor_amortizacion = df_sabana_datos[['ID_PROYECTO','MATRICULA_DIGITAL','RANGO_ANIO', 'TOTAL_VOLUMENES_PROD_DESPUES_R', 'CAPEX']]

            # Aplicar procesar_grupo a cada grupo de ID_PROYECTO
            grupos_procesados = [procesar_grupo(grupo) for _, grupo in df_factor_amortizacion.groupby('ID_PROYECTO')]

            # Concatenar los resultados
            df_total = pd.concat(grupos_procesados, ignore_index=True)

            df_sabana_datos = pd.concat([df_sabana_datos, df_total], axis='columns')
            df_sabana_datos = df_sabana_datos.loc[:, ~df_sabana_datos.columns.duplicated()]


            # IMPUESTOS			----------------------------------------------------------------------------------------								
            df_sabana_datos['EBITDA']  = df_sabana_datos['TOTAL_INGRESOS'] - df_sabana_datos['TOTAL_COSTOS']  # EBITDA

            df_sabana_datos['EBITDA'] = df_sabana_datos['EBITDA'].fillna(0)

            #---------------------------------------------------------------------------------------------------------------------------------------
            df_sabana_datos['CAPEX_ACUM'] = df_sabana_datos['CAPEX'] # Capex Acum	
            df_sabana_datos['MAS_REGALIAS']  = df_sabana_datos['TOTAL_R_NACION'] 
            # Costo de prod Regalias	
            #df_sabana_datos['COSTO_PROD_REGALIAS_CRUDO'] = df_sabana_datos['CRUDO_R_NACION'] + df_sabana_datos['BLANCOS_R_NACION']
            df_sabana_datos['COSTO_PROD_REGALIAS_CRUDO'] = (df_sabana_datos['TOTAL_REGALIAS'] *  df_sabana_datos['OPEX_UNITARIO'] * df_sabana_datos['CANT_DIAS_ANIO']) /1000  


            # Base Grabable (EBITDA-Amort -Costo Regalias - depreciacion y amortizacion )	
            df_sabana_datos['BASE_GRABABLE'] = (df_sabana_datos['EBITDA']  - df_sabana_datos['SUM_DEPRE']) + df_sabana_datos['COSTO_INTERNO_CO2'] 


            #----------------funcion

            # Aplicamos la función 'calcular_impuestos' a cada fila del DataFrame y almacenamos los resultados en una nueva columna 'Impuestos'
            df_sabana_datos['IMPUESTOS'] = df_sabana_datos.apply(calcular_impuestos, axis=1)


            # Regalias no son deducibles, solo descuento el Lifting y una deprecision unitaria	Impuestos
            # impuestos 
            #df_sabana_datos['IMPUESTOS']= df_sabana_datos['BASE_GRABABLE'] *(df_sabana_datos['IMPUESTO_RENTA']+ df_sabana_datos['SOBRETASA'])

            # capex	 df_sabana_datos['CAPEX']
            # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
            df_sabana_datos['FLUJO_CAJA']= df_sabana_datos['EBITDA']  - df_sabana_datos['IMPUESTOS'] - df_sabana_datos['CAPEX']
            
            df_sabana_datos['EBIT'] =  df_sabana_datos['EBITDA'] - df_sabana_datos['IMPUESTOS']

            # Dividendo	 utilidad * payout
            df_sabana_datos['DIVIDENDO'] = df_sabana_datos['EBIT'] * (df_sabana_datos['PAYOUT'])

            df_sabana_datos["CAPITAL_EMPLEADO"] = df_sabana_datos["CAPITAL_EMPLEADO"].fillna(0) 
            df_sabana_datos['EBIT'] = df_sabana_datos["EBIT"].fillna(0)

            # df_sabana_datos['ROACE'] = (df_sabana_datos['EBIT'] / df_sabana_datos['CAPITAL_EMPLEADO']) 
            # df_sabana_datos['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
            # df_sabana_datos['ROACE'] = df_sabana_datos["ROACE"].fillna(0)


            #-----------------------------funcion
                
            df_sabana_datos = df_sabana_datos.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)
        else:
            logger.info(f"El DataFrame upstream (df_plp_upstream) está vacío.") 

        columnas_deseadas = ['ID_PROYECTO','MATRICULA_DIGITAL', 'LINEA_DE_NEGOCIO' , 'OPCION_ESTRATEGICA', 
                            'EMPRESA',  'SEGMENTO',  'SUBSEGMENTO','UN_NEG_REGIONAL', 'GERENCIA',  'ACTIVO',  'CAMPO',  'TIPO_DE_INVERSION', 
                            'NOMBRE_PROYECTO',  'HIDROCARBURO','FASE_EN_CURSO',  'CAPEX_MENOR_ANIO',  'CAPEX_TOTAL',  'CAPEX_MAYOR_ANIO', 
                            'CAPEX_EXP_ESPERADO_TOTAL',  'VPN',  'EVPN',  'VPI',  'EFI',  'TIR',  'PAY_BACK',  'B_E_CRUDO',  'B_E_GAS',
                            'CAPEX_UNITARIO',  'F&D_COST',  'PROB_DESARROLLO',  'PE_PC',  'PG',  'PX',
                            'APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI', 'CATEGORIA_TESG_PALANCAS',   
                            'RANGO_ANIO',  'CANT_DIAS_ANIO',  'PRECIO_CRUDO_BRENT', 'PRECIO_CRUDO_INTERNACIONAL', 'PRECIO_GAS',  'PRECIO_BLANCOS',
                            'PRECIO_CO2',  'DESCUENTO_CALIDAD',  'PRECIO_TRANSPORTE',  'OPEX_UNITARIO',  'REGALIAS',  'IMPUESTO_RENTA', 'IMPUESTO_RENTA_20', 'IMPUESTO_INTERNACIONAL',
                            'SOBRETASA',  'PROD_CRUDO_RISKED',  'PROD_GAS_RISKED',  'PROD_GAS_RISKED_FACTOR',  'BLANCOS_RISKED',
                            'TOTAL_PROD_NETA_ANTES_REGALIAS','PROD_EQUIV_RISKED',  'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2',  'VOLUMENES_PROD_CRUDO_RISKED_ANTES_R',
                            'VOLUMENES_PROD_GAS_RISKED_ANTES_R', 'VOLUMENES_BLANCOS_RISKED_ANTES_R', 'TOTAL_VOLUMENES_PROD_ANTES_R', 'PCRUDO_REGALIAS',
                            'PGAS_REGALIAS', 'PGAS_REGALIAS_FACTOR', 'PBLANCOS_REGALIAS',  'TOTAL_REGALIAS', 'CRUDO_DESPUES_R', 'GAS_DESPUES_R', 'GAS_DESPUES_R_FACTOR',
                            'BLANCOS_DESPUES_R', 'TOTAL_PROD_DESPUES_R', 'VOLUMEN_CRUDO_DESPUES_R', 'VOLUMEN_GAS_DESPUES_R', 'VOLUMEN_GAS_FACTOR', 'VOLUMEN_BLANCOS_DESPUES_R',
                            'TOTAL_VOLUMENES_PROD_DESPUES_R', 'INGRESOS_CRUDO',  'INGRESOS_GAS', 'INGRESOS_BLANCOS',  'TOTAL_INGRESOS',  'COSTO_INTERNO_CO2',
                            'COSTO_TRANSPORTE','TOTAL_COSTOS', 'EBITDA',  'SUM_DEPRE', 'BASE_GRABABLE', 'IMPUESTOS',  'CAPEX',  'FLUJO_CAJA', 'EBIT', 'PAYOUT', 'DIVIDENDO', 'CAPEX_EXPLORATORIO',
                            'AGUA_NEUTRALIDAD', 'CRUDO_R_NACION',  'GAS_R_NACION', 'BLANCOS_R_NACION', 'TOTAL_R_NACION', 'CAPITAL_EMPLEADO', 'CAPEX_ACUM',
                            'MAS_REGALIAS', 'COSTO_PROD_REGALIAS_CRUDO', 'ANIO_INICIAL', 'ANIO_FINAL', 'DURACION','EMPLEOS_NP','ESTUDIANTES', 'ACCESO_AGUA_POTABLE','ACCESO_GAS','KM', 'OPEX_UNITARIO_TOTAL']
                            

        # Crear un nuevo DataFrame con las columnas deseadas
        df_flujo_caja_up = df_sabana_datos[columnas_deseadas].copy()
            
        # ------------------------------------------------------
        # ------------------------------------------------------Bajas_emisiones
        
        #PLP - bajas emisiones
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_baja_emisiones= self.flujo_caja_config["fuente"]["plp"]["bajas_emisiones_config"]["bajas_emisiones"]
        df_baja_emisiones = pd.read_excel(file_path_up,sheet_name=plp_baja_emisiones, header= 2, engine='openpyxl')


        if not df_baja_emisiones.empty:
            df_sabana_emisiones = pd.DataFrame()
            df_sabana_emisiones.loc[:, "MATRICULA_DIGITAL"] = df_baja_emisiones.loc[:, "Matrícula Digital"]
            df_sabana_emisiones.loc[:, "LINEA_DE_NEGOCIO"] = df_baja_emisiones.loc[:, "Linea de Negocio"]
            df_sabana_emisiones.loc[:, "OPCION_ESTRATEGICA"] = df_baja_emisiones.loc[:, "Opción Estratégica"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip()
            df_sabana_emisiones.loc[:, "EMPRESA"] = df_baja_emisiones.loc[:, "Empresa"]
            df_sabana_emisiones.loc[:, "SEGMENTO"] = df_baja_emisiones.loc[:, "Segmento"].str.title()
            df_sabana_emisiones.loc[:, "SUBSEGMENTO"] = df_baja_emisiones.loc[:, "Subsegmento"].str.title()
            df_sabana_emisiones.loc[:, "UN_NEG_REGIONAL"] = df_baja_emisiones.loc[:, "Un. Neg./Regional"]
            df_sabana_emisiones.loc[:, "GERENCIA"] = df_baja_emisiones.loc[:, "Gerencia"]
            df_sabana_emisiones.loc[:, "ACTIVO"] = df_baja_emisiones.loc[:, "Activo"]
            df_sabana_emisiones.loc[:, "DEPARTAMENTO"] = df_baja_emisiones.loc[:, "Departamento"]
            df_sabana_emisiones.loc[:, "TIPO_DE_INVERSION"] = df_baja_emisiones.loc[:, "Tipo de inversión"]
            df_sabana_emisiones.loc[:, "NOMBRE_PROYECTO"] = df_baja_emisiones.loc[:, "Nombre Proyecto"]
            df_sabana_emisiones.loc[:, "WI"] = df_baja_emisiones.loc[:, "WI"].fillna(0)
            df_sabana_emisiones.loc[:, "FASE_EN_CURSO"] = df_baja_emisiones.loc[:, "Fase en curso"]
            df_sabana_emisiones.loc[:, "CAPEX_MENOR_ANIO"] = df_baja_emisiones.loc[:, "CapEx < 23"].fillna(0)
            df_sabana_emisiones.loc[:, "CAPEX_MAYOR_ANIO"] = df_baja_emisiones.loc[:, "CapEx > 40 "].fillna(0)                        
            df_sabana_emisiones.loc[:, "CAPEX_TOTAL"] = df_baja_emisiones.loc[:, "CapEx Total"].fillna(0)
            df_sabana_emisiones.loc[:, "CAPEX_TOTAL_PROYECTO"] = df_baja_emisiones.loc[:, "CapEx Total Proyecto"].fillna(0) 
            df_sabana_emisiones.loc[:, "CAPEX_TOTAL_PROYECTO_MENOR_ANIO"] = df_baja_emisiones.loc[:, "CapEx Total Proyecto < 23"].fillna(0) 
            df_sabana_emisiones.loc[:, "CAPEX_TOTAL_PROYECTO_MAYOR_ANIO"] = df_baja_emisiones.loc[:, "CapEx Total Proyecto > 40 "].fillna(0) 
            df_sabana_emisiones.loc[:, "VPN"] = df_baja_emisiones.loc[:, "VPN"].fillna(0)
            df_sabana_emisiones.loc[:, "VPN_COSTO_INTERNO_CARBONO_EVITADO_ABATIDO"] = df_baja_emisiones.loc[:, "VPN del costo interno del Carbono evitado o abatido"].fillna(0)
            df_sabana_emisiones.loc[:, "EVPN"] =df_baja_emisiones.loc[:, "(E) VPN"].fillna(0)
            df_sabana_emisiones.loc[:, "VPI"] =df_baja_emisiones.loc[:, "VPI"].fillna(0)
            df_sabana_emisiones.loc[:, "EFI"] =df_baja_emisiones.loc[:, "EFI"].fillna(0)  
            df_sabana_emisiones.loc[:, "TIR"] =df_baja_emisiones.loc[:, "TIR"].fillna(0) 
            df_sabana_emisiones.loc[:, "PAY_BACK"] =df_baja_emisiones.loc[:, "Pay Back"].fillna(0)    
            df_sabana_emisiones.loc[:, "B_E_GAS"] =df_baja_emisiones.loc[:, "B.E Gas"].fillna(0)
            df_sabana_emisiones.loc[:, "LCOH_H2"] =df_baja_emisiones.loc[:, "LCOH H2"].fillna(0)
            df_sabana_emisiones.loc[:, "LCOH_H2"] = df_baja_emisiones.loc[:, "LCOH H2"] * self.api_config[
                "val_sensi_precio_Hidro"]
            df_sabana_emisiones.loc[:, "LCOE_ENERGIA"] =df_baja_emisiones.loc[:, "LCOE Energía"]
            df_sabana_emisiones.loc[:, "LCO_CCUS"] =df_baja_emisiones.loc[:, "LCO CCUS"]
            df_sabana_emisiones.loc[:, "OPEX_UNITARIO_ENERGIA"] =df_baja_emisiones.loc[:, "OpEx Unitario Energía"]
            df_sabana_emisiones.loc[:, "CAPEX_UNITARIO_ENERGIA"] =df_baja_emisiones.loc[:, "CapEx Unitario Energía"]
            df_sabana_emisiones.loc[:, "OPEX_UNITARIO_H2"] =df_baja_emisiones.loc[:, "OpEx Unitario H2"]
            df_sabana_emisiones.loc[:, "CAPEX_UNITARIO_H2"] =df_baja_emisiones.loc[:, "CapEx Unitario H2"]
            df_sabana_emisiones.loc[:, "OPEX_UNITARIO_CCUS"] =df_baja_emisiones.loc[:, "OpEx Unitario CCUS"].fillna(0) 
            df_sabana_emisiones.loc[:, "CAPEX_UNITARIO_CCUS"] =df_baja_emisiones.loc[:, "CapEx Unitario CCUS"]
            df_sabana_emisiones.loc[:, "PX"] =df_baja_emisiones.loc[:, "Px"]
            df_sabana_emisiones.loc[:, "EBITDA_2040"] =df_baja_emisiones.loc[:, "Ebitda 2040"]
            df_sabana_emisiones.loc[:, "FACTOR_DE_PLANTA"] =df_baja_emisiones.loc[:, "Factor de Planta"]
            df_sabana_emisiones.loc[:, "CAPACIDAD_INSTALADA"] =df_baja_emisiones.loc[:, "Capacidad Instalada"]
            df_sabana_emisiones.loc[:, "VOLUMEN_HABILITADO"] =df_baja_emisiones.loc[:, "Volumen Habilitado"]
            df_sabana_emisiones.loc[:, "DELTA_TARIFA_DE_ENERGIA"] =df_baja_emisiones.loc[:, "Delta tarifa de energía"].fillna(0)
            df_sabana_emisiones.loc[:, "DELTA_TARIFA_DE_ENERGIA"] = (df_baja_emisiones.loc[:,
                                                                    "Delta tarifa de energía"] *
                                                                    self.api_config["val_delta_energia"])
            df_sabana_emisiones.loc[:, "DELTA_OPEX"] =df_baja_emisiones.loc[:, "Delta Opex"]
            df_sabana_emisiones.loc[:, "APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] =df_baja_emisiones.loc[:, "Aporte EBITDA Acum 2040 proyectos asociados a CT+i"]
            df_sabana_emisiones.loc[:, "CATEGORIA_TESG_PALANCAS"] =df_baja_emisiones.loc[:, "Categoria TESG (Palancas)"]
            df_sabana_emisiones.loc[:, "MW_INCORPORADOS_MENOR_ANIO"] =df_baja_emisiones.loc[:, "MW incorporados <2023"]
            df_sabana_emisiones.loc[:, "TIPO_DE_HIDROGENO_A_PRODUCIR"] =df_baja_emisiones.loc[:, "Tipo de Hidrógeno a producir"]
            df_sabana_emisiones.loc[:, "PRECIO_ENERGIA_H2"] =df_baja_emisiones.loc[:, "Precio Energia (H2)"].fillna(0)
            df_sabana_emisiones.loc[:, "PRECIO_NO_ENERGIA_H2"] =df_baja_emisiones.loc[:, "Precio No Energia (H2)"].fillna(0)	

            #-------------------------------

            df_sabana_emisiones["LINEA_DE_NEGOCIO"] = df_sabana_emisiones["LINEA_DE_NEGOCIO"].str.strip()

            df_sabana_emisiones.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_emisiones) + 1))

            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1


            # Aplicar la función a la columna 'columna_sin_asignar' y asignar el resultado a una nueva columna
            df_sabana_emisiones['MATRICULA_DIGITAL'] = df_sabana_emisiones.apply(asignar_valor_be, axis=1)

            # la sabana esta vacia pendiente activar y poner validadcion de error si esta vaxia 
            df_sabana_emisiones = pd.concat(df_sabana_emisiones.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

            
            #capex 
            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']
            # columnas_agregar_capex = df_baja_emisiones.columns[df_baja_emisiones.columns.str.match("CapEx \d+")]
            nombre_columna_capex = 'CAPEX'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)
            df_sabana_emisiones["CAPEX"] = df_sabana_emisiones["CAPEX"].fillna(0)


            #CapEx Total Proyecto -----------------------------------------------
            columnas_capex_total_proyecto = [
                'CapEx Total Proyecto 24', 'CapEx Total Proyecto 25', 'CapEx Total Proyecto 26', 'CapEx Total Proyecto 27', 'CapEx Total Proyecto 28', 'CapEx Total Proyecto 29', 'CapEx Total Proyecto 30', 'CapEx Total Proyecto 31', 'CapEx Total Proyecto 32', 'CapEx Total Proyecto 33', 'CapEx Total Proyecto 34', 'CapEx Total Proyecto 35', 'CapEx Total Proyecto 36','CapEx Total Proyecto 37', 'CapEx Total Proyecto 38','CapEx Total Proyecto 39', 'CapEx Total Proyecto 40'
            ]

            nueva_columna_capex_total_proyecto = 'CAPEX_TOTAL_PROYECTO'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_capex_total_proyecto, nueva_columna_capex_total_proyecto, annio_inicio, annio_fin)
            df_sabana_emisiones["CAPEX_TOTAL_PROYECTO"] = df_sabana_emisiones["CAPEX_TOTAL_PROYECTO"].fillna(0)


            #Prod. Crudo Risked ------------------------------------------
            columnas_prod_crudo_risked = [
                'Prod. Crudo 24 Risked', 'Prod. Crudo 25 Risked', 'Prod. Crudo 26 Risked',
                'Prod. Crudo 27 Risked', 'Prod. Crudo 28 Risked', 'Prod. Crudo 29 Risked', 'Prod. Crudo 30 Risked',
                'Prod. Crudo 31 Risked', 'Prod. Crudo 32 Risked', 'Prod. Crudo 33 Risked', 'Prod. Crudo 34 Risked',
                'Prod. Crudo 35 Risked', 'Prod. Crudo 36 Risked', 'Prod. Crudo 37 Risked', 'Prod. Crudo 38 Risked',
                'Prod. Crudo 39 Risked', 'Prod. Crudo 40 Risked'
            ]

            nueva_columna_producto_crudo_risked = 'PROD_CRUDO_RISKED'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones,  df_sabana_emisiones,columnas_prod_crudo_risked, nueva_columna_producto_crudo_risked, annio_inicio, annio_fin)
            df_sabana_emisiones["PROD_CRUDO_RISKED"] = df_sabana_emisiones["PROD_CRUDO_RISKED"].fillna(0)

            #Prod. Gas Risked ------------------------------------------
            columnas_prod_gas_risked = [
                'Prod. Gas 24 Risked', 'Prod. Gas 25 Risked', 'Prod. Gas 26 Risked',
                'Prod. Gas 27 Risked', 'Prod. Gas 28 Risked', 'Prod. Gas 29 Risked', 'Prod. Gas 30 Risked',
                'Prod. Gas 31 Risked', 'Prod. Gas 32 Risked', 'Prod. Gas 33 Risked', 'Prod. Gas 34 Risked',
                'Prod. Gas 35 Risked', 'Prod. Gas 36 Risked', 'Prod. Gas 37 Risked', 'Prod. Gas 38 Risked',
                'Prod. Gas 39 Risked', 'Prod. Gas 40 Risked'
            ]

            nueva_columna_producto_gas_risked = 'PROD_GAS_RISKED'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_prod_gas_risked, nueva_columna_producto_gas_risked, annio_inicio, annio_fin)
            df_sabana_emisiones["PROD_GAS_RISKED"] = df_sabana_emisiones["PROD_GAS_RISKED"].fillna(0)


            # BLANCOS ----------------------------------------------------

            columnas_blancos_risked = [
                'Blancos 24 Risked', 'Blancos 25 Risked', 'Blancos 26 Risked',
                'Blancos 27 Risked', 'Blancos 28 Risked', 'Blancos 29 Risked', 'Blancos 30 Risked',
                'Blancos 31 Risked', 'Blancos 32 Risked', 'Blancos 33 Risked', 'Blancos 34 Risked',
                'Blancos 35 Risked', 'Blancos 36 Risked', 'Blancos 37 Risked', 'Blancos 38 Risked',
                'Blancos 39 Risked', 'Blancos 40 Risked'
            ]

            nueva_columna_blancos_risked = 'BLANCOS_RISKED'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_blancos_risked, nueva_columna_blancos_risked, annio_inicio, annio_fin)
            df_sabana_emisiones["BLANCOS_RISKED"] = df_sabana_emisiones["BLANCOS_RISKED"].fillna(0)


            # PROD EQUIVALENTE ------------------------------------------------------
            columnas_rod_Equiv_risked= [
                'Prod. Equiv. 24 Risked', 'Prod. Equiv. 25 Risked', 'Prod. Equiv. 26 Risked',
                'Prod. Equiv. 27 Risked', 'Prod. Equiv. 28 Risked', 'Prod. Equiv. 29 Risked', 'Prod. Equiv. 30 Risked',
                'Prod. Equiv. 31 Risked', 'Prod. Equiv. 32 Risked', 'Prod. Equiv. 33 Risked', 'Prod. Equiv. 34 Risked',
                'Prod. Equiv. 35 Risked', 'Prod. Equiv. 36 Risked', 'Prod. Equiv. 37 Risked', 'Prod. Equiv. 38 Risked',
                'Prod. Equiv. 39 Risked', 'Prod. Equiv. 40 Risked'
            ]

            nueva_columna_rod_Equiv_risked = 'PROD_EQUIV_RISKED'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_rod_Equiv_risked, nueva_columna_rod_Equiv_risked, annio_inicio, annio_fin)
            df_sabana_emisiones["PROD_EQUIV_RISKED"] = df_sabana_emisiones["PROD_EQUIV_RISKED"].fillna(0)


            #-----------------------------EMISONES NETAS
            #Emisiones Netas Co2_Alcance 1 y 2
            columnas_emisiones_netas_co2_alcance_1_y_2 = [
                'Emisiones Netas Co2 Alcance 1 y 2 2024',
                'Emisiones Netas Co2 Alcance 1 y 2 2025', 'Emisiones Netas Co2 Alcance 1 y 2 2026',
                'Emisiones Netas Co2 Alcance 1 y 2 2027', 'Emisiones Netas Co2 Alcance 1 y 2 2028',
                'Emisiones Netas Co2 Alcance 1 y 2 2029', 'Emisiones Netas Co2 Alcance 1 y 2 2030',
                'Emisiones Netas Co2 Alcance 1 y 2 2031', 'Emisiones Netas Co2 Alcance 1 y 2 2032',
                'Emisiones Netas Co2 Alcance 1 y 2 2033', 'Emisiones Netas Co2 Alcance 1 y 2 2034',
                'Emisiones Netas Co2 Alcance 1 y 2 2035', 'Emisiones Netas Co2 Alcance 1 y 2 2036',
                'Emisiones Netas Co2 Alcance 1 y 2 2037', 'Emisiones Netas Co2 Alcance 1 y 2 2038',
                'Emisiones Netas Co2 Alcance 1 y 2 2039', 'Emisiones Netas Co2 Alcance 1 y 2 2040'
            ]

            nueva_columna_emisiones_netas_co2_alcance_1_y_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_emisiones_netas_co2_alcance_1_y_2, nueva_columna_emisiones_netas_co2_alcance_1_y_2, annio_inicio, annio_fin)
            df_sabana_emisiones["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_sabana_emisiones["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)


            #Emisiones Netas Co2_Alcance 3
            columnas_emisiones_netas_co2_alcance_3 = [
                'Emisiones Netas Co2 Alcance 3 2024',
                'Emisiones Netas Co2 Alcance 3 2025', 'Emisiones Netas Co2 Alcance 3 2026',
                'Emisiones Netas Co2 Alcance 3 2027', 'Emisiones Netas Co2 Alcance 3 2028',
                'Emisiones Netas Co2 Alcance 3 2029', 'Emisiones Netas Co2 Alcance 3 2030',
                'Emisiones Netas Co2 Alcance 3 2031', 'Emisiones Netas Co2 Alcance 3 2032',
                'Emisiones Netas Co2 Alcance 3 2033', 'Emisiones Netas Co2 Alcance 3 2034',
                'Emisiones Netas Co2 Alcance 3 2035', 'Emisiones Netas Co2 Alcance 3 2036',
                'Emisiones Netas Co2 Alcance 3 2037', 'Emisiones Netas Co2 Alcance 3 2038',
                'Emisiones Netas Co2 Alcance 3 2039', 'Emisiones Netas Co2 Alcance 3 2040'
            ]

            nueva_columna_emisiones_netas_co2_alcance_3 = 'EMISIONES_NETAS_CO2_ALCANCE_3'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_emisiones_netas_co2_alcance_3, nueva_columna_emisiones_netas_co2_alcance_3, annio_inicio, annio_fin)
            df_sabana_emisiones["EMISIONES_NETAS_CO2_ALCANCE_3"] = df_sabana_emisiones["EMISIONES_NETAS_CO2_ALCANCE_3"].fillna(0)


            #-----------------------------mw_incorporados
            #mw_incorporados
            columnas_mw_incorporados = [
                'MW incorporados 2024',
                'MW incorporados 2025', 'MW incorporados 2026',
                'MW incorporados 2027', 'MW incorporados 2028',
                'MW incorporados 2029', 'MW incorporados 2030',
                'MW incorporados 2031', 'MW incorporados 2032',
                'MW incorporados 2033', 'MW incorporados 2034',
                'MW incorporados 2035', 'MW incorporados 2036',
                'MW incorporados 2037', 'MW incorporados 2038',
                'MW incorporados 2039', 'MW incorporados 2040'
            ]

            nueva_mw_incorporados = 'MW_INCORPORADOS'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones,columnas_mw_incorporados, nueva_mw_incorporados, annio_inicio, annio_fin)
            df_sabana_emisiones["MW_INCORPORADOS"] = df_sabana_emisiones["MW_INCORPORADOS"].fillna(0)


            #-----------------------------kwh
            #columnas_kwh
            columnas_kwh = [
                'KWh 2024',
                'KWh 2025', 'KWh 2026',
                'KWh 2027', 'KWh 2028',
                'KWh 2029', 'KWh 2030',
                'KWh 2031', 'KWh 2032',
                'KWh 2033', 'KWh 2034',
                'KWh 2035', 'KWh 2036',
                'KWh 2037', 'KWh 2038',
                'KWh 2039', 'KWh 2040'
            ]

            nueva_columnas_kwh = 'KWH'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_kwh, nueva_columnas_kwh, annio_inicio, annio_fin)
            df_sabana_emisiones["KWH"] = df_sabana_emisiones["KWH"].fillna(0)

            #-----------------------------Hidrogeno
            #columnas_hidrogeno
            columnas_Produccion_de_hidrogeno = [
                'Producción de Hidrogeno 2024',
                'Producción de Hidrogeno 2025', 'Producción de Hidrogeno 2026',
                'Producción de Hidrogeno 2027', 'Producción de Hidrogeno 2028',
                'Producción de Hidrogeno 2029', 'Producción de Hidrogeno 2030',
                'Producción de Hidrogeno 2031', 'Producción de Hidrogeno 2032',
                'Producción de Hidrogeno 2033', 'Producción de Hidrogeno 2034',
                'Producción de Hidrogeno 2035', 'Producción de Hidrogeno 2036',
                'Producción de Hidrogeno 2037', 'Producción de Hidrogeno 2038',
                'Producción de Hidrogeno 2039', 'Producción de Hidrogeno 2040'
            ]

            nueva_columna_hidrogeno= 'PRODUCCION_DE_HIDROGENO'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_Produccion_de_hidrogeno, nueva_columna_hidrogeno, annio_inicio, annio_fin)
            df_sabana_emisiones["PRODUCCION_DE_HIDROGENO"] = df_sabana_emisiones["PRODUCCION_DE_HIDROGENO"].fillna(0)

            #-----------------------------AGUA NEUTRALIDAD

            columnas_agua_neutralidad = ['Agua Neutralidad 2024', 	 
                                        'Agua Neutralidad 2025', 'Agua Neutralidad 2026',	
                                        'Agua Neutralidad 2027', 'Agua Neutralidad 2028', 	
                                        'Agua Neutralidad 2029', 'Agua Neutralidad 2030',
                                        'Agua Neutralidad 2031', 'Agua Neutralidad 2032',
                                        'Agua Neutralidad 2033', 'Agua Neutralidad 2034',
                                        'Agua Neutralidad 2035', 'Agua Neutralidad 2036',	 
                                        'Agua Neutralidad 2037', 'Agua Neutralidad 2038',	 
                                        'Agua Neutralidad 2039', 'Agua Neutralidad 2040' ]

            nueva_columna_agua_neutralidad = 'AGUA_NEUTRALIDAD'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_agua_neutralidad, nueva_columna_agua_neutralidad, annio_inicio, annio_fin)
            df_sabana_emisiones["AGUA_NEUTRALIDAD"] = df_sabana_emisiones["AGUA_NEUTRALIDAD"].fillna(0)

            #-----------------------------Empleon NP 

            columnas_empleo_np = [
                        'Empleos NP 2024',
                        'Empleos NP 2025', 'Empleos NP 2026',
                        'Empleos NP 2027', 'Empleos NP 2028',
                        'Empleos NP 2029', 'Empleos NP 2030',
                        'Empleos NP 2031', 'Empleos NP 2032',
                        'Empleos NP 2033', 'Empleos NP 2034',
                        'Empleos NP 2035', 'Empleos NP 2036',
                        'Empleos NP 2037', 'Empleos NP 2038',
                        'Empleos NP 2039', 'Empleos NP 2040'
            ]

            nueva_columna_empleon = 'EMPLEOS_NP'

            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_empleo_np, nueva_columna_empleon, annio_inicio, annio_fin)
            df_sabana_emisiones["EMPLEOS_NP"] = df_sabana_emisiones["EMPLEOS_NP"].fillna(0)

            #----------------------------------------------Estudiantes

            columnas= ['Estudiantes 2024', 'Estudiantes 2025',
                                    'Estudiantes 2026', 'Estudiantes 2027', 'Estudiantes 2028',
                                    'Estudiantes 2029', 'Estudiantes 2030', 'Estudiantes 2031', 
                                    'Estudiantes 2032', 'Estudiantes 2033', 'Estudiantes 2034',	
                                    'Estudiantes 2035', 'Estudiantes 2036', 'Estudiantes 2037',
                                    'Estudiantes 2038',	'Estudiantes 2039',	'Estudiantes 2040' ]

            nueva_columna= 'ESTUDIANTES'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas, nueva_columna, annio_inicio, annio_fin)
            df_sabana_emisiones["ESTUDIANTES"] = df_sabana_emisiones["ESTUDIANTES"].fillna(0)

            #----------------------------------ACCESO AGUA POTABLE
            columnas_acceso_potable= ['Acceso agua potable 2024', 'Acceso agua potable 2025',	 'Acceso agua potable 2026','Acceso agua potable 2027',	'Acceso agua potable 2028',	'Acceso agua potable 2029',	'Acceso agua potable 2030',	'Acceso agua potable 2031',	'Acceso agua potable 2032',	'Acceso agua potable 2033',	'Acceso agua potable 2034',	'Acceso agua potable 2035',	'Acceso agua potable 2036',	'Acceso agua potable 2037',	'Acceso agua potable 2038',	'Acceso agua potable 2039','Acceso agua potable 2040']

            nueva_columna_acceso_agua_potable = 'ACCESO_AGUA_POTABLE'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_acceso_potable, nueva_columna_acceso_agua_potable, annio_inicio, annio_fin)
            df_sabana_emisiones["ACCESO_AGUA_POTABLE"] = df_sabana_emisiones["ACCESO_AGUA_POTABLE"].fillna(0)


            #-----------------ACCESO A GAS
            columnas_acceso_gas= ['Acceso Gas 2024','Acceso Gas 2025', 'Acceso Gas 2026','Acceso Gas 2027','Acceso Gas 2028','Acceso Gas 2029','Acceso Gas 2030','Acceso Gas 2031','Acceso Gas 2032',	'Acceso Gas 2033','Acceso Gas 2034','Acceso Gas 2035','Acceso Gas 2036','Acceso Gas 2037','Acceso Gas 2038',	'Acceso Gas 2039','Acceso Gas 2040']

            nueva_columna_acceso_gas= 'ACCESO_GAS'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_emisiones["ACCESO_GAS"] = df_sabana_emisiones["ACCESO_GAS"].fillna(0)
                    
            #-----------------KM
            columnas_acceso_km= ['Km 2024', 'Km 2025', 'Km 2026', 'Km 2027', 'Km 2028', 'Km 2029', 'Km 2030', 'Km 2031', 'Km 2032', 'Km 2033', 'Km 2034', 'Km 2035', 'Km 2036', 'Km 2037', 'Km 2038', 'Km 2039	Km 2040']

            nueva_columna_acceso_gas= 'KM'
            df_sabana_emisiones = plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_emisiones["KM"] = df_sabana_emisiones["KM"].fillna(0)

            df_sabana_emisiones = df_sabana_emisiones.loc[:, ~df_sabana_emisiones.columns.duplicated()]

            # Adiciono las columnas precio_crudo,  
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]
            impuesto_renta =  self.flujo_caja_config["parametros"]["impuesto_renta"]
            payout =  self.flujo_caja_config["parametros"]["payout"] 
            precio_co2 =  self.flujo_caja_config["parametros"]["precio_co2"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios= list(range(annio_inicio, annio_fin))

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios , 
                    'PRECIO_CRUDO_BRENT': precio_crudo, 
                    'IMPUESTO_RENTA': impuesto_renta, 
                    'PAYOUT':payout,     
                    'PRECIO_CO2':precio_co2        
                    }

            df_anio_crudo= pd.DataFrame(data)

            df_sabana_emisiones = df_sabana_emisiones.merge(df_anio_crudo, on='RANGO_ANIO', how='left')
            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]

            df_energia = df_sabana_emisiones[df_sabana_emisiones['OPCION_ESTRATEGICA'] =='Renovables']
            df_ccus = df_sabana_emisiones[df_sabana_emisiones['OPCION_ESTRATEGICA'] =='CCUS']
            df_sabana_emisiones = df_sabana_emisiones[df_sabana_emisiones['OPCION_ESTRATEGICA'] =='Hidrogeno']

            if not df_sabana_emisiones.empty:  
                #Flujo caja por proyecto hidrogeno
                df_sabana_emisiones['PRODUCCION_DE_HIDROGENO_MKG'] = df_sabana_emisiones['PRODUCCION_DE_HIDROGENO'] /1000000
                #A--> Volumen H2 --> 
                df_sabana_emisiones['VOLUMENES_H2'] = df_sabana_emisiones['PRODUCCION_DE_HIDROGENO']

                #precio H2  df_sabana_emisiones['LCOH_H2']

                #C--> TOTAL INGRESOS--> A B
                df_sabana_emisiones['TOTAL_INGRESOS'] = (df_sabana_emisiones['VOLUMENES_H2'] * df_sabana_emisiones['LCOH_H2'])/1000000 

                #D--> Costos de energia  --> Tarifa * A --> parametros 
                df_sabana_emisiones['COSTO_DE_ENERGIA'] = ( df_sabana_emisiones['VOLUMENES_H2'] * df_sabana_emisiones['PRECIO_ENERGIA_H2']) / 1000000

                #E--> costos de no energia --> TARIFA * A --> parametros
                df_sabana_emisiones['COSTO_DE_NO_ENERGIA'] = (df_sabana_emisiones['VOLUMENES_H2'] * df_sabana_emisiones['PRECIO_NO_ENERGIA_H2']) / 1000000

                #F--> G&A --> % *  C--paraametro de porcentaje de ingresos --> porcentaje regalias

                df_sabana_emisiones['COSTO_INTERNO_CO2'] = df_sabana_emisiones['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_emisiones['PRECIO_CO2']

                #Hallar la sobretasa 
                #Consultar parametros 
                rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
                historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
                percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
                percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
                percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
                intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]

                annio_inicio_s = rango_anio_sobretasa[0]
                annio_fin_s = rango_anio_sobretasa[1] + 1
                anios = list(range(annio_inicio_s, annio_fin_s))

                df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

                # Replicar registros de df1 para que coincida con la longitud de df2
                replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_sabana_emisiones) // len(df_sobretasa)), ignore_index=True)

                # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
                replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_sabana_emisiones)]

                df_sabana_emisiones['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']


                #G--> TOTAL COSTOS --> D+E+F
                df_sabana_emisiones['TOTAL_COSTOS'] = df_sabana_emisiones['COSTO_DE_ENERGIA']  + df_sabana_emisiones['COSTO_DE_NO_ENERGIA'] + df_sabana_emisiones['COSTO_INTERNO_CO2']

                #H--> EBITDA -->C-G 
                df_sabana_emisiones['EBITDA'] = df_sabana_emisiones['TOTAL_INGRESOS']  - df_sabana_emisiones['TOTAL_COSTOS'] 

                #DEPRECIACION Hidrogeno
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_emisiones['ID_PROYECTO'].unique()})

                porcentaje_depreciacion_anio = self.flujo_caja_config["parametros"]["porcentaje_depreciacion_anio"] 
                porcentaje_deduccion_especial= self.flujo_caja_config["parametros"]["porcentaje_deduccion_especial"]

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']   
                    
                    columnas_deseadas = ['MATRICULA_DIGITAL','ID_PROYECTO','RANGO_ANIO','PRODUCCION_DE_HIDROGENO', 'CAPEX']
                    columna_filtro_valor = ID_PROYECTO

                    df_data = df_sabana_emisiones[df_sabana_emisiones['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                    primer_anio_produccion = df_data.loc[df_data['PRODUCCION_DE_HIDROGENO'] > 0, 'RANGO_ANIO'].min()
                    df_data_seleccionadas =  df_data[df_data['RANGO_ANIO'] == primer_anio_produccion]
                    df_data_seleccionadas['ANIO_PRODUCCION']= primer_anio_produccion   
                    df_data_seleccionadas['SUMA_CAPEX'] = df_data['CAPEX'].sum()


                    if df_data_seleccionadas.empty:  
                        data = {'ID_PROYECTO': ID_PROYECTO,
                                'RANGO_ANIO': list(range(annio_inicio, annio_fin)),
                                'SUM_DEPRE': 0,
                                'CAPEX_TOTAL_ANIO': 0}
                        df = pd.DataFrame(data)    
                    else:
                        df= calcular_depreciacion_hidrogeno(df_data_seleccionadas, porcentaje_depreciacion_anio, annio_inicio, annio_fin )
                    
                    merged_df = pd.merge(df_data, df,on=['ID_PROYECTO', 'RANGO_ANIO'], how='inner')
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
                    
                    merged_df['SUM_DEPRE'] = merged_df['SUM_DEPRE'].astype(float)

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df)  

                    df_total = df_total.append(df_con_ppe, ignore_index=True)   
                    
                df_sabana_emisiones = pd.merge(df_sabana_emisiones, df_total, how='inner')
                
                #J Base grabable --> H-I 
                df_sabana_emisiones['BASE_GRABABLE'] = df_sabana_emisiones['EBITDA'] +  df_sabana_emisiones['COSTO_INTERNO_CO2']  - df_sabana_emisiones['SUM_DEPRE']

                #K IMPUESTOS J* 35% * SOBRETASA 
                df_sabana_emisiones['IMPUESTOS'] = df_sabana_emisiones['BASE_GRABABLE'] * (df_sabana_emisiones['IMPUESTO_RENTA']  + df_sabana_emisiones['SOBRETASA'] )


                porcentaje_deduccion_especial = self.flujo_caja_config["parametros"]["porcentaje_deduccion_especial"] 
                #CAPEX PRIMER AÑO * 50 % (35%+ SOBRETASA) porcentaje_deduccion_especial 
                df_sabana_emisiones['DEDUCCION_ESPECIAL'] =  df_sabana_emisiones['CAPEX_TOTAL_ANIO']  *  porcentaje_deduccion_especial * (df_sabana_emisiones['IMPUESTO_RENTA']  + df_sabana_emisiones['SOBRETASA'])

                # FLUJO DE CAJA --> H–K+L-M
                df_sabana_emisiones['FLUJO_CAJA'] = df_sabana_emisiones['EBITDA'] - df_sabana_emisiones['IMPUESTOS']  + df_sabana_emisiones['DEDUCCION_ESPECIAL'] - df_sabana_emisiones['CAPEX']

                #utilidad
                df_sabana_emisiones['EBIT'] =  df_sabana_emisiones['EBITDA']  -  df_sabana_emisiones['IMPUESTOS']
                #dividendo
                df_sabana_emisiones['DIVIDENDO'] =  df_sabana_emisiones['EBIT']  * df_sabana_emisiones['PAYOUT']

                df_sabana_emisiones["CAPITAL_EMPLEADO"] = df_sabana_emisiones["CAPITAL_EMPLEADO"].fillna(0) 
                df_sabana_emisiones['EBIT'] = df_sabana_emisiones["EBIT"].fillna(0) 
                # df_sabana_emisiones['ROACE'] = (df_sabana_emisiones['EBIT'] / df_sabana_emisiones['CAPITAL_EMPLEADO'])
                # df_sabana_emisiones['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_sabana_emisiones['ROACE'] = df_sabana_emisiones["ROACE"].fillna(0)   
            else:
                logger.info(f"El DataFrame hidrogeno (df_sabana_emisiones) está vacío.")

            if not df_ccus.empty:
                # 	CCUS Almacenamiento	

                # A.	VOLUMEN_CAPTURADO_CO2 --> Co2
                df_ccus['VOLUMEN_CAPTURADO_CO2'] = df_ccus['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2']	

                df_ccus['TOTAL_INGRESOS'] = 0 

                # B.	COSTO_VARIABLE	--> OpEx Unitario CCUS * A.
                df_ccus['COSTO_VARIABLE'] = (df_ccus['LCO_CCUS'] * df_ccus['VOLUMEN_CAPTURADO_CO2']) * - 1

                df_ccus['COSTO_INTERNO_CO2'] = df_ccus['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_ccus['PRECIO_CO2']

                # C.	TOTAL_COSTOS	-->  B.
                df_ccus['TOTAL_COSTOS'] = df_ccus['COSTO_VARIABLE'] + df_ccus['COSTO_INTERNO_CO2']

                # D.	EBITDA	
                df_ccus['EBITDA'] = df_ccus['TOTAL_INGRESOS'] - df_ccus['TOTAL_COSTOS']
                #DEPRECIACION_CCUS
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_ccus['ID_PROYECTO'].unique()})

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']  
                        
                    columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','EMISIONES_NETAS_CO2_ALCANCE_1_Y_2', 'CAPEX']
                    columna_filtro_valor = ID_PROYECTO
                    df_data = df_ccus[df_ccus['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas] 


                    emisiones_distintas_de_cero = df_data[df_data['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] != 0]
                    primer_anio_emisiones = emisiones_distintas_de_cero['RANGO_ANIO'].iloc[0]  
                    df_data_seleccionadas =  df_data[df_data['RANGO_ANIO'] == primer_anio_emisiones]
                    df_data_seleccionadas['ANIO_EMISIONES']= primer_anio_emisiones  
                    df_data_seleccionadas['SUMA_CAPEX'] = df_data['CAPEX'].sum()

                    
                    df = calcular_depreciacion_ccus(df_data_seleccionadas, porcentaje_depreciacion_anio, annio_inicio, annio_fin)
                    merged_df = pd.merge(df_data, df,on=['ID_PROYECTO', 'RANGO_ANIO'], how='inner')
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df) 

                    df_total = df_total.append(df_con_ppe, ignore_index=True)   

                df_ccus = pd.merge(df_ccus, df_total, how='inner')

                #Calcular_sobretasa_CCUS 
                df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

                # Replicar registros de df1 para que coincida con la longitud de df2
                replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_ccus) // len(df_sobretasa)), ignore_index=True)

                # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
                replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_ccus)]

                porcentaje_deduccion_especial_ccus = self.flujo_caja_config["parametros"]["porcentaje_deduccion_especial_ccus"] 

                df_ccus['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']


                # F. BASE_GRABABLE -->	D-E
                df_ccus['BASE_GRABABLE'] = df_ccus['EBITDA'] + df_ccus['COSTO_INTERNO_CO2'] - df_ccus['SUM_DEPRE']

                # G	IMPUESTOS	F * 35% + Sobre tasa --
                df_ccus['IMPUESTOS'] =  df_ccus['BASE_GRABABLE'] * (df_ccus['IMPUESTO_RENTA']  + df_ccus['SOBRETASA']) 
                

                df_ccus['DEDUCCION_ESPECIAL'] =  df_ccus['CAPEX_TOTAL_ANIO']   *  porcentaje_deduccion_especial_ccus * (df_ccus['IMPUESTO_RENTA']  + df_ccus['SOBRETASA'])

                df_ccus['FLUJO_CAJA'] = df_ccus['EBITDA'] - df_ccus['IMPUESTOS']  + df_ccus['DEDUCCION_ESPECIAL'] - df_ccus['CAPEX']

                # utilidad
                df_ccus['EBIT'] =  df_ccus['EBITDA']  -  df_ccus['IMPUESTOS']

                #dividendo
                df_ccus['DIVIDENDO'] =  df_ccus['EBIT']  * df_ccus['PAYOUT']

                #CAPITAL EMPLEADO
                df_ccus["CAPITAL_EMPLEADO"] = df_ccus["CAPITAL_EMPLEADO"].fillna(0) 

                #UTILIDAD
                df_ccus['EBIT'] = df_ccus["EBIT"].fillna(0) 

                # df_ccus['ROACE'] = (df_ccus['EBIT'] / df_ccus['CAPITAL_EMPLEADO']) 
                # df_ccus['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_ccus["ROACE"] = df_ccus["ROACE"].fillna(0) 
            else:
                logger.info(f"El DataFrame(df_ccus) está vacío.")

            if not df_energia.empty:
                # flujo de caja Energia 
                # A	 (Energía) MW	 (Energía) MW*1000  
                # df_energia['KWH'] = df_energia['KWH'] 

                # B	Delta tarifa de energía	/4100 /1000 --> VERIFICAR PARAMETRO  se cambia a 1000000 por solicitud cliente
                df_energia['DELTA_TARIFA_DE_ENERGIA'] = df_energia['DELTA_TARIFA_DE_ENERGIA']/ 4100 / 1000000

                # C	Ingresos	A*B
                df_energia ['TOTAL_INGRESOS'] = df_energia['KWH'] * df_energia['DELTA_TARIFA_DE_ENERGIA']

                # D	Costo Variable	(0)
                df_energia['COSTO_VARIABLE'] = 0

                # E	Costo Interno CO2	Emisiones*Precio CO2
                df_energia['COSTO_INTERNO_CO2'] = df_energia['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_energia['PRECIO_CO2']

                # F	Total Costos	D+E
                df_energia['TOTAL_COSTOS'] = df_energia['COSTO_VARIABLE'] + df_energia['COSTO_INTERNO_CO2'] 

                # G	EBITDA	C-F 
                df_energia ['EBITDA'] = df_energia ['TOTAL_INGRESOS'] - df_energia ['TOTAL_COSTOS']


                #DEPRECIACION_ENERGIA
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_energia['ID_PROYECTO'].unique()})

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']  
                            
                    columnas_deseadas = ['MATRICULA_DIGITAL','ID_PROYECTO','RANGO_ANIO','KWH', 'CAPEX']
                    columna_filtro_valor = ID_PROYECTO
                    df_data = df_energia[df_energia['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]
                    
                    df_energia_cero = df_data[df_data['KWH'] != 0]
                    primer_anio_energia= df_energia_cero['RANGO_ANIO'].iloc[0]  
                    df_data_sele = df_data[df_data['RANGO_ANIO'] == primer_anio_energia]  
                    df_data_sele['ANIO_ENERGIA']= primer_anio_energia        
                    df_data_sele['SUMA_CAPEX'] = df_data['CAPEX'].sum()
                    
                    df = calcular_depreciacion_energia(df_data_sele, porcentaje_depreciacion_anio, annio_inicio, annio_fin)
                    merged_df = pd.merge(df_data, df,on=['ID_PROYECTO', 'RANGO_ANIO'], how='inner')
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
                    
                    merged_df['SUM_DEPRE'] = merged_df['SUM_DEPRE'].astype(float)

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df) 


                    df_total = df_total.append(df_con_ppe, ignore_index=True)   

                df_energia = pd.merge(df_energia, df_total, how='inner')

                # Calcular_sobretasa_energia

                df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

                # Replicar registros de df1 para que coincida con la longitud de df2
                replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_energia) // len(df_sobretasa)), ignore_index=True)

                # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
                replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_energia)]

                df_energia['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']
                # I	Base Grabable	G-H
                # df_energia['SUM_DEPRE'] = 0
                
                df_energia['BASE_GRABABLE'] = df_energia['EBITDA'] +df_energia['COSTO_INTERNO_CO2']  - df_energia['SUM_DEPRE']

                # J	Impuestos	I*(35%+Sobre tasa)
                df_energia['IMPUESTOS'] =  df_energia['BASE_GRABABLE'] * (df_energia['IMPUESTO_RENTA'] + df_energia['SOBRETASA']) 

                # K	Deducción especial	Capex Total*50%*(35%+ Sobre tasa)
                df_energia['DEDUCCION_ESPECIAL'] =  df_energia['CAPEX_TOTAL_ANIO']   *  porcentaje_deduccion_especial * (df_energia['IMPUESTO_RENTA']  + df_energia['SOBRETASA'])

                # # L	Capex	Columna AA+
                # df_energia['CAPEX']

                # 	FC	G-J+K-L
                df_energia['FLUJO_CAJA'] = df_energia['EBITDA'] - df_energia['IMPUESTOS']  + df_energia['DEDUCCION_ESPECIAL'] - df_energia['CAPEX']

                # utilidad
                df_energia['EBIT'] =  df_energia['EBITDA']  -  df_energia['IMPUESTOS']

                #dividendo
                df_energia['DIVIDENDO'] =  df_energia['EBIT']  * df_energia['PAYOUT']

                #CAPITAL EMPLEADO
                df_energia["CAPITAL_EMPLEADO"] = df_energia["CAPITAL_EMPLEADO"].fillna(0) 

                #UTILIDAD
                df_energia['EBIT'] = df_energia["EBIT"].fillna(0) 
                # df_energia['ROACE'] = (df_energia['EBIT'] / df_energia['CAPITAL_EMPLEADO']) 
                # df_energia['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_energia["ROACE"] = df_energia["ROACE"].fillna(0) 

            else:
                logger.info(f"El DataFrame(df_energia) está vacío.")
            
            df_sabana_emisiones = pd.concat([df_sabana_emisiones, df_ccus, df_energia], ignore_index=True)  
            df_sabana_emisiones = df_sabana_emisiones.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)
        else:
            logger.info(f"El DataFrame Bajas emisiones (df_baja_emisiones) está vacío.") 

        
        
        #-------------------------------------------------------
        #----------------------------------------Trasnmision_vias
       
        #PLP - Transmisión y Vías
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_trans_vias = self.flujo_caja_config["fuente"]["plp"]["trans_vias_config"]["trans_vias"]
        df_trans_vias = pd.read_excel(file_path_up, sheet_name=plp_trans_vias, header=2, engine='openpyxl')

        #----------------------------------------------------
        if not df_trans_vias.empty: 
            # Crear un DataFrame vacío para almacenar los datos de transmisión
            df_sabana_transmision = pd.DataFrame()

            # Copiar datos de otras columnas al DataFrame
            df_sabana_transmision["MATRICULA_DIGITAL"] = df_trans_vias["Matrícula Digital"]
            df_sabana_transmision["LINEA_DE_NEGOCIO"] = df_trans_vias["Linea de Negocio"]
            df_sabana_transmision["OPCION_ESTRATEGICA"] = df_trans_vias["Opción Estratégica"]
            df_sabana_transmision["EMPRESA"] = df_trans_vias["Empresa"]
            df_sabana_transmision["SEGMENTO"] = df_trans_vias["Segmento"].str.title()
            df_sabana_transmision["SUBSEGMENTO"] = df_trans_vias["Subsegmento"].str.title()
            df_sabana_transmision["UN_NEG_REGIONAL"] = df_trans_vias["Un. Neg./Regional"]
            df_sabana_transmision["GERENCIA"] = df_trans_vias["Gerencia"]
            df_sabana_transmision["ACTIVO"] = df_trans_vias["Activo"]
            df_sabana_transmision["DEPARTAMENTO"] = df_trans_vias["Departamento"]
            df_sabana_transmision["TIPO_DE_INVERSION"] = df_trans_vias["Tipo de inversión"]
            df_sabana_transmision["NOMBRE_PROYECTO"] = df_trans_vias["Nombre Proyecto"]
            df_sabana_transmision["WI"] = df_trans_vias["WI (Equity Ecopetrol)"].fillna(0)
            df_sabana_transmision["FASE_EN_CURSO"] = df_trans_vias["Fase en curso"]
            df_sabana_transmision["VPN"] = df_trans_vias["VPN"].fillna(0)
            df_sabana_transmision["VPI"] = df_trans_vias["VPI"].fillna(0)
            df_sabana_transmision["EFI"] = df_trans_vias["EFI"].fillna(0)
            df_sabana_transmision["TIR"] = df_trans_vias["TIR"].fillna(0)
            df_sabana_transmision["PAY_BACK"] = df_trans_vias["Pay Back"].fillna(0)
            df_sabana_transmision["CAPEX_MENOR_ANIO"] = df_trans_vias["CapEx < 23"].fillna(0)
            df_sabana_transmision["CAPEX_MAYOR_ANIO"] = df_trans_vias["CapEx > 40 "].fillna(0)
            df_sabana_transmision["CAPEX_TOTAL"] = df_trans_vias["CapEx Total"].fillna(0)


            # ---------------------------------------------
            # Insertar una columna "ID_PROYECTO" al principio del DataFrame
            df_sabana_transmision.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_transmision) + 1))

            # Obtener el rango de años de la configuración
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1

            # Función para expandir cada fila del DataFrame con un rango de años


            # Aplicar la función     a cada fila del DataFrame y concatenar los resultados
            df_sabana_transmision = pd.concat(df_sabana_transmision.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

            # Obtener las columnas de CapEx en df_trans_vias
            # columnas_agregar_capex = df_trans_vias.columns[df_trans_vias.columns.str.match("CapEx \d+")]

            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']

            # Nombre de la nueva columna de CapEx en df_sabana_transmision
            nombre_columna_capex = 'CAPEX'

            # Agregar una nueva columna de CapEx a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'CAPEX' con 0
            df_sabana_transmision["CAPEX"] = df_sabana_transmision["CAPEX"].fillna(0)


            # INGRESO_TOTAL--------------------------------------------------------------------------------------
            # Columnas de ingresos totales en df_trans_vias
            columnas_ingresos_total = [
                'Ingresos Total 24', 'Ingresos Total 25', 'Ingresos Total 26',
                'Ingresos Total 27', 'Ingresos Total 28', 'Ingresos Total 29', 'Ingresos Total 30',
                'Ingresos Total 31', 'Ingresos Total 32', 'Ingresos Total 33', 'Ingresos Total 34',
                'Ingresos Total 35', 'Ingresos Total 36', 'Ingresos Total 37', 'Ingresos Total 38',
                'Ingresos Total 39', 'Ingresos Total 40'
            ]

            # Nombre de la nueva columna de ingreso total en df_sabana_transmision
            nueva_columna_ingresos_total = 'TOTAL_INGRESOS'

            # Agregar una nueva columna de ingreso total a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_ingresos_total, nueva_columna_ingresos_total, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'INGRESO_TOTAL' con 0
            df_sabana_transmision["TOTAL_INGRESOS"] = df_sabana_transmision["TOTAL_INGRESOS"].fillna(0)


            # COSTO_TOTAL--------------------------------------------------------------------------------------
            # Columnas de costos totales en df_trans_vias
            columnas_costos_total = [
                'Costos Total 24', 'Costos Total 25', 'Costos Total 26',
                'Costos Total 27', 'Costos Total 28', 'Costos Total 29', 'Costos Total 30',
                'Costos Total 31', 'Costos Total 32', 'Costos Total 33', 'Costos Total 34',
                'Costos Total 35', 'Costos Total 36', 'Costos Total 37', 'Costos Total 38',
                'Costos Total 39', 'Costos Total 40'
            ]

            # Nombre de la nueva columna de costo total en df_sabana_transmision
            nueva_columna_costos_total = 'TOTAL_COSTOS'

            # Agregar una nueva columna de costo total a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_costos_total, nueva_columna_costos_total, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'COSTO_TOTAL' con 0
            df_sabana_transmision["TOTAL_COSTOS"] = df_sabana_transmision["TOTAL_COSTOS"].fillna(0)


            # EBITA_TOTAL--------------------------------------------------------------------------------------
            # Columnas de EBITDA totales en df_trans_vias
            columnas_ebitda_total = [
                'EBITDA Total 24', 'EBITDA Total 25', 'EBITDA Total 26',
                'EBITDA Total 27', 'EBITDA Total 28', 'EBITDA Total 29', 'EBITDA Total 30',
                'EBITDA Total 31', 'EBITDA Total 32', 'EBITDA Total 33', 'EBITDA Total 34',
                'EBITDA Total 35', 'EBITDA Total 36', 'EBITDA Total 37', 'EBITDA Total 38',
                'EBITDA Total 39', 'EBITDA Total 40'
            ]

            # Nombre de la nueva columna de EBITA total en df_sabana_transmision
            nueva_columna_ebitda_total = 'EBITDA'

            # Agregar una nueva columna de EBITA total a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_ebitda_total, nueva_columna_ebitda_total, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'EBITA_TOTAL' con 0
            df_sabana_transmision["EBITDA"] = df_sabana_transmision["EBITDA"].fillna(0)


            # FLUJO_CAJA--------------------------------------------------------------------------------------
            # Columnas de flujo de caja en df_trans_vias
            columnas_flujo_caja = [
                'Flujo Caja 24', 'Flujo Caja 25', 'Flujo Caja 26',
                'Flujo Caja 27', 'Flujo Caja 28', 'Flujo Caja 29', 'Flujo Caja 30',
                'Flujo Caja 31', 'Flujo Caja 32', 'Flujo Caja 33', 'Flujo Caja 34',
                'Flujo Caja 35', 'Flujo Caja 36', 'Flujo Caja 37', 'Flujo Caja 38',
                'Flujo Caja 39', 'Flujo Caja 40'
            ]

            # Nombre de la nueva columna de flujo de caja en df_sabana_transmision
            nueva_columna_flujo_caja = 'FLUJO_CAJA'

            # Agregar una nueva columna de flujo de caja a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_flujo_caja, nueva_columna_flujo_caja, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'FLUJO_CAJA' con 0
            df_sabana_transmision["FLUJO_CAJA"] = df_sabana_transmision["FLUJO_CAJA"].fillna(0)


            # TRANS_NACION--------------------------------------------------------------------------------------
            # Columnas de transacción a la Nación en df_trans_vias
            columnas_impuestos = [
                'Impuestos 2024', 'Impuestos 2025',	'Impuestos 2026', 
                'Impuestos 2027', 'Impuestos 2028',	'Impuestos 2029',
                'Impuestos 2030', 'Impuestos 2031',	'Impuestos 2032',
                'Impuestos 2033', 'Impuestos 2034',	'Impuestos 2035', 
                'Impuestos 2036', 'Impuestos 2037',	'Impuestos 2038', 
                'Impuestos 2039', 'Impuestos 2040'    
            ]

            # Nombre de la nueva columna de transacción a la Nación en df_sabana_transmision
            nueva_columna_impuestos = 'IMPUESTOS'

            # Agregar una nueva columna de transacción a la Nación a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_impuestos, nueva_columna_impuestos, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'TRANS_NACION' con 0
            df_sabana_transmision["IMPUESTOS"] = df_sabana_transmision["IMPUESTOS"].fillna(0)

            #   # ROACE--------------------------------------------------------------------------------------
            #   # Columnas de ROACE en df_trans_vias
            #   columnas_roace = [
            #       'ROACE 24', 'ROACE 25', 'ROACE 26',
            #       'ROACE 27', 'ROACE 28', 'ROACE 29', 'ROACE 30',
            #       'ROACE 31', 'ROACE 32', 'ROACE 33', 'ROACE 34',
            #       'ROACE 35', 'ROACE 36', 'ROACE 37', 'ROACE 38',
            #       'ROACE 39', 'ROACE 40'
            #   ]

            #   # Nombre de la nueva columna de ROACE en df_sabana_transmision
            #   nueva_columna_roace = 'ROACE'

            #   # Agregar una nueva columna de ROACE a df_sabana_transmision y expandir los valores
            #   df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_roace, nueva_columna_roace, annio_inicio, annio_fin)

            #   # Llenar los valores NaN en la columna 'ROACE' con 0
            #   df_sabana_transmision["ROACE"] = df_sabana_transmision["ROACE"].fillna(0)


            # KM_RED--------------------------------------------------------------------------------------
            # Columnas de KM de Red en df_trans_vias
            columnas_km_red = [
                'KM de Red 24', 'KM de Red 25', 'KM de Red 26',
                'KM de Red 27', 'KM de Red 28', 'KM de Red 29', 'KM de Red 30',
                'KM de Red 31', 'KM de Red 32', 'KM de Red 33', 'KM de Red 34',
                'KM de Red 35', 'KM de Red 36', 'KM de Red 37', 'KM de Red 38',
                'KM de Red 39', 'KM de Red 40'
            ]

            # Nombre de la nueva columna de KM de Red en df_sabana_transmision
            nueva_columna_km_red = 'KM_RED'

            # Agregar una nueva columna de KM de Red a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_km_red, nueva_columna_km_red, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'KM_RED' con 0
            df_sabana_transmision["KM_RED"] = df_sabana_transmision["KM_RED"].fillna(0)


            # KM_VIAS--------------------------------------------------------------------------------------
            # Columnas de KM de Vías en df_trans_vias
            columnas_km = [
                'Km 2024',	'Km 2025',	
                'Km 2026', 'Km 2027', 'Km 2028',	
                'Km 2029', 'Km 2030', 'Km 2031',	
                'Km 2032', 'Km 2033', 'Km 2034',
                'Km 2035', 'Km 2036', 'Km 2037',
                'Km 2038', 'Km 2039', 'Km 2040'
            ]

            # Nombre de la nueva columna de KM de Vías en df_sabana_transmision
            nueva_columna_km = 'KM'

            # Agregar una nueva columna de KM de Vías a df_sabana_transmision y expandir los valores
            df_sabana_transmision = plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, columnas_km, nueva_columna_km, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'KM_VIAS' con 0
            df_sabana_transmision["KM"] = df_sabana_transmision["KM"].fillna(0)

            #-------------------------------------------------------------------------------------------------

            # Eliminar columnas duplicadas en df_sabana_transmision
            df_sabana_transmision = df_sabana_transmision.loc[:, ~df_sabana_transmision.columns.duplicated()]

            #Calculos del Flujo de caja adicionales 

            #_-----------------------------------------------------------
            # Adiciono las columnas precio_crudo,  
            payout =  self.flujo_caja_config["parametros"]["payout"] 

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios= list(range(annio_inicio, annio_fin))

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios,        
                    'PAYOUT':payout
                    }

            df_anio_crudo= pd.DataFrame(data)

            df_sabana_transmision = df_sabana_transmision.merge(df_anio_crudo, on='RANGO_ANIO', how='left')

            df_sabana_transmision['EBIT'] =  df_sabana_transmision['EBITDA'] - df_sabana_transmision['IMPUESTOS']

            # Dividendo	 utilidad * payout
            df_sabana_transmision['DIVIDENDO'] = df_sabana_transmision['EBIT'] * (df_sabana_transmision['PAYOUT'])

            # Aplicar la función calcular_duracion_proyectos al grupo 'ID_PROYECTO' en df_sabana_transmision
            df_sabana_transmision = df_sabana_transmision.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)
        else:
            logger.info(f"El DataFrame Transmision y Vias (df_trans_vias) está vacío.") 

               
       
       #-----------------------------------------------
       # ----------------------------------down_icos
        
        #PLP
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_downstream = self.flujo_caja_config["fuente"]["plp"]["upstream_config"]["downstream"]
        df_plp_downstream = pd.read_excel(file_path_up, sheet_name=plp_downstream, header= 2, engine='openpyxl')


        if not df_plp_downstream.empty:
            # Crear un DataFrame vacío para almacenar los datos de transmisión
            df_sabana_downstream = pd.DataFrame()

            # Copiar datos de otras columnas al DataFrame
            df_sabana_downstream.loc[:,"MATRICULA_DIGITAL"] = df_plp_downstream["Matrícula Digital"]
            df_sabana_downstream.loc[:,"LINEA_DE_NEGOCIO"] = df_plp_downstream["Linea de Negocio"]
            df_sabana_downstream.loc[:,"OPCION_ESTRATEGICA"] = df_plp_downstream["Opción estratégica"]
            df_sabana_downstream.loc[:,"EMPRESA"] = df_plp_downstream["Empresa"]
            df_sabana_downstream.loc[:,"SEGMENTO"] = df_plp_downstream["Segmento"].str.title()
            df_sabana_downstream.loc[:,"SUBSEGMENTO"] = df_plp_downstream["Subsegmento"].str.title()
            df_sabana_downstream.loc[:,"UN_NEG_REGIONAL"] = df_plp_downstream["Un. Neg./Regional"]
            df_sabana_downstream.loc[:,"GERENCIA"] = df_plp_downstream["Gerencia"].str.strip()
            df_sabana_downstream.loc[:,"ACTIVO"] = df_plp_downstream["Activo"]
            df_sabana_downstream.loc[:,"DEPARTAMENTO"] = df_plp_downstream["Departamento"]
            df_sabana_downstream.loc[:,"TIPO_DE_INVERSION"] = df_plp_downstream["Tipo de inversión"]
            df_sabana_downstream.loc[:,"NOMBRE_PROYECTO"] = df_plp_downstream["Nombre Proyecto"]
            df_sabana_downstream.loc[:,"WI"] = df_plp_downstream["WI"].fillna(0)
            df_sabana_downstream.loc[:,"FASE_EN_CURSO"] = df_plp_downstream["Fase en curso"]
            df_sabana_downstream.loc[:,"CAPEX_MENOR_ANIO"] = df_plp_downstream["CapEx < 23"].fillna(0)
            df_sabana_downstream.loc[:,"CAPEX_MAYOR_ANIO"] = df_plp_downstream["CapEx > 40"].fillna(0)
            df_sabana_downstream.loc[:,"CAPEX_TOTAL"] = df_plp_downstream["CapEx Total"].fillna(0)
            df_sabana_downstream.loc[:,"VPN"] = df_plp_downstream["VPN"].fillna(0)
            df_sabana_downstream.loc[:,"VPI"] = df_plp_downstream["VPI"].fillna(0)
            df_sabana_downstream.loc[:,"EFI"] = df_plp_downstream["EFI"].fillna(0)
            df_sabana_downstream.loc[:,"TIR"] = df_plp_downstream["TIR"].fillna(0)
            df_sabana_downstream.loc[:,"PAY_BACK"] = df_plp_downstream["Pay Back"].fillna(0)
            df_sabana_downstream.loc[:,"OPEX_UNITARIO"] = df_plp_downstream["Opex Unitario"].fillna(0)
            df_sabana_downstream.loc[:,"COSTO_TOTAL_UNITARIO"] = df_plp_downstream["Costo Total Unitario"].fillna(0)
            df_sabana_downstream.loc[:,"MARGEN_INCREMENTAL"] = df_plp_downstream["Margen Incremental"].fillna(0)
            df_sabana_downstream.loc[:,"VPN_CON_CO2"] = df_plp_downstream["VPN con Co2"].fillna(0)
            df_sabana_downstream.loc[:,"PAY_BACK_CON_CO2"] = df_plp_downstream["Pay Back con Co2"].fillna(0)
            df_sabana_downstream.loc[:,"APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] = df_plp_downstream["Aporte EBITDA Acum 2040 proyectos asociados a CT+i"].fillna(0)
            df_sabana_downstream.loc[:,"CATEGORIA_TESG_PALANCAS"] = df_plp_downstream["Categoria TESG (Palancas)"].fillna(0)

            df_sabana_downstream["LINEA_DE_NEGOCIO"] = df_sabana_downstream["LINEA_DE_NEGOCIO"].str.strip()

            # Aplicar la función a la columna "Valor"
            df_sabana_downstream['MATRICULA_DIGITAL'] = df_sabana_downstream['MATRICULA_DIGITAL'].apply(fill_alphanumeric_for_nan_or_empty)

            # Insertar una columna "ID_PROYECTO" al principio del DataFrame
            df_sabana_downstream.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_downstream) + 1))

            # Obtener el rango de años de la configuración
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1

            # Función para expandir cada fila del DataFrame con un rango de años


            # Aplicar la función expand_row a cada fila del DataFrame y concatenar los resultados
            df_sabana_downstream = pd.concat(df_sabana_downstream.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

            # Obtener las columnas de CapEx en df_plp_downstream
            # columnas_agregar_capex = df_plp_downstream.columns[df_plp_downstream.columns.str.match("CapEx \d+")]
            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']

            # Nombre de la nueva columna de CapEx en df_sabana_downstream
            nombre_columna_capex = 'CAPEX'

            # Agregar una nueva columna de CapEx a df_sabana_downstream y expandir los valores
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)

            # Llenar los valores NaN en la columna 'CAPEX' con 0
            df_sabana_downstream["CAPEX"] = df_sabana_downstream["CAPEX"].fillna(0)

            #-------------------------------------------------------------------------------------------------
            #-----------------------------EMISONES NETAS
            # Emisiones Netas Co2_Alcance 1 y 2
            columnas_emisiones_netas_co2_alcance_1_y_2 = [
                'Emisiones Netas Co2 Alcance 1 y 2 2024',
                'Emisiones Netas Co2 Alcance 1 y 2 2025', 'Emisiones Netas Co2 Alcance 1 y 2 2026',
                'Emisiones Netas Co2 Alcance 1 y 2 2027', 'Emisiones Netas Co2 Alcance 1 y 2 2028',
                'Emisiones Netas Co2 Alcance 1 y 2 2029', 'Emisiones Netas Co2 Alcance 1 y 2 2030',
                'Emisiones Netas Co2 Alcance 1 y 2 2031', 'Emisiones Netas Co2 Alcance 1 y 2 2032',
                'Emisiones Netas Co2 Alcance 1 y 2 2033', 'Emisiones Netas Co2 Alcance 1 y 2 2034',
                'Emisiones Netas Co2 Alcance 1 y 2 2035', 'Emisiones Netas Co2 Alcance 1 y 2 2036',
                'Emisiones Netas Co2 Alcance 1 y 2 2037', 'Emisiones Netas Co2 Alcance 1 y 2 2038',
                'Emisiones Netas Co2 Alcance 1 y 2 2039', 'Emisiones Netas Co2 Alcance 1 y 2 2040'
            ]

            nueva_columna_emisiones_netas_co2_alcance_1_y_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'

            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_emisiones_netas_co2_alcance_1_y_2, nueva_columna_emisiones_netas_co2_alcance_1_y_2, annio_inicio, annio_fin)
            df_sabana_downstream["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_sabana_downstream["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)

            #-------------------------------AGUA NEUTRALIDAD

            columnas_agua_neutralidad= [
            'Agua Neutralidad 2024', 'Agua Neutralidad 2025', 'Agua Neutralidad 2026', 	 
            'Agua Neutralidad 2027', 'Agua Neutralidad 2028', 'Agua Neutralidad 2029', 	'Agua Neutralidad 2030', 	 
            'Agua Neutralidad 2031', 'Agua Neutralidad 2032', 'Agua Neutralidad 2033', 'Agua Neutralidad 2034',  
            'Agua Neutralidad 2035', 'Agua Neutralidad 2036', 'Agua Neutralidad 2037', 'Agua Neutralidad 2038', 
            'Agua Neutralidad 2039', 'Agua Neutralidad 2040'
            ]

            nueva_columna_agua = 'AGUA_NEUTRALIDAD'

            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_agua_neutralidad, nueva_columna_agua, annio_inicio, annio_fin)
            df_sabana_downstream["AGUA_NEUTRALIDAD"]  = df_sabana_downstream["AGUA_NEUTRALIDAD"] .replace('-', '')
            df_sabana_downstream["AGUA_NEUTRALIDAD"] = df_sabana_downstream["AGUA_NEUTRALIDAD"].fillna(0)

            #-----------------------------Empleos NP 

            columnas_empleon_np= ['Empleos NP 2024','Empleos NP 2025',	
                                    'Empleos NP 2026','Empleos NP 2027','Empleos NP 2028',
                                    'Empleos NP 2029','Empleos NP 2030','Empleos NP 2031',
                                    'Empleos NP 2032','Empleos NP 2033','Empleos NP 2034',
                                    'Empleos NP 2035','Empleos NP 2036','Empleos NP 2037',
                                    'Empleos NP 2038','Empleos NP 2039','Empleos NP 2040' ]

            nueva_columna_empleon = 'EMPLEOS_NP'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_empleon_np, nueva_columna_empleon, annio_inicio, annio_fin)
            df_sabana_downstream["EMPLEOS_NP"] = df_sabana_downstream["EMPLEOS_NP"].fillna(0)

            #----------------------------------------------Estudiantes

            columnas= ['Estudiantes 2024', 'Estudiantes 2025',
                                    'Estudiantes 2026', 'Estudiantes 2027', 'Estudiantes 2028',
                                    'Estudiantes 2029', 'Estudiantes 2030', 'Estudiantes 2031', 
                                    'Estudiantes 2032', 'Estudiantes 2033', 'Estudiantes 2034',	
                                    'Estudiantes 2035', 'Estudiantes 2036', 'Estudiantes 2037',
                                    'Estudiantes 2038',	'Estudiantes 2039',	'Estudiantes 2040' ]

            nueva_columna= 'ESTUDIANTES'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas, nueva_columna, annio_inicio, annio_fin)
            df_sabana_downstream["ESTUDIANTES"] = df_sabana_downstream["ESTUDIANTES"].fillna(0)

            #----------------------------------ACCESO AGUA POTABLE
            columnas_acceso_potable= ['Acceso agua potable 2024', 'Acceso agua potable 2025',	 'Acceso agua potable 2026','Acceso agua potable 2027',	'Acceso agua potable 2028',	'Acceso agua potable 2029',	'Acceso agua potable 2030',	'Acceso agua potable 2031',	'Acceso agua potable 2032',	'Acceso agua potable 2033',	'Acceso agua potable 2034',	'Acceso agua potable 2035',	'Acceso agua potable 2036',	'Acceso agua potable 2037',	'Acceso agua potable 2038',	'Acceso agua potable 2039','Acceso agua potable 2040']

            nueva_columna_acceso_agua_potable = 'ACCESO_AGUA_POTABLE'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_acceso_potable, nueva_columna_acceso_agua_potable, annio_inicio, annio_fin)
            df_sabana_downstream["ACCESO_AGUA_POTABLE"] = df_sabana_downstream["ACCESO_AGUA_POTABLE"].fillna(0)


            #-----------------ACCESO A GAS
            columnas_acceso_gas= ['Acceso Gas 2024','Acceso Gas 2025', 'Acceso Gas 2026','Acceso Gas 2027','Acceso Gas 2028','Acceso Gas 2029','Acceso Gas 2030','Acceso Gas 2031','Acceso Gas 2032',	'Acceso Gas 2033','Acceso Gas 2034','Acceso Gas 2035','Acceso Gas 2036','Acceso Gas 2037','Acceso Gas 2038',	'Acceso Gas 2039','Acceso Gas 2040']

            nueva_columna_acceso_gas= 'ACCESO_GAS'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_downstream["ACCESO_GAS"] = df_sabana_downstream["ACCESO_GAS"].fillna(0)
                    
            #-----------------KM
            columnas_acceso_km= ['Km 2024', 'Km 2025', 'Km 2026', 'Km 2027', 'Km 2028', 'Km 2029', 'Km 2030', 'Km 2031', 'Km 2032', 'Km 2033', 'Km 2034', 'Km 2035', 'Km 2036', 'Km 2037', 'Km 2038', 'Km 2039	Km 2040']

            nueva_columna_acceso_gas= 'KM'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_downstream["KM"] = df_sabana_downstream["KM"].fillna(0)



            #-----------------EBITDA_PLP para los proyectos on y opt
            columnas_ebitda_plp = [
                'EBITDA Total 24', 'EBITDA Total 25',
                'EBITDA Total 26', 'EBITDA Total 27', 'EBITDA Total 28',
                'EBITDA Total 29', 'EBITDA Total 30', 'EBITDA Total 31',
                'EBITDA Total 32', 'EBITDA Total 33', 'EBITDA Total 34',	
                'EBITDA Total 35', 'EBITDA Total 36', 'EBITDA Total 37',
                'EBITDA Total 38', 'EBITDA Total 39', 'EBITDA Total 40']
            nueva_columna_ebitda_plp = 'EBITDA_PLP'
            df_sabana_downstream = plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, columnas_ebitda_plp, nueva_columna_ebitda_plp, annio_inicio, annio_fin)
            df_sabana_downstream["EBITDA_PLP"] = df_sabana_downstream["EBITDA_PLP"].fillna(0)   

            # Eliminar columnas duplicadas en df_sabana_downstream
            df_sabana_downstream = df_sabana_downstream.loc[:, ~df_sabana_downstream.columns.duplicated()]

                # Adiciono las columnas precio_crudo,  
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]
            impuesto_renta =  self.flujo_caja_config["parametros"]["impuesto_renta"]
            payout =  self.flujo_caja_config["parametros"]["payout"] 
            precio_co2 =  self.flujo_caja_config["parametros"]["precio_co2"]
            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]
            impuesto_renta_20 = self.flujo_caja_config["parametros"]["impuesto_renta_20"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios= list(range(annio_inicio, annio_fin))

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios , 
                    'PRECIO_CRUDO_BRENT': precio_crudo, 
                    'IMPUESTO_RENTA': impuesto_renta, 
                    'IMPUESTO_RENTA_20':impuesto_renta_20,
                    'PAYOUT':payout,        
                    'PRECIO_CO2':precio_co2        
                    }

            df_anio_crudo= pd.DataFrame(data)

            df_sabana_downstream = df_sabana_downstream.merge(df_anio_crudo, on='RANGO_ANIO', how='left')
            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anioss = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anioss, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Replicar registros de df1 para que coincida con la longitud de df2
            replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_sabana_downstream) // len(df_sobretasa)), ignore_index=True)

            # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
            replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_sabana_downstream)]

            df_sabana_downstream['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']

                #filtrar down para los proyectos ICOs
            df_condicion1 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'ICO']
            df_condicion2 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'EST']
            df_condicion3 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'ICO - Mto Mayor']
            df_condicion4 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'ICO - Normativa']
            df_condicion5 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'ON']
            df_condicion6 = df_sabana_downstream[df_sabana_downstream['TIPO_DE_INVERSION'] == 'OPT']

            df_sabana_downstream_icos = pd.concat([df_condicion1, df_condicion2, df_condicion3, df_condicion4],ignore_index=True)
            df_downstream_on_opt = pd.concat([df_condicion5, df_condicion6],ignore_index=True)

            #Flujo caja Downstream proyectos ICOS
            #Filtrar solos los icos
            if not df_sabana_downstream_icos.empty:
                    #Calcular depreciacion 
                    df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_downstream_icos['ID_PROYECTO'].unique()})

                    cantidad_anios_depreciacion = self.flujo_caja_config["parametros"]["cantidad_anios_depreciacion_down"] 

                    df_total = pd.DataFrame()

                    for index, row in df_id_proyectos.iterrows():
                        ID_PROYECTO = row['ID_PROYECTO']

                        columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                        columna_filtro_valor = ID_PROYECTO

                        df_data = df_sabana_downstream_icos[df_sabana_downstream_icos['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                        df_depreciacion = calcular_depreciacion_icos_downstream(df_data, cantidad_anios_depreciacion)

                        merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                        #calcular capital de empleado
                        df_con_ppe = calcular_ppe(merged_df1)

                        df_total = df_total.append(df_con_ppe, ignore_index=True)   

                    df_sabana_downstream_icos = pd.merge(df_sabana_downstream_icos, df_total, how='inner')


                    df_sabana_downstream_icos['TOTAL_INGRESOS'] = 0

                    df_sabana_downstream_icos['COSTO_INTERNO_CO2'] = df_sabana_downstream_icos['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_downstream_icos['PRECIO_CO2']

                    df_sabana_downstream_icos['TOTAL_COSTOS'] = df_sabana_downstream_icos['COSTO_INTERNO_CO2']
                                            
                    df_sabana_downstream_icos['EBITDA']  = df_sabana_downstream_icos['TOTAL_INGRESOS'] - df_sabana_downstream_icos['TOTAL_COSTOS']  # EBITDA

                    df_sabana_downstream_icos['BASE_GRABABLE'] = df_sabana_downstream_icos['EBITDA']  + df_sabana_downstream_icos['COSTO_INTERNO_CO2']  - df_sabana_downstream_icos['SUM_DEPRE']

                    # Aplicamos la función 'calcular_impuestos' a cada fila del DataFrame y almacenamos los resultados en una nueva columna 'Impuestos'
                    df_sabana_downstream_icos['IMPUESTOS'] = df_sabana_downstream_icos.apply(calcular_impuestos_icos, axis=1)

                    # capex	 df_sabana_datos['CAPEX']
                    # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
                    df_sabana_downstream_icos['FLUJO_CAJA']= df_sabana_downstream_icos['EBITDA']  - df_sabana_downstream_icos['IMPUESTOS'] - df_sabana_downstream_icos['CAPEX']

                    df_sabana_downstream_icos['EBIT'] =  df_sabana_downstream_icos['EBITDA'] - df_sabana_downstream_icos['IMPUESTOS']

                    # Dividendo	 utilidad * payout
                    df_sabana_downstream_icos['DIVIDENDO'] = df_sabana_downstream_icos['EBIT'] * (df_sabana_downstream_icos['PAYOUT'])

                    df_sabana_downstream_icos["CAPITAL_EMPLEADO"] = df_sabana_downstream_icos["CAPITAL_EMPLEADO"].fillna(0) 
                    df_sabana_downstream_icos['EBIT'] = df_sabana_downstream_icos["EBIT"].fillna(0)

                    # df_sabana_downstream_icos['ROACE'] = (df_sabana_downstream_icos['EBIT'] / df_sabana_downstream_icos['CAPITAL_EMPLEADO']) 
                    # df_sabana_downstream_icos['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                    # df_sabana_downstream_icos['ROACE'] = df_sabana_downstream_icos["ROACE"].fillna(0)
            else: 
                    logger.info(f"Dataframe Vacio (df_sabana_downstream_icos)")
            
            #----------------------------------------------------
            #fLUJO DE CAJA DOWN ON _ OPT 
            #Calcular depreciacion DOWN ON _ OPT 

            if not df_downstream_on_opt.empty:
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_downstream_on_opt['ID_PROYECTO'].unique()})
                
                cantidad_anios_depreciacion = self.flujo_caja_config["parametros"]["cantidad_anios_depreciacion_down"] 

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']  
                    
                    columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                    columna_filtro_valor = ID_PROYECTO

                    df_data = df_downstream_on_opt[df_downstream_on_opt['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                    df_depreciacion = calcular_depreciacion_icos_downstream(df_data, cantidad_anios_depreciacion)
                    
                    merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df1)
                    
                    df_total = df_total.append(df_con_ppe, ignore_index=True)   
                
                df_downstream_on_opt = pd.merge(df_downstream_on_opt, df_total, how='inner')


                df_downstream_on_opt['TOTAL_INGRESOS'] = 0

                df_downstream_on_opt['COSTO_INTERNO_CO2'] = df_downstream_on_opt['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_downstream_on_opt['PRECIO_CO2']

                df_downstream_on_opt['TOTAL_COSTOS'] = df_downstream_on_opt['COSTO_INTERNO_CO2']
                        
                df_downstream_on_opt['EBITDA']  = df_downstream_on_opt['EBITDA_PLP']

                df_downstream_on_opt['BASE_GRABABLE'] = df_downstream_on_opt['EBITDA']  + df_downstream_on_opt['COSTO_INTERNO_CO2']  - df_downstream_on_opt['SUM_DEPRE']

                # Aplicamos la función 'calcular_impuestos' a cada fila del DataFrame y almacenamos los resultados en una nueva columna 'Impuestos'
                df_downstream_on_opt['IMPUESTOS'] = df_downstream_on_opt.apply(calcular_impuestos_icos, axis=1)

                # capex	 df_sabana_datos['CAPEX']
                # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
                df_downstream_on_opt['FLUJO_CAJA']= df_downstream_on_opt['EBITDA']  - df_downstream_on_opt['IMPUESTOS'] - df_downstream_on_opt['CAPEX']
                
                df_downstream_on_opt['EBIT'] =  df_downstream_on_opt['EBITDA'] - df_downstream_on_opt['IMPUESTOS']
                
                # Dividendo	 utilidad * payout
                df_downstream_on_opt['DIVIDENDO'] = df_downstream_on_opt['EBIT'] * (df_downstream_on_opt['PAYOUT'])

                df_downstream_on_opt["CAPITAL_EMPLEADO"] = df_downstream_on_opt["CAPITAL_EMPLEADO"].fillna(0) 
                df_downstream_on_opt['EBIT'] = df_downstream_on_opt["EBIT"].fillna(0)

                # df_downstream_on_opt['ROACE'] = (df_downstream_on_opt['EBIT'] / df_downstream_on_opt['CAPITAL_EMPLEADO']) 
                # df_downstream_on_opt['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_downstream_on_opt['ROACE'] = df_downstream_on_opt["ROACE"].fillna(0)  
            else: 
                    logger.info(f"Dataframe Vacio (df_downstream_on_opt)")

            df_sabana_downstream_total = pd.concat([df_sabana_downstream_icos, df_downstream_on_opt],ignore_index=True, axis=0)
            df_sabana_downstream_total = df_sabana_downstream_total.drop('EBITDA_PLP', axis=1 )
            df_sabana_downstream_total = df_sabana_downstream_total.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)

        else:
            logger.info(f"El DataFrame downstream (df_plp_downstream) está vacío.") 
        
        
       
        #------------------------------------------------------------
        #------------------------------------------------Midstream
        
        #PLP
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_midstream = self.flujo_caja_config["fuente"]["plp"]["upstream_config"]["midstream"]
        df_plp_midstream = pd.read_excel(file_path_up, sheet_name=plp_midstream, header= 2, engine='openpyxl')
        df_plp_midstream = df_plp_midstream.dropna(axis=0, how='all')  

        if not df_plp_midstream.empty:   
            #------------------------------
            # Crear un DataFrame vacío para almacenar los datos de transmisión
            df_sabana_midstream = pd.DataFrame()
            # Copiar datos de otras columnas al DataFrame
            df_sabana_midstream.loc[:,"MATRICULA_DIGITAL"] = df_plp_midstream["Matrícula Digital"]
            df_sabana_midstream.loc[:,"LINEA_DE_NEGOCIO"] = df_plp_midstream["Linea de Negocio"]
            df_sabana_midstream.loc[:,"OPCION_ESTRATEGICA"] = df_plp_midstream["Opción Estratégica"]
            df_sabana_midstream.loc[:,"EMPRESA"] = df_plp_midstream["Empresa"]
            df_sabana_midstream.loc[:,"SEGMENTO"] = df_plp_midstream["Segmento"].str.title()
            df_sabana_midstream.loc[:,"SUBSEGMENTO"] = df_plp_midstream["Subsegmento"]
            df_sabana_midstream.loc[:,"UN_NEG_REGIONAL"] = df_plp_midstream["Un. Neg./Regional"]
            df_sabana_midstream.loc[:,"ACTIVO"] = df_plp_midstream["Activo"]
            df_sabana_midstream.loc[:,"PRODUCTO_A_TRANSPORTAR"] = df_plp_midstream["Producto a Transportar"]
            df_sabana_midstream.loc[:,"DEPARTAMENTO"] = df_plp_midstream["Departamento"]
            df_sabana_midstream.loc[:,"TIPO_DE_INVERSION"] = df_plp_midstream["Tipo de inversión"]
            df_sabana_midstream.loc[:,"NOMBRE_PROYECTO"] = df_plp_midstream["Nombre Proyecto"]
            df_sabana_midstream.loc[:,"WI"] = df_plp_midstream["WI"].fillna(0)
            df_sabana_midstream.loc[:,"FASE_EN_CURSO"] = df_plp_midstream["Fase en curso"]
            df_sabana_midstream.loc[:,"CAPEX_MENOR_ANIO"] = df_plp_midstream["CapEx < 23"].fillna(0)
            df_sabana_midstream.loc[:,"CAPEX_MAYOR_ANIO"] = df_plp_midstream["CapEx > 40"].fillna(0)
            df_sabana_midstream.loc[:,"CAPEX_TOTAL"] = df_plp_midstream["CapEx Total"].fillna(0)
            df_sabana_midstream.loc[:,"VPN"] = df_plp_midstream["VPN"].fillna(0)
            df_sabana_midstream.loc[:,"VPI"] = df_plp_midstream["VPI"].fillna(0)
            df_sabana_midstream.loc[:,"EFI"] = df_plp_midstream["EFI"].fillna(0)
            df_sabana_midstream.loc[:,"TIR"] = df_plp_midstream["TIR"].fillna(0)
            df_sabana_midstream.loc[:,"PAY_BACK"] = df_plp_midstream["Pay Back"].fillna(0)
            df_sabana_midstream.loc[:,"BENEFICIO_COSTO"] = df_plp_midstream["Beneficio / Costo"].fillna(0)
            df_sabana_midstream.loc[:,"OPEX_UNITARIO"] = df_plp_midstream["OpEx Unitario"].fillna(0)
            df_sabana_midstream.loc[:,"CAPEX_UNITARIO"] = df_plp_midstream["CapEx Unitario"].fillna(0)
            df_sabana_midstream.loc[:,"VPN_CON_CO2"] = df_plp_midstream["VPN con Co2"].fillna(0)
            df_sabana_midstream.loc[:,"VPI_CON_CO2"] = df_plp_midstream["VPI con Co2"].fillna(0)
            df_sabana_midstream.loc[:,"EFI_CON_CO2"] = df_plp_midstream["EFI con Co2"].fillna(0)
            df_sabana_midstream.loc[:,"TIR_CON_CO2"] = df_plp_midstream["TIR con Co2"].fillna(0)
            df_sabana_midstream.loc[:,"PAY_BACK_CON_CO2"] = df_plp_midstream["Pay Back con Co2"].fillna(0)
            df_sabana_midstream.loc[:,"APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] = df_plp_midstream["Aporte EBITDA Acum 2040 proyectos asociados a CT+i"].fillna(0)  
            df_sabana_midstream.loc[:,"CATEGORIA_TESG_PALANCAS"] = df_plp_midstream["Categoria TESG (Palancas)"].fillna(0)


            # Aplicar la función a la columna 'columna_sin_asignar' y asignar el resultado a una nueva columna
            df_sabana_midstream['MATRICULA_DIGITAL'] = df_sabana_midstream.apply(asignar_valor_mid, axis=1)

            # ---------------------------------------------
            # Insertar una columna "ID_PROYECTO" al principio del DataFrame
            df_sabana_midstream.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_midstream) + 1))

            # Obtener el rango de años de la configuración
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1

            # Función para expandir cada fila del DataFrame con un rango de años


            # Aplicar la función expand_row a cada fila del DataFrame y concatenar los resultados
            df_sabana_midstream = pd.concat(df_sabana_midstream.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

            #--------------------------------------------------------
            #capex 
            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']
            #columnas_agregar_capex = df_plp_midstream.columns[df_plp_midstream.columns.str.match("CapEx \d+")]
            nombre_columna_capex = 'CAPEX'
            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)
            df_sabana_midstream["CAPEX"] = df_sabana_midstream["CAPEX"].fillna(0)


            #-----------------------------EMISONES NETAS
            #Emisiones Netas Co2_Alcance 1 y 2
            columnas_emisiones_netas_co2_alcance_1_y_2 = [
                'Emisiones Netas Co2 Alcance 1 y 2 2024',
                'Emisiones Netas Co2 Alcance 1 y 2 2025', 'Emisiones Netas Co2 Alcance 1 y 2 2026',
                'Emisiones Netas Co2 Alcance 1 y 2 2027', 'Emisiones Netas Co2 Alcance 1 y 2 2028',
                'Emisiones Netas Co2 Alcance 1 y 2 2029', 'Emisiones Netas Co2 Alcance 1 y 2 2030',
                'Emisiones Netas Co2 Alcance 1 y 2 2031', 'Emisiones Netas Co2 Alcance 1 y 2 2032',
                'Emisiones Netas Co2 Alcance 1 y 2 2033', 'Emisiones Netas Co2 Alcance 1 y 2 2034',
                'Emisiones Netas Co2 Alcance 1 y 2 2035', 'Emisiones Netas Co2 Alcance 1 y 2 2036',
                'Emisiones Netas Co2 Alcance 1 y 2 2037', 'Emisiones Netas Co2 Alcance 1 y 2 2038',
                'Emisiones Netas Co2 Alcance 1 y 2 2039', 'Emisiones Netas Co2 Alcance 1 y 2 2040'
            ]

            nueva_columna_emisiones_netas_co2_alcance_1_y_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_emisiones_netas_co2_alcance_1_y_2, nueva_columna_emisiones_netas_co2_alcance_1_y_2, annio_inicio, annio_fin)
            df_sabana_midstream["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_sabana_midstream["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)


            #-----------------------------AGUA_NEUTRALIDAD
            columnas_agua_neutralidad = [
                'Agua Neutralidad 2024',
                'Agua Neutralidad 2025', 'Agua Neutralidad 2026',
                'Agua Neutralidad 2027', 'Agua Neutralidad 2028',
                'Agua Neutralidad 2029', 'Agua Neutralidad 2030',
                'Agua Neutralidad 2031', 'Agua Neutralidad 2032',
                'Agua Neutralidad 2033', 'Agua Neutralidad 2034',
                'Agua Neutralidad 2035', 'Agua Neutralidad 2036',
                'Agua Neutralidad 2037', 'Agua Neutralidad 2038',
                'Agua Neutralidad 2039', 'Agua Neutralidad 2040'
            ]

            nueva_columna_agua_neutralidad = 'AGUA_NEUTRALIDAD'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_agua_neutralidad, nueva_columna_agua_neutralidad, annio_inicio, annio_fin)
            df_sabana_midstream["AGUA_NEUTRALIDAD"] = df_sabana_midstream["AGUA_NEUTRALIDAD"].fillna(0)

            #----------------------------EMPLEO_NP
            columnas_empleo_np = [
                'Empleos NP 2024',
                'Empleos NP 2025', 'Empleos NP 2026',
                'Empleos NP 2027', 'Empleos NP 2028',
                'Empleos NP 2029', 'Empleos NP 2030',
                'Empleos NP 2031', 'Empleos NP 2032',
                'Empleos NP 2033', 'Empleos NP 2034',
                'Empleos NP 2035', 'Empleos NP 2036',
                'Empleos NP 2037', 'Empleos NP 2038',
                'Empleos NP 2039', 'Empleos NP 2040'
            ]

            nueva_columna_empleo_np = 'EMPLEOS_NP'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_empleo_np, nueva_columna_empleo_np, annio_inicio, annio_fin)
            df_sabana_midstream["EMPLEOS_NP"] = df_sabana_midstream["EMPLEOS_NP"].fillna(0)

            #-------------------------------ESTUDIANTES
            columnas_estudiantes = [
                'Estudiantes 2024',
                'Estudiantes 2025', 'Estudiantes 2026',
                'Estudiantes 2027', 'Estudiantes 2028',
                'Estudiantes 2029', 'Estudiantes 2030',
                'Estudiantes 2031', 'Estudiantes 2032',
                'Estudiantes 2033', 'Estudiantes 2034',
                'Estudiantes 2035', 'Estudiantes 2036',
                'Estudiantes 2037', 'Estudiantes 2038',
                'Estudiantes 2039', 'Estudiantes 2040'
            ]

            nueva_columna_estudiantes = 'ESTUDIANTES'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_estudiantes, nueva_columna_estudiantes, annio_inicio, annio_fin)
            df_sabana_midstream["ESTUDIANTES"] = df_sabana_midstream["ESTUDIANTES"].fillna(0)


            #-------------------------------------ACCESO_AGUA_POTABLE
            columnas_acceso_agua_potable = [
                'Acceso agua potable 2024',
                'Acceso agua potable 2025', 'Acceso agua potable 2026',
                'Acceso agua potable 2027', 'Acceso agua potable 2028',
                'Acceso agua potable 2029', 'Acceso agua potable 2030',
                'Acceso agua potable 2031', 'Acceso agua potable 2032',
                'Acceso agua potable 2033', 'Acceso agua potable 2034',
                'Acceso agua potable 2035', 'Acceso agua potable 2036',
                'Acceso agua potable 2037', 'Acceso agua potable 2038',
                'Acceso agua potable 2039', 'Acceso agua potable 2040'
            ]

            nueva_columna_acceso_agua_potable = 'ACCESO_AGUA_POTABLE'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_acceso_agua_potable, nueva_columna_acceso_agua_potable, annio_inicio, annio_fin)
            df_sabana_midstream["ACCESO_AGUA_POTABLE"] = df_sabana_midstream["ACCESO_AGUA_POTABLE"].fillna(0)


            #---------------------------------ACCESO_GAS
            columnas_acceso_gas = [
                'Acceso Gas 2024',
                'Acceso Gas 2025', 'Acceso Gas 2026',
                'Acceso Gas 2027', 'Acceso Gas 2028',
                'Acceso Gas 2029', 'Acceso Gas 2030',
                'Acceso Gas 2031', 'Acceso Gas 2032',
                'Acceso Gas 2033', 'Acceso Gas 2034',
                'Acceso Gas 2035', 'Acceso Gas 2036',
                'Acceso Gas 2037', 'Acceso Gas 2038',
                'Acceso Gas 2039', 'Acceso Gas 2040'
            ]

            nueva_columna_acceso_gas = 'ACCESO_GAS'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_midstream["ACCESO_GAS"] = df_sabana_midstream["ACCESO_GAS"].fillna(0)

            #-----------------------------KM
            columnas_km = [
                'Km 2024',
                'Km 2025', 'Km 2026',
                'Km 2027', 'Km 2028',
                'Km 2029', 'Km 2030',
                'Km 2031', 'Km 2032',
                'Km 2033', 'Km 2034',
                'Km 2035', 'Km 2036',
                'Km 2037', 'Km 2038',
                'Km 2039', 'Km 2040'
            ]

            nueva_columnas_km = 'KM'

            df_sabana_midstream = plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream,columnas_km, nueva_columnas_km, annio_inicio, annio_fin)
            df_sabana_midstream["KM"] = df_sabana_midstream["KM"].fillna(0)

            #-------------------------------------------------------------------------------------------------

            # Eliminar columnas duplicadas en df_sabana_midstream
            df_sabana_midstream = df_sabana_midstream.loc[:, ~df_sabana_midstream.columns.duplicated()]

            #---------------------------------------
            # Adiciono las columnas precio_crudo
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]
            impuesto_renta =  self.flujo_caja_config["parametros"]["impuesto_renta"]
            payout =  self.flujo_caja_config["parametros"]["payout"] 
            precio_co2 =  self.flujo_caja_config["parametros"]["precio_co2"]
            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]
            impuesto_renta_20 = self.flujo_caja_config["parametros"]["impuesto_renta_20"]

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios , 
                    'PRECIO_CRUDO_BRENT': precio_crudo, 
                    'IMPUESTO_RENTA': impuesto_renta, 
                    'IMPUESTO_RENTA_20':impuesto_renta_20,               
                    'PRECIO_CO2':precio_co2,
                    'PAYOUT': payout
                    }

            df_anio_crudo= pd.DataFrame(data)

            df_sabana_midstream = df_sabana_midstream.merge(df_anio_crudo, on='RANGO_ANIO', how='left')
            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anios = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Replicar registros de df1 para que coincida con la longitud de df2
            replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_sabana_midstream) // len(df_sobretasa)), ignore_index=True)

            # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
            replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_sabana_midstream)]

            df_sabana_midstream['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']

            #----------------------------------------------
            #Calcular depreciacion 

            df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_midstream['ID_PROYECTO'].unique()})

            cantidad_anios_depreciacion = self.flujo_caja_config["parametros"]["cantidad_anios_depreciacion"] 

            df_total = pd.DataFrame()

            for index, row in df_id_proyectos.iterrows():
                ID_PROYECTO = row['ID_PROYECTO']  
                
                columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                columna_filtro_valor = ID_PROYECTO

                df_data = df_sabana_midstream[df_sabana_midstream['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                df_depreciacion = calcular_depreciacion(df_data)
                
                merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                #calcular capital de empleado
                df_con_ppe = calcular_ppe(merged_df1)
                
                df_total = df_total.append(df_con_ppe, ignore_index=True)   

            df_sabana_midstream = pd.merge(df_sabana_midstream, df_total, how='inner')
                    
            #------------------------------------------------------
            #Flujo caja Midstream
            #Filtrar solos los icos
            # INGRESOS  
            df_sabana_midstream['TOTAL_INGRESOS'] = 0

            df_sabana_midstream['COSTO_INTERNO_CO2'] = df_sabana_midstream['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_midstream['PRECIO_CO2']

            df_sabana_midstream['TOTAL_COSTOS'] = df_sabana_midstream['COSTO_INTERNO_CO2']
                    
            df_sabana_midstream['EBITDA']  = df_sabana_midstream['TOTAL_INGRESOS'] - df_sabana_midstream['TOTAL_COSTOS']  # EBITDA

            df_sabana_midstream['BASE_GRABABLE'] = df_sabana_midstream['EBITDA'] + df_sabana_midstream['COSTO_INTERNO_CO2'] - df_sabana_midstream['SUM_DEPRE']

            df_sabana_midstream['IMPUESTOS'] = df_sabana_midstream['BASE_GRABABLE'] * df_sabana_midstream['IMPUESTO_RENTA']

            # capex	 df_sabana_datos['CAPEX']
            # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
            df_sabana_midstream['FLUJO_CAJA']= df_sabana_midstream['EBITDA']  - df_sabana_midstream['IMPUESTOS'] - df_sabana_midstream['CAPEX']
            
            df_sabana_midstream['EBIT'] =  df_sabana_midstream['EBITDA'] - df_sabana_midstream['IMPUESTOS']
            
            # Dividendo	 utilidad * payout
            df_sabana_midstream['DIVIDENDO'] = df_sabana_midstream['EBIT'] * (df_sabana_midstream['PAYOUT'])

            df_sabana_midstream["CAPITAL_EMPLEADO"] = df_sabana_midstream["CAPITAL_EMPLEADO"].fillna(0) 
            df_sabana_midstream['EBIT'] = df_sabana_midstream["EBIT"].fillna(0)

            # df_sabana_midstream['ROACE'] = (df_sabana_midstream['EBIT'] / df_sabana_midstream['CAPITAL_EMPLEADO']) 
            # df_sabana_midstream['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
            # df_sabana_midstream['ROACE'] = df_sabana_midstream["ROACE"].fillna(0)

            df_sabana_midstream = df_sabana_midstream.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)
        else:
            logger.info(f"El DataFrame Midstream(df_plp_midstream) está vacío.") 
        
        
        #----------------------------------------------------------------
        # -------------------------------------------accorp
        #PLP - AACORP
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_aacorp = self.flujo_caja_config["fuente"]["plp"]["upstream_config"]["aacorp"]
        df_plp_aacorp = pd.read_excel(file_path_up, sheet_name=plp_aacorp, header= 2, engine='openpyxl')

        #---------------------------

        if not df_plp_aacorp.empty: 
            # Crear un DataFrame vacío para almacenar los datos de transmisión
            df_sabana_aacorp = pd.DataFrame()

            # Copiar datos de otras columnas al DataFrame
            df_sabana_aacorp.loc[:, "MATRICULA_DIGITAL"] = df_plp_aacorp.loc[:, "Matrícula Digital"]
            df_sabana_aacorp.loc[:, "LINEA_DE_NEGOCIO"] = df_plp_aacorp.loc[:, "Linea de Negocio"]
            df_sabana_aacorp.loc[:, "OPCION_ESTRATEGICA"] = df_plp_aacorp.loc[:, "Opción Estratégica"]
            df_sabana_aacorp.loc[:, "MEGA_RETO"] = df_plp_aacorp.loc[:, "Mega Reto"]
            df_sabana_aacorp.loc[:, "EMPRESA"] = df_plp_aacorp.loc[:, "Empresa"]
            df_sabana_aacorp.loc[:, "SEGMENTO"] = df_plp_aacorp.loc[:, "Segmento"]
            df_sabana_aacorp.loc[:, "SEGMENTO_IMPACTADO"] = df_plp_aacorp.loc[:, "Segmento Impactado"]
            df_sabana_aacorp.loc[:, "UN_NEG_REGIONAL"] = df_plp_aacorp.loc[:, "Un. Neg./Regional"]
            df_sabana_aacorp.loc[:, "GERENCIA"] = df_plp_aacorp.loc[:, "Gerencia"]
            df_sabana_aacorp.loc[:, "ACTIVO"] = df_plp_aacorp.loc[:, "Activo"]
            df_sabana_aacorp.loc[:, "DEPARTAMENTO"] = df_plp_aacorp.loc[:, "Departamento"]
            df_sabana_aacorp.loc[:, "TIPO_DE_INVERSION"] = df_plp_aacorp.loc[:, "Tipo de inversión"]
            df_sabana_aacorp.loc[:, "NOMBRE_PROYECTO"] = df_plp_aacorp.loc[:, "Nombre Proyecto"]
            df_sabana_aacorp.loc[:, "WI"] = df_plp_aacorp.loc[:, "WI"]
            df_sabana_aacorp.loc[:, "FASE_EN_CURSO"] = df_plp_aacorp.loc[:, "Fase en curso"]
            df_sabana_aacorp.loc[:, "CAPEX_MENOR_ANIO"] = df_plp_aacorp.loc[:, "CapEx < 23"].fillna(0)
            df_sabana_aacorp.loc[:, "CAPEX_MAYOR_ANIO"] = df_plp_aacorp.loc[:, "CapEx > 40"].fillna(0) 
            df_sabana_aacorp.loc[:, "CAPEX_TOTAL"] = df_plp_aacorp.loc[:, "CapEx Total"].fillna(0)
            df_sabana_aacorp.loc[:, "VPN"] = df_plp_aacorp.loc[:, "VPN"].fillna(0)
            df_sabana_aacorp.loc[:, "EVPN"] =df_plp_aacorp.loc[:, "(E) VPN"].fillna(0)
            df_sabana_aacorp.loc[:, "VPI"] =df_plp_aacorp.loc[:, "VPI"].fillna(0)
            df_sabana_aacorp.loc[:, "EFI"] =df_plp_aacorp.loc[:, "EFI"].fillna(0)  
            df_sabana_aacorp.loc[:, "TIR"] =df_plp_aacorp.loc[:, "TIR"].fillna(0) 
            df_sabana_aacorp.loc[:, "PAY_BACK"] =df_plp_aacorp.loc[:, "Pay Back"].fillna(0)
            df_sabana_aacorp.loc[:, "APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] = df_plp_aacorp.loc[:, "Aporte EBITDA Acum 2040 proyectos asociados a CT+i"]
            df_sabana_aacorp.loc[:, "OPEX_UNITARIO"] = df_plp_aacorp.loc[:, "OpEx Unitario"]
            df_sabana_aacorp.loc[:, "CATEGORIA_TESG_PALANCAS"] = df_plp_aacorp.loc[:, "Categoria TESG (Palancas)"]


            df_sabana_aacorp["LINEA_DE_NEGOCIO"] = df_sabana_aacorp["LINEA_DE_NEGOCIO"].str.strip()

            #-----------------------------------
            

            # Aplicar la función a la columna 'columna_sin_asignar' y asignar el resultado a una nueva columna
            df_sabana_aacorp['MATRICULA_DIGITAL'] = df_sabana_aacorp.apply(asignar_valor_aacorp, axis=1)

            #-------------------------------

            df_sabana_aacorp.insert(0, 'ID_PROYECTO', range(1, len(df_sabana_aacorp) + 1))

            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1


            # la sabana esta vacia pendiente activar y poner validadcion de error si esta vaxia 
            df_sabana_aacorp = pd.concat(df_sabana_aacorp.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True) 
            

            #-----------------------------------------------CapEx
            #CapEx 
            columnas_agregar_capex = ['CapEx 24', 'CapEx 25',
                                        'CapEx 26', 'CapEx 27',
                                        'CapEx 28', 'CapEx 29',
                                        'CapEx 30', 'CapEx 31',
                                        'CapEx 32' , 'CapEx 33',
                                        'CapEx 34' , 'CapEx 35',
                                        'CapEx 36' , 'CapEx 37',
                                        'CapEx 38' , 'CapEx 39',
                                        'CapEx 40']
            # columnas_agregar_capex = df_plp_aacorp.columns[df_plp_aacorp.columns.str.match("CapEx \d+")]
            nombre_columna_capex = 'CAPEX'
            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp,columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin)
            df_sabana_aacorp["CAPEX"] = df_sabana_aacorp["CAPEX"].fillna(0)


            #-----------------------------EBITA
            #EBITDA
            ebitda = [
                'EBITDA Total 24', 'EBITDA Total 25', 'EBITDA Total 26', 'EBITDA Total 27',
                'EBITDA Total 28', 'EBITDA Total 29', 'EBITDA Total 30', 'EBITDA Total 31', 
                'EBITDA Total 32', 'EBITDA Total 33', 'EBITDA Total 34', 'EBITDA Total 35', 
                'EBITDA Total 36', 'EBITDA Total 37', 'EBITDA Total 38', 'EBITDA Total 39',
                'EBITDA Total 40'
            ]

            nueva_columna_ebitda = 'EBITDA'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, ebitda, nueva_columna_ebitda, annio_inicio, annio_fin)
            df_sabana_aacorp["EBITDA"] = df_sabana_aacorp["EBITDA"].fillna(0)
            df_sabana_aacorp['EBITDA'] = pd.to_numeric(df_sabana_aacorp['EBITDA'], errors='coerce')


            #-----------------------------EMISONES NETAS
            #Emisiones Netas Co2_Alcance 1 y 2
            columnas_emisiones_netas_co2_alcance_1_y_2 = [
                'Emisiones Netas Co2 Alcance 1 y 2 2024',
                'Emisiones Netas Co2 Alcance 1 y 2 2025', 'Emisiones Netas Co2 Alcance 1 y 2 2026',
                'Emisiones Netas Co2 Alcance 1 y 2 2027', 'Emisiones Netas Co2 Alcance 1 y 2 2028',
                'Emisiones Netas Co2 Alcance 1 y 2 2029', 'Emisiones Netas Co2 Alcance 1 y 2 2030',
                'Emisiones Netas Co2 Alcance 1 y 2 2031', 'Emisiones Netas Co2 Alcance 1 y 2 2032',
                'Emisiones Netas Co2 Alcance 1 y 2 2033', 'Emisiones Netas Co2 Alcance 1 y 2 2034',
                'Emisiones Netas Co2 Alcance 1 y 2 2035', 'Emisiones Netas Co2 Alcance 1 y 2 2036',
                'Emisiones Netas Co2 Alcance 1 y 2 2037', 'Emisiones Netas Co2 Alcance 1 y 2 2038',
                'Emisiones Netas Co2 Alcance 1 y 2 2039', 'Emisiones Netas Co2 Alcance 1 y 2 2040'
            ]

            nueva_columna_emisiones_netas_co2_alcance_1_y_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, columnas_emisiones_netas_co2_alcance_1_y_2, nueva_columna_emisiones_netas_co2_alcance_1_y_2, annio_inicio, annio_fin)
            df_sabana_aacorp["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_sabana_aacorp["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)


            #-----------------------------Empleos NP
            #Empleon NP
            columnas_Empleon_NP = [
                'Empleos NP 2024', 'Empleos NP 2025', 'Empleos NP 2026',
                'Empleos NP 2027', 'Empleos NP 2028', 'Empleos NP 2029', 'Empleos NP 2030',
                'Empleos NP 2031', 'Empleos NP 2032', 'Empleos NP 2033', 'Empleos NP 2034',
                'Empleos NP 2035', 'Empleos NP 2036', 'Empleos NP 2037', 'Empleos NP 2038',
                'Empleos NP 2039', 'Empleos NP 2040'
            ]


            nueva_columna_Empleon_np = 'EMPLEOS_NP'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, columnas_Empleon_NP, nueva_columna_Empleon_np, annio_inicio, annio_fin)
            df_sabana_aacorp["EMPLEOS_NP"] = df_sabana_aacorp["EMPLEOS_NP"].fillna(0)


            #-------------------------------------agua potable
            #agua potable
            columnas_acceso_agua_potable = [
                'Acceso agua potable 2024', 'Acceso agua potable 2025',
                'Acceso agua potable 2026', 'Acceso agua potable 2027', 'Acceso agua potable 2028',
                'Acceso agua potable 2029', 'Acceso agua potable 2030', 'Acceso agua potable 2031',
                'Acceso agua potable 2032', 'Acceso agua potable 2033', 'Acceso agua potable 2034',
                'Acceso agua potable 2035', 'Acceso agua potable 2036', 'Acceso agua potable 2037',
                'Acceso agua potable 2038', 'Acceso agua potable 2039', 'Acceso agua potable 2040'
            ]

            nueva_columna_acceso_agua_potable = 'ACCESO_AGUA_POTABLE'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, columnas_acceso_agua_potable, nueva_columna_acceso_agua_potable, annio_inicio, annio_fin)
            df_sabana_aacorp["ACCESO_AGUA_POTABLE"] = df_sabana_aacorp["ACCESO_AGUA_POTABLE"].fillna(0)

            #---------------------------------ACCESO_GAS
            columnas_acceso_gas = [
                'Acceso Gas 2024',
                'Acceso Gas 2025', 'Acceso Gas 2026',
                'Acceso Gas 2027', 'Acceso Gas 2028',
                'Acceso Gas 2029', 'Acceso Gas 2030',
                'Acceso Gas 2031', 'Acceso Gas 2032',
                'Acceso Gas 2033', 'Acceso Gas 2034',
                'Acceso Gas 2035', 'Acceso Gas 2036',
                'Acceso Gas 2037', 'Acceso Gas 2038',
                'Acceso Gas 2039', 'Acceso Gas 2040'
            ]

            nueva_columna_acceso_gas = 'ACCESO_GAS'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, columnas_acceso_gas, nueva_columna_acceso_gas, annio_inicio, annio_fin)
            df_sabana_aacorp["ACCESO_GAS"] = df_sabana_aacorp["ACCESO_GAS"].fillna(0)


            #-----------------------------kwh
            #columnas_kwh
            columnas_kwh = [
                'KWh 2024',
                'KWh 2025', 'KWh 2026',
                'KWh 2027', 'KWh 2028',
                'KWh 2029', 'KWh 2030',
                'KWh 2031', 'KWh 2032',
                'KWh 2033', 'KWh 2034',
                'KWh 2035', 'KWh 2036',
                'KWh 2037', 'KWh 2038',
                'KWh 2039', 'KWh 2040'
            ]

            nueva_columnas_kwh = 'KWH'

            df_sabana_aacorp = plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, columnas_kwh, nueva_columnas_kwh, annio_inicio, annio_fin)
            df_sabana_aacorp["KWH"] = df_sabana_aacorp["KWH"].fillna(0)

            df_sabana_aacorp = df_sabana_aacorp.loc[:, ~df_sabana_aacorp.columns.duplicated()]

            #_-----------------------------------------------------------
            # Adiciono las columnas precio_crudo,  
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo =  self.flujo_caja_config["parametros"]["precio_crudo"]
            impuesto_renta =  self.flujo_caja_config["parametros"]["impuesto_renta"]
            payout =  self.flujo_caja_config["parametros"]["payout"] 
            precio_co2 =  self.flujo_caja_config["parametros"]["precio_co2"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios= list(range(annio_inicio, annio_fin))

            # Asignacion precio crudo por año 
            data = {'RANGO_ANIO': anios , 
                    'PRECIO_CRUDO_BRENT': precio_crudo, 
                    'IMPUESTO_RENTA': impuesto_renta, 
                    'PAYOUT':payout,     
                    'PRECIO_CO2':precio_co2        
                    }

            df_anio_crudo= pd.DataFrame(data)

            df_sabana_aacorp = df_sabana_aacorp.merge(df_anio_crudo, on='RANGO_ANIO', how='left')

            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]

            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anios = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Replicar registros de df1 para que coincida con la longitud de df2
            replicated_df_sobretasa = pd.concat([df_sobretasa] * (len(df_sabana_aacorp) // len(df_sobretasa)), ignore_index=True)

            # Asegurarse de que la longitud de replicated_df_sobretasa coincida con la longitud de df2
            replicated_df_sobretasa = replicated_df_sobretasa.iloc[:len(df_sabana_aacorp)]

            df_sabana_aacorp['SOBRETASA'] = replicated_df_sobretasa['SOBRETASA']

            #se divide por segmentos
            df_sabana_aacorp_be = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Bajas Emisiones']
            df_sabana_aacorp_down = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Downstream']
            df_sabana_aacorp_up_condi1 = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Upstream']
            df_sabana_aacorp_isa = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Transmisión y Vias']
            df_sabana_aacorp_coorporativo_condi2 = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Corporativo'] 
            df_sabana_aacorp_mid = df_sabana_aacorp[df_sabana_aacorp['SEGMENTO_IMPACTADO'] =='Midstream'] 
            df_sabana_aacorp_up = pd.concat([df_sabana_aacorp_up_condi1, df_sabana_aacorp_coorporativo_condi2],ignore_index=True)

                
            #Flujo Caja Upstream 

            if not df_sabana_aacorp_up.empty:
                
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_aacorp_up['ID_PROYECTO'].unique()})

                cantidad_anios_depreciacion_hidrocarburos = self.flujo_caja_config["parametros"]["cantidad_anios_depreciacion_hidrocarburos"] 

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    
                    ID_PROYECTO = row['ID_PROYECTO']  
                        
                    columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                    columna_filtro_valor = ID_PROYECTO

                    df_data = df_sabana_aacorp_up[df_sabana_aacorp_up['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                    df_depreciacion = calcular_depreciacion_up(df_data)
                    
                    merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df1)
                    
                    df_total = df_total.append(df_con_ppe, ignore_index=True)    

                df_sabana_aacorp_up = pd.merge(df_sabana_aacorp_up, df_total, how='inner')

                # INGRESOS  
                df_sabana_aacorp_up['TOTAL_INGRESOS'] = 0

                df_sabana_aacorp_up['COSTO_INTERNO_CO2'] = df_sabana_aacorp_up['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_aacorp_up['PRECIO_CO2']

                df_sabana_aacorp_up['TOTAL_COSTOS'] = df_sabana_aacorp_up['COSTO_INTERNO_CO2']
                        
                df_sabana_aacorp_up['BASE_GRABABLE'] = df_sabana_aacorp_up['EBITDA'] + df_sabana_aacorp_up['COSTO_INTERNO_CO2'] - df_sabana_aacorp_up['SUM_DEPRE']

                # G	IMPUESTOS	F * 35% + Sobre tasa --
                df_sabana_aacorp_up['IMPUESTOS'] =  df_sabana_aacorp_up['BASE_GRABABLE'] * (df_sabana_aacorp_up['IMPUESTO_RENTA']  + df_sabana_aacorp_up['SOBRETASA']) 

                # capex	 df_sabana_datos['CAPEX']
                # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
                df_sabana_aacorp_up['FLUJO_CAJA']= df_sabana_aacorp_up['EBITDA']  - df_sabana_aacorp_up['IMPUESTOS'] - df_sabana_aacorp_up['CAPEX']

                df_sabana_aacorp_up['EBIT'] =  df_sabana_aacorp_up['EBITDA'] - df_sabana_aacorp_up['IMPUESTOS']
                
                # Dividendo	 utilidad * payout
                df_sabana_aacorp_up['DIVIDENDO'] = df_sabana_aacorp_up['EBIT'] * (df_sabana_aacorp_up['PAYOUT'])

                df_sabana_aacorp_up["CAPITAL_EMPLEADO"] = df_sabana_aacorp_up["CAPITAL_EMPLEADO"].fillna(0) 
                df_sabana_aacorp_up['EBIT'] = df_sabana_aacorp_up["EBIT"].fillna(0)

                # df_sabana_aacorp_up['ROACE'] = (df_sabana_aacorp_up['EBIT'] / df_sabana_aacorp_up['CAPITAL_EMPLEADO'])
                # df_sabana_aacorp_up['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_sabana_aacorp_up['ROACE'] = df_sabana_aacorp_up["ROACE"].fillna(0) 
            else: 
                    logger.info(f"Dataframe vacio (df_sabana_aacorp_up) para los proyectos Upstream en segmento Aacorp")
            
            #Flujo Caja downstream 
            # INGRESOS  
            #Calcular depreciacion down
            if not df_sabana_aacorp_down.empty:      
                    df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_aacorp_down['ID_PROYECTO'].unique()})

                    cantidad_anios_depreciacion_down = self.flujo_caja_config["parametros"]["cantidad_anios_depreciacion_down"] 

                    df_total = pd.DataFrame()

                    for index, row in df_id_proyectos.iterrows():
                        
                        ID_PROYECTO = row['ID_PROYECTO']  

                        columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                        columna_filtro_valor = ID_PROYECTO

                        df_data = df_sabana_aacorp_down[df_sabana_aacorp_down['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                        df_depreciacion = calcular_depreciacion_down(df_data)
                        
                        merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                        #calcular capital de empleado
                        df_con_ppe = calcular_ppe(merged_df1)
                        
                        df_total = df_total.append(df_con_ppe, ignore_index=True)   

                    df_sabana_aacorp_down = pd.merge(df_sabana_aacorp_down, df_total, how='inner')

                    df_sabana_aacorp_down['TOTAL_INGRESOS'] = 0

                    df_sabana_aacorp_down['COSTO_INTERNO_CO2'] = df_sabana_aacorp_down['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_aacorp_down['PRECIO_CO2']

                    df_sabana_aacorp_down['TOTAL_COSTOS'] = df_sabana_aacorp_down['COSTO_INTERNO_CO2']		

                    df_sabana_aacorp_down['BASE_GRABABLE'] = df_sabana_aacorp_down['EBITDA']  + df_sabana_aacorp_down ['COSTO_INTERNO_CO2']  - df_sabana_aacorp_down['SUM_DEPRE']



                    # Aplicamos la función 'calcular_impuestos' a cada fila del DataFrame y almacenamos los resultados en una nueva columna 'Impuestos'
                    df_sabana_aacorp_down['IMPUESTOS'] = df_sabana_aacorp_down.apply(calcular_impuestos_icos, axis=1)

                    # capex	 df_sabana_datos['CAPEX']
                    # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
                    df_sabana_aacorp_down['FLUJO_CAJA']= df_sabana_aacorp_down['EBITDA']  - df_sabana_aacorp_down['IMPUESTOS'] - df_sabana_aacorp_down['CAPEX']

                    df_sabana_aacorp_down['EBIT'] =  df_sabana_aacorp_down['EBITDA'] - df_sabana_aacorp_down['IMPUESTOS']

                    # Dividendo	 utilidad * payout
                    df_sabana_aacorp_down['DIVIDENDO'] = df_sabana_aacorp_down['EBIT'] * (df_sabana_aacorp_down['PAYOUT'])

                    df_sabana_aacorp_down["CAPITAL_EMPLEADO"] = df_sabana_aacorp_down["CAPITAL_EMPLEADO"].fillna(0) 
                    df_sabana_aacorp_down['EBIT'] = df_sabana_aacorp_down["EBIT"].fillna(0)

                    # df_sabana_aacorp_down['ROACE'] = (df_sabana_aacorp_down['EBIT'] / df_sabana_aacorp_down['CAPITAL_EMPLEADO']) 
                    # df_sabana_aacorp_down['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                    # df_sabana_aacorp_down['ROACE'] = df_sabana_aacorp_down["ROACE"].fillna(0)
            else: 
                    logger.info(f"Dataframe vacio (df_sabana_aacorp_down) para los proyectos downstream en segmento Aacorp")

            #Flujo Caja BE 

            #------------------------------------------------------
            #deperciacion Bajas emeisiones 

            if not df_sabana_aacorp_be.empty:  
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_aacorp_be['ID_PROYECTO'].unique()})

                porcentaje_depreciacion_anio = self.flujo_caja_config["parametros"]["porcentaje_depreciacion_anio"] 
                porcentaje_deduccion_especial= self.flujo_caja_config["parametros"]["porcentaje_deduccion_especial"]

                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']   
                    
                    columnas_deseadas = ['MATRICULA_DIGITAL','ID_PROYECTO','RANGO_ANIO','CAPEX','EBITDA']
                    columna_filtro_valor = ID_PROYECTO

                    df_data = df_sabana_aacorp_be[df_sabana_aacorp_be['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                    primer_anio_produccion = df_data.loc[df_data['EBITDA'] > 0, 'RANGO_ANIO'].min()
                    df_data_seleccionadas =  df_data[df_data['RANGO_ANIO'] == primer_anio_produccion]
                    df_data_seleccionadas['ANIO_PRODUCCION']= primer_anio_produccion   
                    df_data_seleccionadas['SUMA_CAPEX'] = df_data['CAPEX'].sum() 

                    if df_data_seleccionadas.empty:  
                        data = {'ID_PROYECTO': ID_PROYECTO,
                                'RANGO_ANIO': list(range(annio_inicio, annio_fin)),
                                'SUM_DEPRE': 0,
                                'CAPEX_TOTAL_ANIO': 0}
                        df = pd.DataFrame(data)    
                    else:
                        df= calcular_depreciacion_be(df_data_seleccionadas, porcentaje_depreciacion_anio, annio_inicio, annio_fin )
                    
                    merged_df = pd.merge(df_data, df,on=['ID_PROYECTO', 'RANGO_ANIO'], how='inner')
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
                    
                    merged_df['SUM_DEPRE'] = merged_df['SUM_DEPRE'].astype(float)

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df)  

                    df_total = df_total.append(df_con_ppe, ignore_index=True)   
                    
                df_sabana_aacorp_be = pd.merge(df_sabana_aacorp_be, df_total, how='inner')

                # INGRESOS  
                df_sabana_aacorp_be['TOTAL_INGRESOS'] = 0

                df_sabana_aacorp_be['COSTO_INTERNO_CO2'] = df_sabana_aacorp_be['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_aacorp_be['PRECIO_CO2']

                df_sabana_aacorp_be['TOTAL_COSTOS'] = df_sabana_aacorp_be['COSTO_INTERNO_CO2']
                        
                # df_sabana_aacorp_be['EBITDA']  = df_sabana_aacorp_be['TOTAL_INGRESOS'] - df_sabana_aacorp_be['TOTAL_COSTOS']  # EBITDA

                df_sabana_aacorp_be['BASE_GRABABLE'] = df_sabana_aacorp_be['EBITDA'] + df_sabana_aacorp_be['COSTO_INTERNO_CO2'] - df_sabana_aacorp_be['SUM_DEPRE']

                # G	IMPUESTOS	F * 35% + Sobre tasa --
                df_sabana_aacorp_be['IMPUESTOS'] =  df_sabana_aacorp_be['BASE_GRABABLE'] * (df_sabana_aacorp_be['IMPUESTO_RENTA']  + df_sabana_aacorp_be['SOBRETASA']) 


                porcentaje_deduccion_especial = self.flujo_caja_config["parametros"]["porcentaje_deduccion_especial"] 
                #CAPEX PRIMER AÑO * 50 % (35%+ SOBRETASA) porcentaje_deduccion_especial 
                df_sabana_aacorp_be['DEDUCCION_ESPECIAL'] =  df_sabana_aacorp_be['CAPEX_TOTAL_ANIO']  *  porcentaje_deduccion_especial * (df_sabana_aacorp_be['IMPUESTO_RENTA']  + df_sabana_aacorp_be['SOBRETASA'])

                # FLUJO DE CAJA --> H–K+L-M
                df_sabana_aacorp_be['FLUJO_CAJA'] = df_sabana_aacorp_be['EBITDA'] - df_sabana_aacorp_be['IMPUESTOS']  + df_sabana_aacorp_be['DEDUCCION_ESPECIAL'] - df_sabana_aacorp_be['CAPEX']


                df_sabana_aacorp_be['EBIT'] =  df_sabana_aacorp_be['EBITDA'] - df_sabana_aacorp_be['IMPUESTOS']
                
                # Dividendo	 utilidad * payout
                df_sabana_aacorp_be['DIVIDENDO'] = df_sabana_aacorp_be['EBIT'] * (df_sabana_aacorp_be['PAYOUT'])

                df_sabana_aacorp_be["CAPITAL_EMPLEADO"] = df_sabana_aacorp_be["CAPITAL_EMPLEADO"].fillna(0) 
                df_sabana_aacorp_be['EBIT'] = df_sabana_aacorp_be["EBIT"].fillna(0)

                # df_sabana_aacorp_be['ROACE'] = (df_sabana_aacorp_be['EBIT'] / df_sabana_aacorp_be['CAPITAL_EMPLEADO'])
                # df_sabana_aacorp_be['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_sabana_aacorp_be['ROACE'] = df_sabana_aacorp_be["ROACE"].fillna(0) 
            else: 
                    logger.info(f"Dataframe vacio (df_sabana_aacorp_be) para los proyectos Bajas Emisiones en segmento Aacorp")

                
            #------------------------------------------------------
            #Flujo caja Midstream

            #----------------------------------------------
            #Calcular depreciacion 

            if not df_sabana_aacorp_mid.empty:
                
                df_id_proyectos = pd.DataFrame({'ID_PROYECTO': df_sabana_aacorp_mid['ID_PROYECTO'].unique()})
                
                df_total = pd.DataFrame()

                for index, row in df_id_proyectos.iterrows():
                    ID_PROYECTO = row['ID_PROYECTO']  
                    
                    columnas_deseadas = ['ID_PROYECTO','RANGO_ANIO','CAPEX']
                    columna_filtro_valor = ID_PROYECTO

                    df_data = df_sabana_aacorp_mid[df_sabana_aacorp_mid['ID_PROYECTO'] == columna_filtro_valor][columnas_deseadas]

                    df_depreciacion = calcular_depreciacion_mid(df_data)
                    
                    merged_df1 = pd.merge(df_data, df_depreciacion, on='RANGO_ANIO', how='inner')

                    #calcular capital de empleado
                    df_con_ppe = calcular_ppe(merged_df1)
                    
                    df_total = df_total.append(df_con_ppe, ignore_index=True)   

                df_sabana_aacorp_mid = pd.merge(df_sabana_aacorp_mid, df_total, how='inner')     


                # INGRESOS  
                df_sabana_aacorp_mid['TOTAL_INGRESOS'] = 0

                df_sabana_aacorp_mid['COSTO_INTERNO_CO2'] = df_sabana_aacorp_mid['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_sabana_aacorp_mid['PRECIO_CO2']

                df_sabana_aacorp_mid['TOTAL_COSTOS'] = df_sabana_aacorp_mid['COSTO_INTERNO_CO2']				

                df_sabana_aacorp_mid['BASE_GRABABLE'] = df_sabana_aacorp_mid['EBITDA'] + df_sabana_aacorp_mid['COSTO_INTERNO_CO2'] - df_sabana_aacorp_mid['SUM_DEPRE']

                df_sabana_aacorp_mid['IMPUESTOS'] = df_sabana_aacorp_mid['BASE_GRABABLE'] * df_sabana_aacorp_mid['IMPUESTO_RENTA']

                # capex	 df_sabana_datos['CAPEX']
                # Flujo de caja	 EBITDA - IMPUESTO - CAPEX 
                df_sabana_aacorp_mid['FLUJO_CAJA']= df_sabana_aacorp_mid['EBITDA']  - df_sabana_aacorp_mid['IMPUESTOS'] - df_sabana_aacorp_mid['CAPEX']
                
                df_sabana_aacorp_mid['EBIT'] =  df_sabana_aacorp_mid['EBITDA'] - df_sabana_aacorp_mid['IMPUESTOS']
                
                # Dividendo	 utilidad * payout
                df_sabana_aacorp_mid['DIVIDENDO'] = df_sabana_aacorp_mid['EBIT'] * (df_sabana_aacorp_mid['PAYOUT'])

                df_sabana_aacorp_mid["CAPITAL_EMPLEADO"] = df_sabana_aacorp_mid["CAPITAL_EMPLEADO"].fillna(0) 
                df_sabana_aacorp_mid['EBIT'] = df_sabana_aacorp_mid["EBIT"].fillna(0)

                # df_sabana_aacorp_mid['ROACE'] = (df_sabana_aacorp_mid['EBIT'] / df_sabana_aacorp_mid['CAPITAL_EMPLEADO']) 
                # df_sabana_aacorp_mid['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
                # df_sabana_aacorp_mid['ROACE'] = df_sabana_aacorp_mid["ROACE"].fillna(0)
            else: 
                    logger.info(f"Dataframe vacio (df_sabana_aacorp_mid) para los proyectos Midstream en segmento Aacorp")
            
            df_flujo_caja_aacorp = pd.concat([df_sabana_aacorp_up, df_sabana_aacorp_be, df_sabana_aacorp_down, df_sabana_aacorp_mid], axis=0)
            # # Aplicar la función calcular_duracion_proyectos al grupo 'ID_PROYECTO' en df_sabana_aacorp
            df_flujo_caja_aacorp = df_flujo_caja_aacorp.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)

        else:
            logger.info(f"El DataFrame aacorp (df_plp_aacorp) está vacío.") 
       
        df_union = pd.concat([df_flujo_caja_up, df_sabana_emisiones, df_sabana_transmision, df_sabana_downstream_total,
                              df_sabana_midstream, df_flujo_caja_aacorp], axis=0)

        return df_union