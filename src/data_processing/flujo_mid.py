import pandas as pd
import numpy as np

from data_processing.flujo_caja_utils import (
    expand_row,
    procesar_grupo_mid,
    calcular_duracion_proyectos,
    plpl_add_new_column_in_activo_mid
)
from utils.logger import Logger

logger = Logger(__name__).get_logger()


class FlujoMid:

    def __init__(self, input_results, flujo_caja_config, api_config):
        self.flujo_caja_config = flujo_caja_config

        # PLP - Hoja Midstream
        file_path_up = flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_midstream_activo = flujo_caja_config["fuente"]["plp"]["upstream_config"]["activo_midstream"]
        self.df_plp_activo_midstream = pd.read_excel(file_path_up, sheet_name=plp_midstream_activo, header=2, engine='openpyxl')

        # ---------------------------------------------------
        # portafolio
        file_path_portafolio = flujo_caja_config["fuente"]["portafolios"]["dbfs_path_portafolios"]
        portafolio = flujo_caja_config["fuente"]["portafolios"]["portafolios_config"]["sheet_name1"]
        self.df_portafolio = pd.read_excel(file_path_portafolio, sheet_name=portafolio, header=0, engine='openpyxl')

        # ---------------------------------------------------
        # flujo caja midstream
        file_path_mid = flujo_caja_config["fuente"]["midstream_proyectos"]["dbfs_path_midstream"]
        midstream = flujo_caja_config["fuente"]["midstream_proyectos"]["midstream_config"]["sheet_name1"]
        df_midstream_proyectos = pd.read_excel(file_path_mid, sheet_name=midstream, header=0, engine='openpyxl')
        self.df_midstream_proyectos = df_midstream_proyectos.dropna(axis=0, how='all')

        self.df_portafolio_results = input_results

        val_curva_crudo = str(api_config['val_curva_crudo'])
        val_curva_crudo = val_curva_crudo[0]
        self.flujo_caja_config["parametros"]["precio_crudo"] = [i * api_config["val_sensi_precio_crudo"] for i in
                                                self.flujo_caja_config["parametros"]
                                                [f"precio_crudo_{val_curva_crudo}"]]

        self.flujo_caja_config["parametros"]["precio_co2"] = [i * api_config["val_sensi_precio_co2"] for i in
                                              self.flujo_caja_config["parametros"]["precio_co2"]]

    def calculate_mid(self, corrida_fc_portafolio):
        df_plp_activo_midstream = self.df_plp_activo_midstream
        df_midstream_activo = pd.DataFrame()
        if not df_plp_activo_midstream.empty:

            # Copiar datos de otras columnas al DataFrame

            df_midstream_activo.loc[:, "NEGOCIO"] = df_plp_activo_midstream["Negocio"]
            df_midstream_activo.loc[:, "LINEA_DE_NEGOCIO"] = df_plp_activo_midstream["Linea de Negocio"]
            df_midstream_activo.loc[:, "OPCION_ESTRATEGICA"] = df_plp_activo_midstream["Opción Estratégica"]
            df_midstream_activo.loc[:, "EMPRESA"] = df_plp_activo_midstream["Empresa"]
            df_midstream_activo.loc[:, "ACTIVO"] = df_plp_activo_midstream["Activo"]
            df_midstream_activo.loc[:, "TIPO_DE_ACTIVO"] = df_plp_activo_midstream["Tipo de activo"]

            df_midstream_activo.loc[:, "POTENCIAL_DESARROLLO"] = df_plp_activo_midstream["Potencial desarrollo"]
            df_midstream_activo.loc[:, "EBITDA_BARRIL"] = df_plp_activo_midstream["EBITDA/ Barril"]
            df_midstream_activo.loc[:, "CAPTURA_VALOR"] = df_plp_activo_midstream["Captura valor"]
            df_midstream_activo.loc[:, "VPN"] = df_plp_activo_midstream["VPN"]
            df_midstream_activo.loc[:, "VPN_CON_CO2"] = df_plp_activo_midstream["VPN con Co2"]
            df_midstream_activo.loc[:, "VPN_BARRIL"] = df_plp_activo_midstream["VPN /Barril"]
            df_midstream_activo.loc[:, "VOL_TRANSP_ANIO_CURSO"] = df_plp_activo_midstream["Vol. transp. Año en curso"]
            df_midstream_activo.loc[:, "COSTO_UNITARIO"] = df_plp_activo_midstream["Costo Unitario"] 
            df_midstream_activo.loc[:, "INTENSIDAD_EMISIONES"] = df_plp_activo_midstream["Intensidad emisiones"] 
            df_midstream_activo.loc[:, "TIPO_DE_INVERSION"] =  'Básica'
            df_midstream_activo.insert(1, 'NOMBRE_PROYECTO', 'Operacion ' + df_midstream_activo['ACTIVO'] )  
            df_midstream_activo.insert(2, 'SEGMENTO', 'Midstream')  
            df_midstream_activo.insert(0, 'MATRICULA_DIGITAL', range(1, len(df_midstream_activo) + 1))
            df_midstream_activo['MATRICULA_DIGITAL'] = df_midstream_activo['MATRICULA_DIGITAL'].apply(lambda x: 'MID' + str(x))

            # Obtener el rango de años de la configuración
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1

            # Aplicar la función expand_row a cada fila del DataFrame y concatenar los resultados
            df_midstream_activo = pd.concat(df_midstream_activo.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)


            #-----------------------------------------------------------------
            #-----------------------------EBITDA
            #2
            columnas_ebitda = ['EBITDA 2024', 'EBITDA 2025', 
                            'EBITDA 2026', 'EBITDA 2027', 'EBITDA 2028',
                            'EBITDA 2029', 'EBITDA 2030', 'EBITDA 2031', 
                            'EBITDA 2032', 'EBITDA 2033', 'EBITDA 2034', 
                            'EBITDA 2035', 'EBITDA 2036', 'EBITDA 2037', 
                            'EBITDA 2038', 'EBITDA 2039', 'EBITDA 2040'
                            ]
                

            nueva_columna_ebitda = 'EBITDA'

            df_midstream_activo = plpl_add_new_column_in_activo_mid(df_plp_activo_midstream, df_midstream_activo, columnas_ebitda, nueva_columna_ebitda, annio_inicio, annio_fin)
            df_midstream_activo["EBITDA"] = df_midstream_activo["EBITDA"].fillna(0)

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

            df_midstream_activo = plpl_add_new_column_in_activo_mid(df_plp_activo_midstream, df_midstream_activo,columnas_emisiones_netas_co2_alcance_1_y_2,
                                                                    nueva_columna_emisiones_netas_co2_alcance_1_y_2, 
                                                                    annio_inicio, annio_fin)
            df_midstream_activo["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_midstream_activo["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)


            df_midstream_activo = df_midstream_activo.loc[:, ~df_midstream_activo.columns.duplicated()]

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

            df_midstream_activo = df_midstream_activo.merge(df_anio_crudo, on='RANGO_ANIO', how='left')

            porfafolio = 'PORTAFOLIO_CASO_BASE'
            df_midstream_activo['NOMBRE_PORTAFOLIO'] = porfafolio

            df_midstream_activo['CAPEX'] = 0
            df_midstream_activo['TOTAL_INGRESOS'] = 0 
            df_midstream_activo['TOTAL_COSTOS'] = 0 
            #Flujo Caja 
            df_midstream_activo['COSTO_INTERNO_CO2'] = df_midstream_activo['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_midstream_activo['PRECIO_CO2']    

            df_grupo_proyectos = df_midstream_activo[['ID_PROYECTO','MATRICULA_DIGITAL','RANGO_ANIO','CAPEX']]

            # Aplicar procesar_grupo_mid a cada grupo de ID_PROYECTO
            grupos_procesados = [procesar_grupo_mid(grupo) for _, grupo in df_grupo_proyectos.groupby('ID_PROYECTO')]

            # # Concatenar los resultados
            df_total = pd.concat(grupos_procesados, ignore_index=True)

            df_midstream_activo = pd.concat([df_midstream_activo, df_total], axis='columns')
            df_midstream_activo = df_midstream_activo.loc[:, ~df_midstream_activo.columns.duplicated()]

            #J Base grabable --> H-I 
            df_midstream_activo['BASE_GRABABLE'] = df_midstream_activo['EBITDA'] + df_midstream_activo['COSTO_INTERNO_CO2'] - df_midstream_activo['SUM_DEPRE']

            #K IMPUESTOS J* 35% *
            df_midstream_activo['IMPUESTOS'] = df_midstream_activo['BASE_GRABABLE'] * (df_midstream_activo['IMPUESTO_RENTA'] ) 


            # FLUJO DE CAJA --> H–K+L-M
            df_midstream_activo['FLUJO_CAJA'] = df_midstream_activo['EBITDA'] - df_midstream_activo['IMPUESTOS']  - df_midstream_activo['CAPEX']

            #utilidad
            df_midstream_activo['EBIT'] =  df_midstream_activo['EBITDA']  -  df_midstream_activo['IMPUESTOS']

            #dividendo
            df_midstream_activo['DIVIDENDO'] =  df_midstream_activo['EBIT']  * df_midstream_activo['PAYOUT']
            df_midstream_activo["CAPITAL_EMPLEADO"] = df_midstream_activo["CAPITAL_EMPLEADO"].fillna(0) 
            df_midstream_activo['EBIT'] = df_midstream_activo["EBIT"].fillna(0) 


            # df_midstream_activo['ROACE'] = (df_midstream_activo['EBIT'] / df_midstream_activo['CAPITAL_EMPLEADO']) 
            # df_midstream_activo['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0

            df_midstream_activo.insert(0, 'PORTAFOLIO', porfafolio)         

            df_midstream_activo = df_midstream_activo.groupby('ID_PROYECTO').apply(calcular_duracion_proyectos).reset_index(drop=True)  

        else:
            Logger.info("El DataFrame midstream activo (df_plp_activo_midstream) está vacío.")
        
        return df_midstream_activo

