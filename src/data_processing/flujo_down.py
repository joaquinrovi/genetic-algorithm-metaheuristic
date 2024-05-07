import pandas as pd
import numpy as np

from data_processing.flujo_caja_utils import (
    obtener_cantidad_dias,
    expand_row,
    plpl_add_new_column_in_sabana_datos_downstream_base,
    calcular_duracion_proyectos,
    calcular_carga_livianos,
    calcular_compra_n_liviano,
    calcular_carga_mediano,
    calcular_compra_n_pesado_barranca,
    calcular_carga_cast,
    calcular_compra_n_cast,
    calcular_compra_n_pesado_car,
    calcular_compra_n_mediano,
    calcular_depreciacion,
    calcular_sobretasa,
    calcular_ppe
)


# fc_portafolios
class DownBase:
    def __init__(self, input_results, flujo_caja_config, api_config):
        self.flujo_caja_config = flujo_caja_config

        # PLP
        file_path_up = self.flujo_caja_config["fuente"]["flujo_caja_upstream"]["dbfs_path_flujo_caja_upstream"]
        plp_caja_upstream = self.flujo_caja_config["fuente"]["flujo_caja_upstream"]["precios_config"]["sheet_1"]
        df_plp_caja_upstream = pd.read_excel(file_path_up, sheet_name=plp_caja_upstream, header=0, engine='openpyxl')

        # PLP Downstream
        file_path_down = self.flujo_caja_config["fuente"]["plp_downstream"]["dbfs_path_plp_downstream"]
        plp_downstream = self.flujo_caja_config["fuente"]["plp_downstream"]["plp_downstream_config"]["sheet_1"]
        df_plp_downstream = pd.read_excel(file_path_down, sheet_name=plp_downstream, header=0, engine='openpyxl')

        self.input_up = df_plp_caja_upstream
        self.input_results = input_results
        self.input_down = df_plp_downstream

        val_curva_crudo = str(api_config['val_curva_crudo'])
        val_curva_crudo = val_curva_crudo[0]
        self.flujo_caja_config["parametros"]["precio_crudo"] = [i * api_config["val_sensi_precio_crudo"] for i in
                                                                self.flujo_caja_config["parametros"]
                                                                [f"precio_crudo_{val_curva_crudo}"]]

        self.flujo_caja_config["parametros"]["precio_co2"] = [i * api_config["val_sensi_precio_co2"] for i in
                                                              self.flujo_caja_config["parametros"]["precio_co2"]]

    def get_base(self, corrida_fc_portafolio):
        df_plp_caja_upstream = self.input_up

        df_plp_downstream = self.input_down
        # portafolio
        # file_path_portafolio = self.flujo_caja_config["fuente"]["portafolios"]["dbfs_path_portafolios"]
        # portafolio = self.flujo_caja_config["fuente"]["portafolios"]["portafolios_config"]["sheet_name1"]
        df_portafolio = self.input_results

        # PLP Downstream
        file_path_up = self.flujo_caja_config["fuente"]["plp_downstream"]["dbfs_path_plp_downstream"]
        plp_downstream = self.flujo_caja_config["fuente"]["plp_downstream"]["plp_downstream_config"]["sheet_1"]
        df_plp_downstream = pd.read_excel(file_path_up, sheet_name=plp_downstream, header=0, engine='openpyxl')

        # PLP ACTIVO DOWNSTREAM
        file_path_up = self.flujo_caja_config["fuente"]["plp"]["dbfs_path"]
        plp_down_stream = self.flujo_caja_config["fuente"]["plp"]["upstream_config"]["activo_downstream"]
        df_plp_activo_downstream = pd.read_excel(file_path_up, sheet_name=plp_down_stream, header=2, engine='openpyxl')

        df_activo_downstream = pd.DataFrame()
        df_activo_downstream.loc[:, "ACTIVO"] = df_plp_activo_downstream.loc[:, "Activo"]

        rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
        annio_inicio = rango_anios[0]
        annio_fin = rango_anios[1] + 1
        # -----------------------------------

        df_activo_downstream = pd.concat(
            df_activo_downstream.apply(expand_row, args=(annio_inicio, annio_fin,), axis=1).tolist(), ignore_index=True)

        # -----------------------------EMISONES NETAS
        # Emisiones Netas
        columnas_EMISIONES_NETAS_CO2_ALCANCE_1_2 = ['Emisiones Netas Co2 Alcance 1 y 2 2024',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2025',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2026',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2027',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2028',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2029',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2030',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2031',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2032',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2033',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2034',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2035',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2036',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2037',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2038',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2039',
                                                    'Emisiones Netas Co2 Alcance 1 y 2 2040']

        nueva_columna_EMISIONES_NETAS_CO2_ALCANCE_1_2 = 'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'
        df_activo_downstream = plpl_add_new_column_in_sabana_datos_downstream_base(df_plp_activo_downstream,
                                                                                   df_activo_downstream,
                                                                                   columnas_EMISIONES_NETAS_CO2_ALCANCE_1_2,
                                                                                   nueva_columna_EMISIONES_NETAS_CO2_ALCANCE_1_2,
                                                                                   annio_inicio, annio_fin)
        df_activo_downstream["EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"] = df_activo_downstream[
            "EMISIONES_NETAS_CO2_ALCANCE_1_Y_2"].fillna(0)

        activo_grb = df_activo_downstream[df_activo_downstream["ACTIVO"] == 'GRB Consolidado']
        activo_grc = df_activo_downstream[df_activo_downstream["ACTIVO"] == 'RCSAS']

        df_portafolio_1 = df_portafolio[df_portafolio['PORTAFOLIO_1'] == 1]
        df_portafolio_2 = df_portafolio[df_portafolio['PORTAFOLIO_2'] == 1]
        df_portafolio_3 = df_portafolio[df_portafolio['PORTAFOLIO_3'] == 1]
        df_portafolio_4 = df_portafolio[df_portafolio['PORTAFOLIO_4'] == 1]
        df_portafolio_5 = df_portafolio[df_portafolio['PORTAFOLIO_5'] == 1]
        df_portafolio_6 = df_portafolio[df_portafolio['PORTAFOLIO_6'] == 1]

        df_matricula_portafolio_1 = df_portafolio_1['MATRICULA_DIGITAL'].unique()
        df_matricula_portafolio_2 = df_portafolio_2['MATRICULA_DIGITAL'].unique()
        df_matricula_portafolio_3 = df_portafolio_3['MATRICULA_DIGITAL'].unique()
        df_matricula_portafolio_4 = df_portafolio_4['MATRICULA_DIGITAL'].unique()
        df_matricula_portafolio_5 = df_portafolio_5['MATRICULA_DIGITAL'].unique()
        df_matricula_portafolio_6 = df_portafolio_6['MATRICULA_DIGITAL'].unique()

        if corrida_fc_portafolio:
            lista_dataframes = [df_matricula_portafolio_4]
        else:
            lista_dataframes = [df_matricula_portafolio_1, df_matricula_portafolio_2, df_matricula_portafolio_3,
                                df_matricula_portafolio_4, df_matricula_portafolio_5, df_matricula_portafolio_6]

        lista_dataframes = [arr for arr in lista_dataframes if not np.array_equal(arr, np.array([]))]

        # Crear un DataFrame vacío para almacenar los resultados
        df_resultado = pd.DataFrame()
        df_total = pd.DataFrame()

        # Iterar sobre la lista de DataFrames de matrículas
        for i, matricula_df in enumerate(lista_dataframes):
            # Adiciono las columnas precio_crudo,
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo = self.flujo_caja_config["parametros"]["precio_crudo"]
            # dato = self.flujo_caja_config["parametros"]["dato"]
            # importaciones_dato = self.flujo_caja_config["parametros"]["importaciones_dato"]
            payout = self.flujo_caja_config["parametros"]["payout"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios = list(range(annio_inicio, annio_fin))

            lista_anios_dias = []
            for anio in range(annio_inicio, annio_fin):
                cantidad_dias = obtener_cantidad_dias(anio)
                lista_anios_dias.append((cantidad_dias))

            # Asignacion precio crudo por año
            data = {'RANGO_ANIO': anios,
                    # 'dato': dato,
                    # 'importaciones_dato': importaciones_dato,
                    'PRECIO_CRUDO_BRENT': precio_crudo,
                    'PAYOUT': payout
                    }

            df_anio_crudo = pd.DataFrame(data)
            df_precio_crudo = df_anio_crudo[['RANGO_ANIO', 'PRECIO_CRUDO_BRENT']]
            df_anio_crudo = df_anio_crudo.T

            new_columns = df_anio_crudo.iloc[0]
            df_anio_crudo = df_anio_crudo[1:]
            df_anio_crudo.columns = new_columns

            # Filtrar df_plp_caja_upstream utilizando las matrículas del DataFrame actual
            df_plp_caja_upstream_new = df_plp_caja_upstream[
                df_plp_caja_upstream['MATRICULA_DIGITAL'].isin(matricula_df)]

            if corrida_fc_portafolio:
                porfafolio = 'PORTAFOLIO_CASO_BASE'
                df_plp_caja_upstream_new['NOMBRE_PORTAFOLIO'] = porfafolio
            else:
                df_plp_caja_upstream_new['NOMBRE_PORTAFOLIO'] = f'portafolio_{i + 1}'
                porfafolio = f'portafolio_{i + 1}'

                # FLUJO CAJA UPSTREAM
            df_flujo_caja_downstream = pd.DataFrame()
            df_flujo_caja_downstream = df_plp_caja_upstream_new[
                ['ID_PROYECTO', 'MATRICULA_DIGITAL', 'ACTIVO', 'UN_NEG_REGIONAL', 'RANGO_ANIO', 'CAMPO', 'CAPEX',
                 'CRUDO_DESPUES_R', 'TOTAL_VOLUMENES_PROD_DESPUES_R', 'HIDROCARBURO']]

            # ----------------------------liviano,mediano,pesado
            df_condicion7 = df_flujo_caja_downstream[df_flujo_caja_downstream['HIDROCARBURO'] == 'Pesado']
            df_condicion8 = df_flujo_caja_downstream[df_flujo_caja_downstream['HIDROCARBURO'] == 'Liviano']
            df_condicion9 = df_flujo_caja_downstream[df_flujo_caja_downstream['HIDROCARBURO'] == 'Mediano']
            df_condicion10 = df_flujo_caja_downstream[df_flujo_caja_downstream['HIDROCARBURO'] == 'Castilla']

            df_condicion7['HIDROCARBURO'] = df_condicion7['HIDROCARBURO'].str.upper()
            df_condicion8['HIDROCARBURO'] = df_condicion8['HIDROCARBURO'].str.upper()
            df_condicion9['HIDROCARBURO'] = df_condicion9['HIDROCARBURO'].str.upper()
            df_condicion10['HIDROCARBURO'] = df_condicion10['HIDROCARBURO'].str.upper()

            df_refinacion = pd.concat([df_condicion7, df_condicion8, df_condicion9, df_condicion10], ignore_index=True)

            df_refinacion = df_refinacion.pivot_table(index='HIDROCARBURO', columns='RANGO_ANIO',
                                                      values='CRUDO_DESPUES_R', aggfunc='sum')

            if 'LIVIANO' not in df_refinacion.index:
                Liviano = df_refinacion.loc['LIVIANO'] = 0
            else:
                Liviano = df_refinacion.loc['LIVIANO']

            if 'MEDIANO' not in df_refinacion.index:
                Mediano = df_refinacion.loc['MEDIANO'] = 0
            else:
                Mediano = df_refinacion.loc['MEDIANO']

            if 'CASTILLA' not in df_refinacion.index:
                Castilla = df_refinacion.loc['CASTILLA'] = 0
            else:
                Castilla = df_refinacion.loc['CASTILLA']

            if 'PESADO' not in df_refinacion.index:
                Pesado = df_refinacion.loc['PESADO'] = 0
            else:
                Pesado = df_refinacion.loc['PESADO']
            # ---------------------------------
            df_refinacion_new = df_refinacion.T
            df_refinacion_new.index.names = [None]
            df_refinacion_new['RANGO_ANIO'] = df_refinacion_new.index

            # -------------------------------
            df_refinacion = df_refinacion_new.loc[:, ['RANGO_ANIO', 'LIVIANO', 'MEDIANO', 'CASTILLA', 'PESADO']]
            df_refinacion = df_refinacion.reset_index(drop=True)

            # ----------------------------------
            # CARGAS_BARRANCABERMEJA
            liviano_10_porciento = self.flujo_caja_config["parametros"]["liviano_10_porciento"]
            mediano_10_porciento = self.flujo_caja_config["parametros"]["mediano_10_porciento"]
            castilla_10_porciento = self.flujo_caja_config["parametros"]["castilla_10_porciento"]
            pesado_10_porciento = self.flujo_caja_config["parametros"]["pesado_10_porciento"]
            carga_max_228 = self.flujo_caja_config["parametros"]["carga_max_228"]
            porcentaje_carga_max_liviano = self.flujo_caja_config["parametros"]["porcentaje_carga_max_liviano"]
            porcentaje_carga_max_pesado = self.flujo_caja_config["parametros"]["porcentaje_carga_max_pesado"]
            porcentaje_carga_min_liviano = self.flujo_caja_config["parametros"]["porcentaje_carga_min_liviano"]
            porcentaje_carga_min_pesado = self.flujo_caja_config["parametros"]["porcentaje_carga_min_pesado"]

            df_refinacion['LIVIANO_NAL'] = df_refinacion['LIVIANO'] * liviano_10_porciento
            df_refinacion['MEDIANO_NAL'] = df_refinacion['MEDIANO'] * mediano_10_porciento
            df_refinacion['CASTILLA_NAL'] = df_refinacion['CASTILLA'] * castilla_10_porciento
            df_refinacion['PESADO_NAL'] = df_refinacion['PESADO'] * pesado_10_porciento

            df_refinacion['CARGA_MAX'] = carga_max_228
            df_refinacion['PORCENTAJE_CARGA_MAX_LIVIANO'] = porcentaje_carga_max_liviano
            df_refinacion['PORCENTAJE_CARGA_MAX_PESADO'] = porcentaje_carga_max_pesado

            df_refinacion['L_MAX_LIVIANO'] = df_refinacion['CARGA_MAX'] * df_refinacion['PORCENTAJE_CARGA_MAX_LIVIANO']
            df_refinacion['L_MAX_PESADO'] = df_refinacion['CARGA_MAX'] * df_refinacion['PORCENTAJE_CARGA_MAX_PESADO']

            df_refinacion['PORCENTAJE_CARGA_MIN_LIVIANO'] = porcentaje_carga_min_liviano
            df_refinacion['PORCENTAJE_CARGA_MIN_PESADO'] = porcentaje_carga_min_pesado

            df_refinacion['L_MIN_LIVIANO'] = df_refinacion['CARGA_MAX'] * df_refinacion['PORCENTAJE_CARGA_MIN_LIVIANO']
            df_refinacion['L_MIN_PESADO'] = df_refinacion['CARGA_MAX'] * df_refinacion['PORCENTAJE_CARGA_MIN_PESADO']

            # ---------------------------------------------CALCULOS_LIVIANO
            # carga_livianos

            df_refinacion['CARGA_LIVIANO'] = df_refinacion.apply(calcular_carga_livianos, axis=1)
            df_refinacion['CARGA_LIVIANO'] = df_refinacion['CARGA_LIVIANO'].fillna(0)

            # ----------------------------------------------
            # requiere_compra_livianos
            df_refinacion['REQUIERE_COMPRA_LIVIANO'] = [1 if x1 < x2 else 0 for x1, x2 in
                                                        zip(df_refinacion['CARGA_LIVIANO'],
                                                            df_refinacion['L_MIN_LIVIANO'])]

            # ---------------------------------------------
            # compra_n_liviano

            df_refinacion['COMPRA_N_LIVIANO'] = df_refinacion.apply(calcular_compra_n_liviano, axis=1)

            # ----------------------------------------------
            # requiere_imp_livianos
            df_refinacion['REQUIERE_IMP_LIVIANO'] = [1 if (x1 + x2) < x3 else 0 for x1, x2, x3 in
                                                     zip(df_refinacion['CARGA_LIVIANO'],
                                                         df_refinacion['COMPRA_N_LIVIANO'],
                                                         df_refinacion['L_MIN_LIVIANO'])]

            # ----------------------------------------------
            # compra_imp_livianos
            df_refinacion['COMPRA_IMP_LIVIANO'] = [x2 - (x3 + x4) if x1 == 1 else 0 for x1, x2, x3, x4 in
                                                   zip(df_refinacion['REQUIERE_IMP_LIVIANO'],
                                                       df_refinacion['L_MIN_LIVIANO'], df_refinacion['CARGA_LIVIANO'],
                                                       df_refinacion['COMPRA_N_LIVIANO'])]
            # Multiplicar solo el primer valor por 0
            # df_refinacion['COMPRA_IMP_LIVIANO'].iloc[0] = 0
            # Multiplicar todos por 0
            # df_refinacion['COMPRA_IMP_LIVIANO'] = [x * 0 for x in df_refinacion['COMPRA_IMP_LIVIANO']]

            # ----------------------------------------------
            # carga_f_liviano
            df_refinacion['CARGA_F_LIVIANO'] = df_refinacion['CARGA_LIVIANO'] + df_refinacion['COMPRA_N_LIVIANO'] + \
                                               df_refinacion['COMPRA_IMP_LIVIANO']

            # ---------------------------------------------CALCULOS_PESADOS
            # carga_pesados

            df_refinacion['CARGA_PESADO'] = df_refinacion.apply(calcular_carga_mediano, axis=1)
            df_refinacion['CARGA_PESADO'] = df_refinacion['CARGA_PESADO'].fillna(0)
            # ----------------------------------------------
            # requiere_compra_pesados
            df_refinacion['REQUIERE_COMPRA_PESADO'] = [1 if x1 < x2 else 0 for x1, x2 in
                                                       zip(df_refinacion['CARGA_PESADO'],
                                                           df_refinacion['L_MIN_PESADO'])]

            # ---------------------------------------------
            # compra_n_pesados

            df_refinacion['COMPRA_N_PESADO'] = df_refinacion.apply(calcular_compra_n_pesado_barranca, axis=1)

            # ----------------------------------------------
            # requiere_imp_pesados
            df_refinacion['REQUIERE_IMP_PESADO'] = [1 if (x1 + x2) < x3 else 0 for x1, x2, x3 in
                                                    zip(df_refinacion['CARGA_PESADO'], df_refinacion['COMPRA_N_PESADO'],
                                                        df_refinacion['L_MIN_PESADO'])]

            # ----------------------------------------------
            # compra_imp_pesados
            df_refinacion['COMPRA_IMP_PESADO'] = [x2 - (x3 + x4) if x1 == 1 else 0 for x1, x2, x3, x4 in
                                                  zip(df_refinacion['REQUIERE_IMP_PESADO'],
                                                      df_refinacion['L_MIN_PESADO'], df_refinacion['CARGA_PESADO'],
                                                      df_refinacion['COMPRA_N_PESADO'])]
            # Multiplicar solo el primer valor por 0
            # df_refinacion['COMPRA_IMP_PESADO'].iloc[0] = 0
            # Multiplicar todos por 0
            # df_refinacion['COMPRA_IMP_PESADO'] = [x * 0 for x in df_refinacion['COMPRA_IMP_PESADO']]

            # ----------------------------------------------
            # carga_f_pesados
            df_refinacion['CARGA_F_PESADO'] = df_refinacion['CARGA_PESADO'] + df_refinacion['COMPRA_N_PESADO'] + \
                                              df_refinacion['COMPRA_IMP_PESADO']

            # ----------------------------------------------
            df_refinacion['CARGA_L_Y_PESADO'] = df_refinacion['CARGA_F_LIVIANO'] + df_refinacion['CARGA_F_PESADO']
            df_refinacion['GAP'] = df_refinacion['CARGA_MAX'] - df_refinacion['CARGA_L_Y_PESADO']

            # ---------------------------------------------CALCULOS_MEDIANOS
            # carga_medianos
            df_refinacion['CARGA_MEDIANO'] = [x1 if x1 < x2 else x2 for x1, x2 in
                                              zip(df_refinacion['MEDIANO'], df_refinacion['GAP'])]

            # ----------------------------------------------
            # requiere_compra_mediano
            df_refinacion['REQUIERE_COMPRA_MEDIANO'] = [1 if x1 < x2 else 0 for x1, x2 in
                                                        zip(df_refinacion['CARGA_MEDIANO'], df_refinacion['GAP'])]

            # ----------------------------------------------
            # compra_n_mediano
            df_refinacion['COMPRA_N_MEDIANO'] = [x1 if x1 < (x2 - x3) else 0 for x1, x2, x3 in
                                                 zip(df_refinacion['MEDIANO_NAL'], df_refinacion['GAP'],
                                                     df_refinacion['CARGA_MEDIANO'])]

            # ----------------------------------------------
            # requiere_imp_mediano
            df_refinacion['REQUIERE_IMP_MEDIANO'] = [1 if (x1 + x2) < x3 else 0 for x1, x2, x3 in
                                                     zip(df_refinacion['CARGA_MEDIANO'],
                                                         df_refinacion['COMPRA_N_MEDIANO'], df_refinacion['GAP'])]

            # ----------------------------------------------
            # compra_imp_mediano
            df_refinacion['COMPRA_IMP_MEDIANO'] = [x2 - (x3 + x4) if x1 == 1 else 0 for x1, x2, x3, x4 in
                                                   zip(df_refinacion['REQUIERE_IMP_MEDIANO'], df_refinacion['GAP'],
                                                       df_refinacion['CARGA_MEDIANO'],
                                                       df_refinacion['COMPRA_N_MEDIANO'])]
            # Multiplicar solo el primer valor por 0
            # df_refinacion['COMPRA_IMP_MEDIANO'].iloc[0] = 0
            # Multiplicar todos por 0
            # df_refinacion['COMPRA_IMP_MEDIANO'] = [x * 0 for x in df_refinacion['COMPRA_IMP_MEDIANO']]

            # ----------------------------------------------
            # compra_f_mediano
            df_refinacion['CARGA_F_MEDIANO'] = df_refinacion['CARGA_MEDIANO'] + df_refinacion['COMPRA_N_MEDIANO'] + \
                                               df_refinacion['COMPRA_IMP_MEDIANO']

            # -------------------------------------CARGA_TOTAL
            # carga_total
            df_refinacion['CARGA_TOTAL'] = df_refinacion['CARGA_F_LIVIANO'] + df_refinacion['CARGA_F_PESADO'] + \
                                           df_refinacion['CARGA_F_MEDIANO']

            df_refinacion = df_refinacion.loc[:,
                            ['RANGO_ANIO', 'LIVIANO', 'MEDIANO', 'CASTILLA', 'PESADO', 'LIVIANO_NAL', 'MEDIANO_NAL',
                             'CASTILLA_NAL', 'PESADO_NAL', 'CARGA_MAX', 'PORCENTAJE_CARGA_MAX_LIVIANO',
                             'PORCENTAJE_CARGA_MAX_PESADO', 'PORCENTAJE_CARGA_MIN_LIVIANO',
                             'PORCENTAJE_CARGA_MIN_PESADO', 'L_MAX_LIVIANO', 'L_MAX_PESADO', 'L_MIN_LIVIANO',
                             'L_MIN_PESADO', 'CARGA_LIVIANO', 'REQUIERE_COMPRA_LIVIANO', 'COMPRA_N_LIVIANO',
                             'REQUIERE_IMP_LIVIANO', 'COMPRA_IMP_LIVIANO', 'CARGA_F_LIVIANO', 'CARGA_PESADO',
                             'REQUIERE_COMPRA_PESADO', 'COMPRA_N_PESADO', 'REQUIERE_IMP_PESADO', 'COMPRA_IMP_PESADO',
                             'CARGA_F_PESADO', 'CARGA_L_Y_PESADO', 'GAP', 'CARGA_MEDIANO', 'REQUIERE_COMPRA_MEDIANO',
                             'COMPRA_N_MEDIANO', 'REQUIERE_IMP_MEDIANO', 'COMPRA_IMP_MEDIANO', 'CARGA_F_MEDIANO',
                             'CARGA_TOTAL']]

            # ----------------------------------------------------REFINARIA_CARTAGENEA
            # df_refinacion_cartagena
            df_refinacion_cartagena = pd.DataFrame()

            df_refinacion_cartagena['RANGO_ANIO'] = df_refinacion['RANGO_ANIO']

            # LIVIANO
            df_refinacion_cartagena['LIVIANO'] = df_refinacion['LIVIANO'] - df_refinacion['CARGA_LIVIANO']

            # MEDIANO
            df_refinacion_cartagena['MEDIANO'] = df_refinacion['MEDIANO'] - df_refinacion['CARGA_MEDIANO']

            # CASTILLA
            df_refinacion_cartagena['CASTILLA'] = df_refinacion['CASTILLA']

            # PESADO
            df_refinacion_cartagena['PESADO'] = df_refinacion['PESADO'] - df_refinacion['CARGA_PESADO']

            # LIVIANO_NAL
            df_refinacion_cartagena['LIVIANO_NAL'] = df_refinacion['LIVIANO_NAL'] - df_refinacion['COMPRA_N_LIVIANO']

            # MEDIANO_NAL
            df_refinacion_cartagena['MEDIANO_NAL'] = df_refinacion['MEDIANO_NAL'] - df_refinacion['COMPRA_N_MEDIANO']

            # CAST_NAL
            df_refinacion_cartagena['CAST_NAL'] = df_refinacion['CASTILLA_NAL']

            # PESADO_NAL
            df_refinacion_cartagena['PESADO_NAL'] = df_refinacion['PESADO_NAL'] - df_refinacion['COMPRA_N_PESADO']

            carga_max_210 = self.flujo_caja_config["parametros"]["carga_max_210"]
            porcentaje_carga_max_cast = self.flujo_caja_config["parametros"]["porcentaje_carga_max_cast"]
            porcentaje_carga_max_pesado_cartagena = self.flujo_caja_config["parametros"][
                "porcentaje_carga_max_pesado_cartagena"]
            porcentaje_carga_min_cast = self.flujo_caja_config["parametros"]["porcentaje_carga_min_cast"]
            porcentaje_carga_min_pesado_cartagena = self.flujo_caja_config["parametros"][
                "porcentaje_carga_min_pesado_cartagena"]
            porcentaje_compra_imp_liviano_cartagena = self.flujo_caja_config["parametros"][
                "porcentaje_compra_imp_liviano_cartagena"]

            # CARGA_MAX
            df_refinacion_cartagena['CARGA_MAX'] = carga_max_210
            df_refinacion_cartagena['PORCENTAJE_CARGA_MAX_CAST_CAR'] = porcentaje_carga_max_cast
            df_refinacion_cartagena['PORCENTAJE_CARGA_MAX_PESADO_CAR'] = porcentaje_carga_max_pesado_cartagena

            # L_MAX_CAST
            df_refinacion_cartagena['L_MAX_CAST'] = df_refinacion_cartagena['CARGA_MAX'] * df_refinacion_cartagena[
                'PORCENTAJE_CARGA_MAX_CAST_CAR']

            # L_MAX_PESADO
            df_refinacion_cartagena['L_MAX_PESADO'] = df_refinacion_cartagena['CARGA_MAX'] * df_refinacion_cartagena[
                'PORCENTAJE_CARGA_MAX_PESADO_CAR']

            df_refinacion_cartagena['PORCENTAJE_CARGA_MIN_CAST_CAR'] = porcentaje_carga_min_cast
            df_refinacion_cartagena['PORCENTAJE_CARGA_MIN_PESADO_CAR'] = porcentaje_carga_min_pesado_cartagena

            # L_MIN_CAST
            df_refinacion_cartagena['L_MIN_CAST'] = df_refinacion_cartagena['CARGA_MAX'] * df_refinacion_cartagena[
                'PORCENTAJE_CARGA_MIN_CAST_CAR']

            # L_MIN_PESADO
            df_refinacion_cartagena['L_MIN_PESADO'] = df_refinacion_cartagena['CARGA_MAX'] * df_refinacion_cartagena[
                'PORCENTAJE_CARGA_MIN_PESADO_CAR']

            # ---------------------------------------------CALCULOS_CASTILLA_CARTAGENA
            # carga_castilla

            # Apply the 'calcular_carga_cast' function to calculate 'CARGA_CAST' and add it as a new column.
            df_refinacion_cartagena['CARGA_CAST'] = df_refinacion_cartagena.apply(calcular_carga_cast, axis=1)
            df_refinacion_cartagena['CARGA_CAST'] = df_refinacion_cartagena['CARGA_CAST'].fillna(0)

            # ----------------------------------------------
            # requiere_compra_cast
            df_refinacion_cartagena['REQUIERE_COMPRA_CAST'] = [1 if x1 < x2 else 0 for x1, x2 in
                                                               zip(df_refinacion_cartagena['CARGA_CAST'],
                                                                   df_refinacion_cartagena['L_MIN_CAST'])]

            # ---------------------------------------------
            # compra_n_cast

            # Apply the 'calcular_compra_n_cast' function to calculate 'COMPRA_N_CAST' and add it as a new column.
            df_refinacion_cartagena['COMPRA_N_CAST'] = df_refinacion_cartagena.apply(calcular_compra_n_cast, axis=1)

            # ----------------------------------------------
            # requiere_imp_cast
            df_refinacion_cartagena['REQUIERE_IMP_CAST'] = [1 if (x1 + x2) < x3 else 0 for x1, x2, x3 in
                                                            zip(df_refinacion_cartagena['CARGA_CAST'],
                                                                df_refinacion_cartagena['COMPRA_N_CAST'],
                                                                df_refinacion_cartagena['L_MIN_CAST'])]

            # ----------------------------------------------
            # compra_imp_pesados
            df_refinacion_cartagena['COMPRA_IMP_CAST'] = [x2 - (x3 + x4) if x1 == 1 else 0 for x1, x2, x3, x4 in
                                                          zip(df_refinacion_cartagena['REQUIERE_IMP_CAST'],
                                                              df_refinacion_cartagena['L_MIN_CAST'],
                                                              df_refinacion_cartagena['CARGA_CAST'],
                                                              df_refinacion_cartagena['COMPRA_N_CAST'])]

            # ----------------------------------------------
            # carga_f_pesados
            df_refinacion_cartagena['CARGA_F_CAST'] = df_refinacion_cartagena['CARGA_CAST'] + df_refinacion_cartagena[
                'COMPRA_N_CAST'] + df_refinacion_cartagena['COMPRA_IMP_CAST']

            # _--------------------------------------------
            df_refinacion_cartagena['GRAP_PESADO'] = df_refinacion_cartagena['L_MAX_PESADO'] - df_refinacion_cartagena[
                'CARGA_F_CAST']

            # ---------------------------------------------CALCULOS_PESADO_CARTAGENA
            # carga_pesado
            df_refinacion_cartagena['CARGA_PESADO'] = [x1 if x1 < x2 else x2 for x1, x2 in
                                                       zip(df_refinacion_cartagena['PESADO'],
                                                           df_refinacion_cartagena['GRAP_PESADO'])]

            # ---------------------------------------------
            # requiere_compra_cast
            df_refinacion_cartagena['REQUIERE_COMPRA_PESADO'] = [1 if (x1 + x2) < x3 else 0 for x1, x2, x3 in
                                                                 zip(df_refinacion_cartagena['CARGA_PESADO'],
                                                                     df_refinacion_cartagena['CARGA_F_CAST'],
                                                                     df_refinacion_cartagena['L_MIN_PESADO'])]

            # ---------------------------------------------
            # compra_n_cast

            # Apply the 'calcular_compra_n_pesado_car' function to calculate 'COMPRA_N_PESADO' and add it as a new column.
            df_refinacion_cartagena['COMPRA_N_PESADO'] = df_refinacion_cartagena.apply(calcular_compra_n_pesado_car,
                                                                                       axis=1)

            # ----------------------------------------------
            # requiere_imp_cast
            df_refinacion_cartagena['REQUIERE_IMP_PESADO'] = [1 if (x1 + x2 + x3) < x4 else 0 for x1, x2, x3, x4 in
                                                              zip(df_refinacion_cartagena['CARGA_PESADO'],
                                                                  df_refinacion_cartagena['COMPRA_N_PESADO'],
                                                                  df_refinacion_cartagena['CARGA_F_CAST'],
                                                                  df_refinacion_cartagena['L_MIN_PESADO'])]

            # ----------------------------------------------
            # compra_imp_pesados
            df_refinacion_cartagena['COMPRA_IMP_PESADO'] = [x2 - (x3 + x4 + x5) if x1 == 1 else 0 for x1, x2, x3, x4, x5
                                                            in zip(df_refinacion_cartagena['REQUIERE_IMP_PESADO'],
                                                                   df_refinacion_cartagena['L_MIN_PESADO'],
                                                                   df_refinacion_cartagena['CARGA_PESADO'],
                                                                   df_refinacion_cartagena['COMPRA_N_PESADO'],
                                                                   df_refinacion_cartagena['CARGA_F_CAST'])]

            # ----------------------------------------------
            # carga_f_pesados
            df_refinacion_cartagena['CARGA_F_PESADO'] = df_refinacion_cartagena['CARGA_PESADO'] + \
                                                        df_refinacion_cartagena['COMPRA_N_PESADO'] + \
                                                        df_refinacion_cartagena['COMPRA_IMP_PESADO']

            # --------------------------------
            # Carga_F_Pesado&Cast
            df_refinacion_cartagena['CARGA_F_PESADO_Y_CAST'] = df_refinacion_cartagena['CARGA_F_PESADO'] + \
                                                               df_refinacion_cartagena['CARGA_F_CAST']

            # compra_imp_liviano
            df_refinacion_cartagena['PORCENTAJE_COMPRA_IMP_LIVIANO_CAR'] = porcentaje_compra_imp_liviano_cartagena
            df_refinacion_cartagena['COMPRA_IMP_LIVIANO'] = df_refinacion_cartagena['CARGA_MAX'] * \
                                                            df_refinacion_cartagena['PORCENTAJE_COMPRA_IMP_LIVIANO_CAR']

            # Carga_C_&_P_&_L
            df_refinacion_cartagena['CARGA_C_Y_P_Y_L'] = df_refinacion_cartagena['CARGA_F_CAST'] + \
                                                         df_refinacion_cartagena['CARGA_F_PESADO'] + \
                                                         df_refinacion_cartagena['COMPRA_IMP_LIVIANO']

            # gap
            df_refinacion_cartagena['GAP'] = df_refinacion_cartagena['CARGA_MAX'] - df_refinacion_cartagena[
                'CARGA_C_Y_P_Y_L']

            # ---------------------------------------------CALCULOS_MEDIANO_CARTAGENA
            # carga_mediano
            df_refinacion_cartagena['CARGA_MEDIANO'] = [x1 if x1 < x2 else x2 for x1, x2 in
                                                        zip(df_refinacion_cartagena['MEDIANO'],
                                                            df_refinacion_cartagena['GAP'])]

            # ---------------------------------------------
            # requiere_compra_cast
            df_refinacion_cartagena['REQUIERE_COMPRA_MEDIANO'] = [1 if x1 < x2 else 0 for x1, x2 in
                                                                  zip(df_refinacion_cartagena['CARGA_MEDIANO'],
                                                                      df_refinacion_cartagena['GAP'])]

            # ---------------------------------------------
            # requiere_compra_n_mediano

            # Apply the 'calcular_compra_n_mediano' function to calculate 'COMPRA_N_MEDIANO' and add it as a new column.
            df_refinacion_cartagena['COMPRA_N_MEDIANO'] = df_refinacion_cartagena.apply(calcular_compra_n_mediano,
                                                                                        axis=1)

            # ---------------------------------------------
            # requiere_requiere_imp_mediano
            df_refinacion_cartagena['REQUIERE_IMP_MEDIANO'] = [1 if x1 + x2 < x3 else 0 for x1, x2, x3 in
                                                               zip(df_refinacion_cartagena['CARGA_MEDIANO'],
                                                                   df_refinacion_cartagena['COMPRA_N_MEDIANO'],
                                                                   df_refinacion_cartagena['GAP'])]

            # ---------------------------------------------
            # requiere_compra_imp_mediano
            df_refinacion_cartagena['COMPRA_IMP_MEDIANO'] = [x2 - (x3 + x4) if x1 == 1 else 0 for x1, x2, x3, x4 in
                                                             zip(df_refinacion_cartagena['REQUIERE_IMP_MEDIANO'],
                                                                 df_refinacion_cartagena['GAP'],
                                                                 df_refinacion_cartagena['CARGA_MEDIANO'],
                                                                 df_refinacion_cartagena['COMPRA_N_MEDIANO'])]

            # ---------------------------------------------
            # carga_f_mediano
            df_refinacion_cartagena['CARGA_F_MEDIANO'] = df_refinacion_cartagena['CARGA_MEDIANO'] + \
                                                         df_refinacion_cartagena['COMPRA_N_MEDIANO'] + \
                                                         df_refinacion_cartagena['COMPRA_IMP_MEDIANO']

            # -------------------------------------CARGA_TOTAL_CARTAGENA
            # carga_total
            df_refinacion_cartagena['CARGA_TOTAL'] = df_refinacion_cartagena['CARGA_F_CAST'] + df_refinacion_cartagena[
                'CARGA_F_PESADO'] + df_refinacion_cartagena['COMPRA_IMP_LIVIANO'] + df_refinacion_cartagena[
                                                         'CARGA_F_MEDIANO']

            df_refinacion_cartagena = df_refinacion_cartagena.loc[:,
                                      ['RANGO_ANIO', 'LIVIANO', 'MEDIANO', 'CASTILLA', 'PESADO', 'LIVIANO_NAL',
                                       'MEDIANO_NAL', 'CAST_NAL', 'PESADO_NAL', 'CARGA_MAX',
                                       'PORCENTAJE_CARGA_MAX_CAST_CAR', 'PORCENTAJE_CARGA_MAX_PESADO_CAR',
                                       'PORCENTAJE_CARGA_MIN_CAST_CAR', 'PORCENTAJE_CARGA_MIN_PESADO_CAR', 'L_MAX_CAST',
                                       'L_MAX_PESADO', 'L_MIN_CAST', 'L_MIN_PESADO', 'CARGA_CAST',
                                       'REQUIERE_COMPRA_CAST', 'COMPRA_N_CAST', 'REQUIERE_IMP_CAST', 'COMPRA_IMP_CAST',
                                       'CARGA_F_CAST', 'GRAP_PESADO', 'CARGA_PESADO', 'REQUIERE_COMPRA_PESADO',
                                       'COMPRA_N_PESADO', 'REQUIERE_IMP_PESADO', 'COMPRA_IMP_PESADO', 'CARGA_F_PESADO',
                                       'CARGA_F_PESADO_Y_CAST', 'PORCENTAJE_COMPRA_IMP_LIVIANO_CAR',
                                       'COMPRA_IMP_LIVIANO', 'CARGA_C_Y_P_Y_L', 'GAP', 'CARGA_MEDIANO',
                                       'REQUIERE_COMPRA_MEDIANO', 'COMPRA_N_MEDIANO', 'REQUIERE_IMP_MEDIANO',
                                       'COMPRA_IMP_MEDIANO', 'CARGA_F_MEDIANO', 'CARGA_TOTAL']]

            # ------------------------------------------df_refinacion, df_refinacion_cartagena
            # productos ventas
            rango_anio = df_refinacion['RANGO_ANIO']
            cargas_grb_barrancabermeja_pesado = df_refinacion['CARGA_F_PESADO']
            cargas_grb_barrancabermeja_mediano = df_refinacion['CARGA_F_MEDIANO']
            cargas_grb_barrancabermeja_liviano = df_refinacion['CARGA_F_LIVIANO']

            # ------------------------------PARAMETROS
            rendimientos_polipropileno_pgr_pesado = self.flujo_caja_config["parametros"][
                "rendimientos_polipropileno_pgr_pesado"]
            rendimientos_polipropileno_pgr_mediano = self.flujo_caja_config["parametros"][
                "rendimientos_polipropileno_pgr_mediano"]
            rendimientos_polipropileno_pgr_liviano = self.flujo_caja_config["parametros"][
                "rendimientos_polipropileno_pgr_liviano"]
            rendimientos_glp_pesado = self.flujo_caja_config["parametros"]["rendimientos_glp_pesado"]
            rendimientos_glp_mediano = self.flujo_caja_config["parametros"]["rendimientos_glp_mediano"]
            rendimientos_glp_liviano = self.flujo_caja_config["parametros"]["rendimientos_glp_liviano"]
            rendimientos_nafta_pesado = self.flujo_caja_config["parametros"]["rendimientos_nafta_pesado"]
            rendimientos_nafta_mediano = self.flujo_caja_config["parametros"]["rendimientos_nafta_mediano"]
            rendimientos_nafta_liviano = self.flujo_caja_config["parametros"]["rendimientos_nafta_liviano"]
            rendimientos_gasolina_pesado = self.flujo_caja_config["parametros"]["rendimientos_gasolina_pesado"]
            rendimientos_gasolina_mediano = self.flujo_caja_config["parametros"]["rendimientos_gasolina_mediano"]
            rendimientos_gasolina_liviano = self.flujo_caja_config["parametros"]["rendimientos_gasolina_liviano"]
            rendimientos_jet_pesado = self.flujo_caja_config["parametros"]["rendimientos_jet_pesado"]
            rendimientos_jet_mediano = self.flujo_caja_config["parametros"]["rendimientos_jet_mediano"]
            rendimientos_jet_liviano = self.flujo_caja_config["parametros"]["rendimientos_jet_liviano"]
            rendimientos_asfalto_pesado = self.flujo_caja_config["parametros"]["rendimientos_asfalto_pesado"]
            rendimientos_asfalto_mediano = self.flujo_caja_config["parametros"]["rendimientos_asfalto_mediano"]
            rendimientos_asfalto_liviano = self.flujo_caja_config["parametros"]["rendimientos_asfalto_liviano"]
            rendimientos_prod_industrial_petroquímico_pesado = self.flujo_caja_config["parametros"][
                "rendimientos_prod_industrial_petroquímico_pesado"]
            rendimientos_prod_industrial_petroquímico_mediano = self.flujo_caja_config["parametros"][
                "rendimientos_prod_industrial_petroquímico_mediano"]
            rendimientos_prod_industrial_petroquímico_liviano = self.flujo_caja_config["parametros"][
                "rendimientos_prod_industrial_petroquímico_liviano"]
            rendimientos_crudo_recosntituido_pesado = self.flujo_caja_config["parametros"][
                "rendimientos_crudo_recosntituido_pesado"]
            rendimientos_crudo_recosntituido_mediano = self.flujo_caja_config["parametros"][
                "rendimientos_crudo_recosntituido_mediano"]
            rendimientos_crudo_recosntituido_liviano = self.flujo_caja_config["parametros"][
                "rendimientos_crudo_recosntituido_liviano"]
            rendimientos_gas_combustion_pesado = self.flujo_caja_config["parametros"][
                "rendimientos_gas_combustion_pesado"]
            rendimientos_gas_combustion_mediano = self.flujo_caja_config["parametros"][
                "rendimientos_gas_combustion_mediano"]
            rendimientos_gas_combustion_liviano = self.flujo_caja_config["parametros"][
                "rendimientos_gas_combustion_liviano"]
            rendimientos_diesel_pesado = self.flujo_caja_config["parametros"]["rendimientos_diesel_pesado"]
            rendimientos_diesel_mediano = self.flujo_caja_config["parametros"]["rendimientos_diesel_mediano"]
            rendimientos_diesel_liviano = self.flujo_caja_config["parametros"]["rendimientos_diesel_liviano"]
            rendimientos_combustoleo_pesado = self.flujo_caja_config["parametros"]["rendimientos_combustoleo_pesado"]
            rendimientos_combustoleo_mediano = self.flujo_caja_config["parametros"]["rendimientos_combustoleo_mediano"]
            rendimientos_combustoleo_liviano = self.flujo_caja_config["parametros"]["rendimientos_combustoleo_liviano"]
            rendimientos_coque_pesado = self.flujo_caja_config["parametros"]["rendimientos_coque_pesado"]
            rendimientos_coque_mediano = self.flujo_caja_config["parametros"]["rendimientos_coque_mediano"]
            rendimientos_coque_liviano = self.flujo_caja_config["parametros"]["rendimientos_coque_liviano"]
            rendimientos_salf_pesado = self.flujo_caja_config["parametros"]["rendimientos_salf_pesado"]
            rendimientos_salf_mediano = self.flujo_caja_config["parametros"]["rendimientos_salf_mediano"]
            rendimientos_salf_liviano = self.flujo_caja_config["parametros"]["rendimientos_salf_liviano"]
            rendimientos_hvo_pesado = self.flujo_caja_config["parametros"]["rendimientos_hvo_pesado"]
            rendimientos_hvo_mediano = self.flujo_caja_config["parametros"]["rendimientos_hvo_mediano"]
            rendimientos_hvo_liviano = self.flujo_caja_config["parametros"]["rendimientos_hvo_liviano"]
            rendimientos_h2_pesado = self.flujo_caja_config["parametros"]["rendimientos_h2_pesado"]
            rendimientos_h2_mediano = self.flujo_caja_config["parametros"]["rendimientos_h2_mediano"]
            rendimientos_h2_liviano = self.flujo_caja_config["parametros"]["rendimientos_h2_liviano"]
            rendimientos_fuel_oil_pesado = self.flujo_caja_config["parametros"]["rendimientos_fuel_oil_pesado"]
            rendimientos_fuel_oil_mediano = self.flujo_caja_config["parametros"]["rendimientos_fuel_oil_mediano"]
            rendimientos_fuel_oil_liviano = self.flujo_caja_config["parametros"]["rendimientos_fuel_oil_liviano"]

            # -------------------------------------POLIPROPILENO
            polipropileno_pgr_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_polipropileno_pgr_pesado))
            polipropileno_pgr_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_polipropileno_pgr_mediano))
            polipropileno_pgr_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_polipropileno_pgr_liviano))

            df_polipropileno_pgr_pesado = pd.DataFrame({'POLIPROPILENO_PGR_PESADO': polipropileno_pgr_pesado})
            df_polipropileno_pgr_mediano = pd.DataFrame({'POLIPROPILENO_PGR_MEDIANO': polipropileno_pgr_mediano})
            df_polipropileno_pgr_liviano = pd.DataFrame({'POLIPROPILENO_PGR_LIVIANO': polipropileno_pgr_liviano})

            # -------------------------------------GLP
            glp_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_glp_pesado))
            glp_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_glp_mediano))
            glp_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_glp_liviano))

            df_glp_pesado = pd.DataFrame({'GLP_PESADO': glp_pesado})
            df_glp_mediano = pd.DataFrame({'GLP_MEDIANO': glp_mediano})
            df_glp_liviano = pd.DataFrame({'GLP_LIVIANO': glp_liviano})

            # ------------------------------------NAFTA
            nafta_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_nafta_pesado))
            nafta_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_nafta_mediano))
            nafta_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_nafta_liviano))

            df_nafta_pesado = pd.DataFrame({'NAFTA_PESADO': nafta_pesado})
            df_nafta_mediano = pd.DataFrame({'NAFTA_MEDIANO': nafta_mediano})
            df_nafta_liviano = pd.DataFrame({'NAFTA_LIVIANO': nafta_liviano})

            # ------------------------------------GASOLINA
            gasolino_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_gasolina_pesado))
            gasolino_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_gasolina_mediano))
            gasolino_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_gasolina_liviano))

            df_gasolino_pesado = pd.DataFrame({'GASOLINA_PESADO': gasolino_pesado})
            df_gasolino_mediano = pd.DataFrame({'GASOLINA_MEDIANO': gasolino_mediano})
            df_gasolino_liviano = pd.DataFrame({'GASOLINA_LIVIANO': gasolino_liviano})

            # ------------------------------------JET
            jet_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_jet_pesado))
            jet_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_jet_mediano))
            jet_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_jet_liviano))

            df_jet_pesado = pd.DataFrame({'JET_PESADO': jet_pesado})
            df_jet_mediano = pd.DataFrame({'JET_MEDIANO': jet_mediano})
            df_jet_liviano = pd.DataFrame({'JET_LIVIANO': jet_liviano})

            # -----------------------------------ASFALTO
            asfalto_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_asfalto_pesado))
            asfalto_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_asfalto_mediano))
            asfalto_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_asfalto_liviano))

            df_asfalto_pesado = pd.DataFrame({'ASFALTO_PESADO': asfalto_pesado})
            df_asfalto_mediano = pd.DataFrame({'ASFALTO_MEDIANO': asfalto_mediano})
            df_asfalto_liviano = pd.DataFrame({'ASFALTO_LIVIANO': asfalto_liviano})

            # ------------------------PROD_INDUSTRIAL_PETROQUIMICO_PESADO
            prod_industrial_petroquímico_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado,
                                                           rendimientos_prod_industrial_petroquímico_pesado))
            prod_industrial_petroquímico_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano,
                                                            rendimientos_prod_industrial_petroquímico_mediano))
            prod_industrial_petroquímico_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano,
                                                            rendimientos_prod_industrial_petroquímico_liviano))

            df_prod_industrial_petroquímico_pesado = pd.DataFrame(
                {'PROD_INDUSTRIAL_PETROQUIMICO_PESADO': prod_industrial_petroquímico_pesado})
            df_prod_industrial_petroquímico_mediano = pd.DataFrame(
                {'PROD_INDUSTRIAL_PETROQUIMICO_MEDIANO': prod_industrial_petroquímico_mediano})
            df_prod_industrial_petroquímico_liviano = pd.DataFrame(
                {'PROD_INDUSTRIAL_PETROQUIMICO_LIVIANO': prod_industrial_petroquímico_liviano})

            # -------------------------------------CRUDO_RECONSTITUIDO
            crudo_recosntituido_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_crudo_recosntituido_pesado))
            crudo_recosntituido_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_crudo_recosntituido_mediano))
            crudo_recosntituido_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_crudo_recosntituido_liviano))

            df_crudo_recosntituido_pesado = pd.DataFrame({'CRUDO_RECONSTITUIDO_PESADO': crudo_recosntituido_pesado})
            df_crudo_recosntituido_mediano = pd.DataFrame({'CRUDO_RECONSTITUIDO_MEDIANO': crudo_recosntituido_mediano})
            df_crudo_recosntituido_liviano = pd.DataFrame({'CRUDO_RECONSTITUIDO_LIVIANO': crudo_recosntituido_liviano})

            # -----------------------------------------GAS_COMBUSTION
            gas_combustion_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_gas_combustion_pesado))
            gas_combustion_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_gas_combustion_mediano))
            gas_combustion_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_gas_combustion_liviano))

            df_gas_combustion_pesado = pd.DataFrame({'GAS_COMBUSTION_PESADO': gas_combustion_pesado})
            df_gas_combustion_mediano = pd.DataFrame({'GAS_COMBUSTION_MEDIANO': gas_combustion_mediano})
            df_gas_combustion_liviano = pd.DataFrame({'GAS_COMBUSTION_LIVIANO': gas_combustion_liviano})

            # -----------------------------------------DIESEL
            diesel_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_diesel_pesado))
            diesel_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_diesel_mediano))
            diesel_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_diesel_liviano))

            df_diesel_pesado = pd.DataFrame({'DIESEL_PESADO': diesel_pesado})
            df_diesel_mediano = pd.DataFrame({'DIESEL_MEDIANO': diesel_mediano})
            df_diesel_liviano = pd.DataFrame({'DIESEL_LIVIANO': diesel_liviano})

            # -----------------------------------------COMBUSTOLEO
            combustoleo_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_combustoleo_pesado))
            combustoleo_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_combustoleo_mediano))
            combustoleo_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_combustoleo_liviano))

            df_combustoleo_pesado = pd.DataFrame({'COMBUSTOLEO_PESADO': combustoleo_pesado})
            df_combustoleo_mediano = pd.DataFrame({'COMBUSTOLEO_MEDIANO': combustoleo_mediano})
            df_combustoleo_liviano = pd.DataFrame({'COMBUSTOLEO_LIVIANO': combustoleo_liviano})

            # -------------------------------------COQUE
            coque_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_coque_pesado))
            coque_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_coque_mediano))
            coque_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_coque_liviano))

            df_coque_pesado = pd.DataFrame({'COQUE_PESADO': coque_pesado})
            df_coque_mediano = pd.DataFrame({'COQUE_MEDIANO': coque_mediano})
            df_coque_liviano = pd.DataFrame({'COQUE_LIVIANO': coque_liviano})

            # -------------------------------SALF
            saf_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_salf_pesado))
            saf_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_salf_mediano))
            saf_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_salf_liviano))

            df_saf_pesado = pd.DataFrame({'SAF_PESADO': saf_pesado})
            df_saf_mediano = pd.DataFrame({'SAF_MEDIANO': saf_mediano})
            df_saf_liviano = pd.DataFrame({'SAF_LIVIANO': saf_liviano})

            # -------------------------------HVO
            # Calcular las listas correspondientes
            hvo_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_hvo_pesado))
            hvo_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_hvo_mediano))
            hvo_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_hvo_liviano))

            df_hvo_pesado = pd.DataFrame({'HVO_PESADO': hvo_pesado})
            df_hvo_mediano = pd.DataFrame({'HVO_MEDIANO': hvo_mediano})
            df_hvo_liviano = pd.DataFrame({'HVO_LIVIANO': hvo_liviano})

            # -------------------------------H2
            h2_pesado = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_h2_pesado))
            h2_mediano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_h2_mediano))
            h2_liviano = list(map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_h2_liviano))

            df_h2_pesado = pd.DataFrame({'H2_PESADO': h2_pesado})
            df_h2_mediano = pd.DataFrame({'H2_MEDIANO': h2_mediano})
            df_h2_liviano = pd.DataFrame({'H2_LIVIANO': h2_liviano})

            # -------------------------------FUEL_OIL
            fuel_oil_pesado = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_pesado, rendimientos_fuel_oil_pesado))
            fuel_oil_mediano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_mediano, rendimientos_fuel_oil_mediano))
            fuel_oil_liviano = list(
                map(lambda x, y: x * y, cargas_grb_barrancabermeja_liviano, rendimientos_fuel_oil_liviano))

            df_fuel_oil_pesado = pd.DataFrame({'FUEL_OIL_PESADO': fuel_oil_pesado})
            df_fuel_oil_mediano = pd.DataFrame({'FUEL_OIL_MEDIANO': fuel_oil_mediano})
            df_fuel_oil_liviano = pd.DataFrame({'FUEL_OIL_LIVIANO': fuel_oil_liviano})

            df_productos_ventas = pd.concat(
                [rango_anio, df_polipropileno_pgr_pesado, df_polipropileno_pgr_mediano, df_polipropileno_pgr_liviano,
                 df_glp_pesado, df_glp_mediano, df_glp_liviano, df_nafta_pesado, df_nafta_mediano, df_nafta_liviano,
                 df_gasolino_pesado, df_gasolino_mediano, df_gasolino_liviano, df_jet_pesado, df_jet_mediano,
                 df_jet_liviano, df_asfalto_pesado, df_asfalto_mediano, df_asfalto_liviano,
                 df_prod_industrial_petroquímico_pesado, df_prod_industrial_petroquímico_mediano,
                 df_prod_industrial_petroquímico_liviano, df_crudo_recosntituido_pesado, df_crudo_recosntituido_mediano,
                 df_crudo_recosntituido_liviano, df_gas_combustion_pesado, df_gas_combustion_mediano,
                 df_gas_combustion_liviano, df_diesel_pesado, df_diesel_mediano, df_diesel_liviano,
                 df_combustoleo_pesado, df_combustoleo_mediano, df_combustoleo_liviano, df_coque_pesado,
                 df_coque_mediano, df_coque_liviano, df_saf_pesado, df_saf_mediano, df_saf_liviano, df_hvo_pesado,
                 df_hvo_mediano, df_hvo_liviano, df_h2_pesado, df_h2_mediano, df_h2_liviano, df_fuel_oil_pesado,
                 df_fuel_oil_mediano, df_fuel_oil_liviano], axis=1)

            # ----------------POLIPROPILENO_PGR_TOTAL
            df_productos_ventas['POLIPROPILENO_PGR_TOTAL'] = df_productos_ventas['POLIPROPILENO_PGR_PESADO'] + \
                                                             df_productos_ventas['POLIPROPILENO_PGR_MEDIANO'] + \
                                                             df_productos_ventas['POLIPROPILENO_PGR_LIVIANO']

            # ----------------GLP_TOTAL
            df_productos_ventas['GLP_TOTAL'] = df_productos_ventas['GLP_PESADO'] + df_productos_ventas['GLP_MEDIANO'] + \
                                               df_productos_ventas['GLP_LIVIANO']

            # ----------------NAFTA_TOTAL
            df_productos_ventas['NAFTA_TOTAL'] = df_productos_ventas['NAFTA_PESADO'] + df_productos_ventas[
                'NAFTA_MEDIANO'] + df_productos_ventas['NAFTA_LIVIANO']

            # ----------------GASOLINA_TOTAL
            df_productos_ventas['GASOLINA_TOTAL'] = df_productos_ventas['GASOLINA_PESADO'] + df_productos_ventas[
                'GASOLINA_MEDIANO'] + df_productos_ventas['GASOLINA_LIVIANO']

            # ----------------JET_TOTAL
            df_productos_ventas['JET_TOTAL'] = df_productos_ventas['JET_PESADO'] + df_productos_ventas['JET_MEDIANO'] + \
                                               df_productos_ventas['JET_LIVIANO']

            # ----------------ASFALTO_TOTAL
            df_productos_ventas['ASFALTO_TOTAL'] = df_productos_ventas['ASFALTO_PESADO'] + df_productos_ventas[
                'ASFALTO_MEDIANO'] + df_productos_ventas['ASFALTO_LIVIANO']

            # ----------------PROD_INDUSTRIALES_Y_PETROQUIMICOS_TOTAL
            df_productos_ventas['PROD_INDUSTRIALES_Y_PETROQUIMICOS_TOTAL'] = df_productos_ventas[
                                                                                 'PROD_INDUSTRIAL_PETROQUIMICO_PESADO'] + \
                                                                             df_productos_ventas[
                                                                                 'PROD_INDUSTRIAL_PETROQUIMICO_MEDIANO'] + \
                                                                             df_productos_ventas[
                                                                                 'PROD_INDUSTRIAL_PETROQUIMICO_LIVIANO']

            # ----------------CRUDO_RECONSTITUIDO_TOTAL
            df_productos_ventas['CRUDO_RECONSTITUIDO_TOTAL'] = df_productos_ventas['CRUDO_RECONSTITUIDO_PESADO'] + \
                                                               df_productos_ventas['CRUDO_RECONSTITUIDO_MEDIANO'] + \
                                                               df_productos_ventas['CRUDO_RECONSTITUIDO_LIVIANO']

            # ----------------GAS_COMBUSTIBLE_TOTAL
            df_productos_ventas['GAS_COMBUSTIBLE_TOTAL'] = df_productos_ventas['GAS_COMBUSTION_PESADO'] + \
                                                           df_productos_ventas['GAS_COMBUSTION_MEDIANO'] + \
                                                           df_productos_ventas['GAS_COMBUSTION_LIVIANO']

            # ----------------DIESEL_TOTAL
            df_productos_ventas['DIESEL_TOTAL'] = df_productos_ventas['DIESEL_PESADO'] + df_productos_ventas[
                'DIESEL_MEDIANO'] + df_productos_ventas['DIESEL_LIVIANO']

            # ----------------COMBUSTOLEO_TOTAL
            df_productos_ventas['COMBUSTOLEO_TOTAL'] = df_productos_ventas['COMBUSTOLEO_PESADO'] + df_productos_ventas[
                'COMBUSTOLEO_MEDIANO'] + df_productos_ventas['COMBUSTOLEO_LIVIANO']

            # ----------------COQUE_TOTAL
            df_productos_ventas['COQUE_TOTAL'] = df_productos_ventas['COQUE_PESADO'] + df_productos_ventas[
                'COQUE_MEDIANO'] + df_productos_ventas['COQUE_LIVIANO']

            # ----------------SAF_TOTAL
            df_productos_ventas['SAF_TOTAL'] = df_productos_ventas['SAF_PESADO'] + df_productos_ventas['SAF_MEDIANO'] + \
                                               df_productos_ventas['SAF_LIVIANO']

            # ----------------HVO_TOTAL
            df_productos_ventas['HVO_TOTAL'] = df_productos_ventas['HVO_PESADO'] + df_productos_ventas['HVO_MEDIANO'] + \
                                               df_productos_ventas['HVO_LIVIANO']

            # ----------------H2_BAJAS_EMISIONES_TOTAL
            df_productos_ventas['H2_BAJAS_EMISIONES_TOTAL'] = df_productos_ventas['H2_PESADO'] + df_productos_ventas[
                'H2_MEDIANO'] + df_productos_ventas['H2_LIVIANO']

            # ----------------FUEL_OIL_TOTAL
            df_productos_ventas['FUEL_OIL_TOTAL'] = df_productos_ventas['FUEL_OIL_PESADO'] + df_productos_ventas[
                'FUEL_OIL_MEDIANO'] + df_productos_ventas['FUEL_OIL_LIVIANO']

            # -------------------------------------
            # el flujo de caja se puede calcular anual
            # Traer el precio del precio_crudo - por año
            # Traer la cantidad de dias por año

            df_fc_down_grb = pd.merge(df_productos_ventas, df_refinacion, on='RANGO_ANIO')

            # ---------------------------------------
            # Adiciono las columnas PARAMETRICAS
            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo = self.flujo_caja_config["parametros"]["precio_crudo"]
            precio_co2 = self.flujo_caja_config["parametros"]["precio_co2"]
            payout = self.flujo_caja_config["parametros"]["payout"]
            impuesto_renta = self.flujo_caja_config["parametros"]["impuesto_renta"]

            diferencial_polipropileno_pgr = self.flujo_caja_config["parametros"]["diferencial_polipropileno_pgr"]
            diferencial_glp = self.flujo_caja_config["parametros"]["diferencial_glp"]
            diferencial_nafta = self.flujo_caja_config["parametros"]["diferencial_nafta"]
            diferencial_gasolina = self.flujo_caja_config["parametros"]["diferencial_gasolina"]
            diferencial_jet = self.flujo_caja_config["parametros"]["diferencial_jet"]
            diferencial_asfalto = self.flujo_caja_config["parametros"]["diferencial_asfalto"]
            diferencial_prod_industriales_y_petroquimicos = self.flujo_caja_config["parametros"][
                "diferencial_prod_industriales_y_petroquimicos"]
            diferencial_crudo_reconstituido = self.flujo_caja_config["parametros"]["diferencial_crudo_reconstituido"]
            diferencial_gas_combustible = self.flujo_caja_config["parametros"]["diferencial_gas_combustible"]
            diferencial_diesel = self.flujo_caja_config["parametros"]["diferencial_diesel"]
            diferencial_combustoleo = self.flujo_caja_config["parametros"]["diferencial_combustoleo"]
            diferencial_coque = self.flujo_caja_config["parametros"]["diferencial_coque"]
            diferencial_saf = self.flujo_caja_config["parametros"]["diferencial_saf"]
            diferencial_hvo = self.flujo_caja_config["parametros"]["diferencial_hvo"]
            diferencial_h2_bajas_emisiones = self.flujo_caja_config["parametros"]["diferencial_h2_bajas_emisiones"]
            diferencial_fuel_oil = self.flujo_caja_config["parametros"]["diferencial_fuel_oil"]
            diferencial_liviano = self.flujo_caja_config["parametros"]["diferencial_liviano"]
            diferencial_mediano = self.flujo_caja_config["parametros"]["diferencial_mediano"]
            diferencial_pesado = self.flujo_caja_config["parametros"]["diferencial_pesado"]
            costos_fijos_perfil_entrada = self.flujo_caja_config["parametros"]["costos_fijos_perfil_entrada"]
            emisiones_netas_co2_alcance_1_y_2 = activo_grb['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2']
            diferencial_compra_n_liviano = self.flujo_caja_config["parametros"]["diferencial_compra_n_liviano"]
            diferencial_compra_imp_liviano = self.flujo_caja_config["parametros"]["diferencial_compra_imp_liviano"]
            diferencial_compra_n_pesado = self.flujo_caja_config["parametros"]["diferencial_compra_n_pesado"]
            diferencial_compra_imp_pesado = self.flujo_caja_config["parametros"]["diferencial_compra_imp_pesado"]
            diferencial_compra_n_mediano = self.flujo_caja_config["parametros"]["diferencial_compra_n_mediano"]
            diferencial_compra_imp_mediano = self.flujo_caja_config["parametros"]["diferencial_compra_imp_mediano"]
            tarifa_c_f = self.flujo_caja_config["parametros"]["tarifa_c_f"]

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios = list(range(annio_inicio, annio_fin))

            lista_anios_dias = []
            for anio in range(annio_inicio, annio_fin):
                cantidad_dias = obtener_cantidad_dias(anio)
                lista_anios_dias.append((cantidad_dias))

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anioss = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anioss, historico_proyeccion_precios, percentil_1, percentil_2,
                                              percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Asignacion precio crudo por año
            data = {'RANGO_ANIO': anios,
                    'CANT_DIAS_ANIO': lista_anios_dias,
                    'PRECIO_CRUDO_BRENT': precio_crudo,
                    'PRECIO_CO2': precio_co2,
                    'IMPUESTO_RENTA': impuesto_renta,
                    'PAYOUT': payout,
                    'DIFERENCIAL_POLIPROPILENO_PGR': diferencial_polipropileno_pgr,
                    'DIFERENCIAL_GLP': diferencial_glp,
                    'DIFERENCIAL_NAFTA': diferencial_nafta,
                    'DIFERENCIAL_GASOLINA': diferencial_gasolina,
                    'DIFERENCIAL_JET': diferencial_jet,
                    'DIFERENCIAL_ASFALTO': diferencial_asfalto,
                    'DIFERENCIAL_PROD_INDUSTRIALES_Y_PETROQUIMICOS': diferencial_prod_industriales_y_petroquimicos,
                    'DIFERENCIAL_CRUDO_RECONSTITUIDO': diferencial_crudo_reconstituido,
                    'DIFERENCIAL_GAS_COMBUSTIBLE': diferencial_gas_combustible,
                    'DIFERENCIAL_DIESEL': diferencial_diesel,
                    'DIFERENCIAL_COMBUSTOLEO': diferencial_combustoleo,
                    'DIFERENCIAL_COQUE': diferencial_coque,
                    'DIFERENCIAL_SAF': diferencial_saf,
                    'DIFERENCIAL_HVO': diferencial_hvo,
                    'DIFERENCIAL_H2_BAJAS_EMISIONES': diferencial_h2_bajas_emisiones,
                    'DIFERENCIAL_FUEL_OIL': diferencial_fuel_oil,
                    'DIFERENCIAL_LIVIANO': diferencial_liviano,
                    'DIFERENCIAL_MEDIANO': diferencial_mediano,
                    'DIFERENCIAL_PESADO': diferencial_pesado,
                    'DIFERENCIAL_COMPRA_N_LIVIANO': diferencial_compra_n_liviano,
                    'DIFERENCIAL_COMPRA_IMP_LIVIANO': diferencial_compra_imp_liviano,
                    'DIFERENCIAL_COMPRA_N_PESADO': diferencial_compra_n_pesado,
                    'DIFERENCIAL_COMPRA_IMP_PESADO': diferencial_compra_imp_pesado,
                    'DIFERENCIAL_COMPRA_N_MEDIANO': diferencial_compra_n_mediano,
                    'DIFERENCIAL_COMPRA_IMP_MEDIANO': diferencial_compra_imp_mediano,
                    'COSTO_FIJOS_PERFIL_ENTRADA': costos_fijos_perfil_entrada,
                    'TARIFA_C_F': tarifa_c_f,
                    'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2': emisiones_netas_co2_alcance_1_y_2
                    }

            df_anio_down_grb = pd.DataFrame(data)

            df_fc_down_grb = df_fc_down_grb.merge(df_anio_down_grb, on='RANGO_ANIO', how='left')

            df_fc_down_grb = pd.merge(df_fc_down_grb, df_sobretasa, on=['RANGO_ANIO', 'PRECIO_CRUDO_BRENT'],
                                      how='inner')

            # _-----------------------------------------------------CALCULOS_BARRANCABERMEJA
            # Flujo de caja GRB
            #	POLIPROPILENO_PGR -->	Z*(Brent + Diferencial) * cantidad dias / 1000 Anual   df_sabana_datos['CANT_DIAS_ANIO'])/ 1000
            # ----------POLIPROPILENO_PGR_TOTAL
            df_fc_down_grb['ING_POLIPROPILENO_PGR'] = df_fc_down_grb['POLIPROPILENO_PGR_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_POLIPROPILENO_PGR']) * (
                                                                  df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	GLP --> 	AA*(Brent + Diferencial) Anual
            # --------------GLP_TOTAL
            df_fc_down_grb['ING_GLP'] = df_fc_down_grb['GLP_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_GLP']) * (
                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	NAFTA -->	AB*(Brent + Diferencial) Anual
            # -----------------NAFTA_TOTAL
            df_fc_down_grb['ING_NAFTA'] = df_fc_down_grb['NAFTA_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_NAFTA']) * (
                                                      df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	GASOLINA -->	AB*(Brent + Diferencial) Anual
            # --------------GASOLINA_TOTAL
            df_fc_down_grb['ING_GASOLINA'] = df_fc_down_grb['GASOLINA_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_GASOLINA']) * (
                                                         df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	JET  -->	AC*(Brent + Diferencial) Anual
            # --------------JET_TOTAL
            df_fc_down_grb['ING_JET'] = df_fc_down_grb['JET_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_JET']) * (
                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	ASFALTO --> 	AD*(Brent + Diferencial) Anual
            # --------------ASFALTO_TOTAL
            df_fc_down_grb['ING_ASFALTO'] = df_fc_down_grb['ASFALTO_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_ASFALTO']) * (
                                                        df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	PROD_INDUSTRIALES_Y_PETROQUIMICOS --> 	AD*(Brent + Diferencial) Anual
            # --------------PROD_INDUSTRIALES_Y_PETROQUIMICOS_TOTAL
            df_fc_down_grb['ING_PROD_INDUSTRIALES_Y_PETROQUIMICOS'] = df_fc_down_grb[
                                                                          'PROD_INDUSTRIALES_Y_PETROQUIMICOS_TOTAL'] * (
                                                                                  df_fc_down_grb['PRECIO_CRUDO_BRENT'] +
                                                                                  df_fc_down_grb[
                                                                                      'DIFERENCIAL_PROD_INDUSTRIALES_Y_PETROQUIMICOS']) * (
                                                                                  df_fc_down_grb[
                                                                                      'CANT_DIAS_ANIO'] / 1000)

            #	CRUDO_RECONSTITUIDO --> 	AD*(Brent + Diferencial) Anual
            # --------------CRUDO_RECONSTITUIDO_TOTAL
            df_fc_down_grb['ING_CRUDO_RECONSTITUIDO'] = df_fc_down_grb['CRUDO_RECONSTITUIDO_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_CRUDO_RECONSTITUIDO']) * (
                                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	GAS_COMBUSTIBLE --> 	AD*(Brent + Diferencial) Anual
            # --------------GAS_COMBUSTIBLE_TOTAL
            df_fc_down_grb['ING_GAS_COMBUSTIBLE'] = df_fc_down_grb['GAS_COMBUSTIBLE_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_GAS_COMBUSTIBLE']) * (
                                                                df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	DIESEL --> 	AD*(Brent + Diferencial) Anual
            # --------------DIESEL_TOTAL
            df_fc_down_grb['ING_DIESEL'] = df_fc_down_grb['DIESEL_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_DIESEL']) * (
                                                       df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	COMBUSTOLEO --> 	AD*(Brent + Diferencial) Anual
            # --------------COMBUSTOLEO_TOTAL
            df_fc_down_grb['ING_COMBUSTOLEO'] = df_fc_down_grb['COMBUSTOLEO_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_COMBUSTOLEO']) * (
                                                            df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	COQUE --> 	AD*(Brent + Diferencial) Anual
            # --------------COQUE_TOTAL
            df_fc_down_grb['ING_COQUE'] = df_fc_down_grb['COQUE_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_COQUE']) * (
                                                      df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	SAF --> 	AD*(Brent + Diferencial) Anual
            # --------------SAF_TOTAL
            df_fc_down_grb['ING_SAF'] = df_fc_down_grb['SAF_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_SAF']) * (
                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	HVO --> 	AD*(Brent + Diferencial) Anual
            # --------------HVO_TOTAL
            df_fc_down_grb['ING_HVO'] = df_fc_down_grb['HVO_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_HVO']) * (
                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            #	H2_BAJAS_EMISIONES --> 	AD*(Brent + Diferencial) Anual
            # --------------H2_BAJAS_EMISIONES_TOTAL
            df_fc_down_grb['ING_H2_BAJAS_EMISIONES'] = df_fc_down_grb['H2_BAJAS_EMISIONES_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_H2_BAJAS_EMISIONES']) * (
                                                                   df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # FUEL_OIL --> 	AD*(Brent + Diferencial) Anual
            # --------------FUEL_OIL_TOTAL
            df_fc_down_grb['ING_FUEL_OIL'] = df_fc_down_grb['FUEL_OIL_TOTAL'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] + df_fc_down_grb['DIFERENCIAL_FUEL_OIL']) * (
                                                         df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # Total Ingresos	Suma
            df_fc_down_grb['TOTAL_INGRESOS'] = df_fc_down_grb['ING_POLIPROPILENO_PGR'] + df_fc_down_grb['ING_GLP'] + \
                                               df_fc_down_grb['ING_NAFTA'] + df_fc_down_grb['ING_GASOLINA'] + \
                                               df_fc_down_grb['ING_JET'] + df_fc_down_grb['ING_ASFALTO'] + \
                                               df_fc_down_grb['ING_PROD_INDUSTRIALES_Y_PETROQUIMICOS'] + df_fc_down_grb[
                                                   'ING_CRUDO_RECONSTITUIDO'] + df_fc_down_grb['ING_GAS_COMBUSTIBLE'] + \
                                               df_fc_down_grb['ING_DIESEL'] + df_fc_down_grb['ING_COMBUSTOLEO'] + \
                                               df_fc_down_grb['ING_COQUE'] + df_fc_down_grb['ING_SAF'] + df_fc_down_grb[
                                                   'ING_HVO'] + df_fc_down_grb['ING_H2_BAJAS_EMISIONES'] + \
                                               df_fc_down_grb['ING_FUEL_OIL']

            # --------------------------------LIVIANOS
            # COST_LIVIANO -->	O*(Brent – Dif)
            df_fc_down_grb['COST_LIVIANO'] = df_fc_down_grb['CARGA_LIVIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_LIVIANO']) * (
                                                         df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_N_LIVIANO -->	O*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_N_LIVIANO'] = df_fc_down_grb['COMPRA_N_LIVIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_N_LIVIANO']) * (
                                                                  df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_IMP_LIVIANO -->	O*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_IMP_LIVIANO'] = df_fc_down_grb['COMPRA_IMP_LIVIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_IMP_LIVIANO']) * (
                                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # ------------------------------PESADOS
            # COST_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grb['COST_PESADO'] = df_fc_down_grb['CARGA_PESADO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_PESADO']) * (
                                                        df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_N_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_N_PESADO'] = df_fc_down_grb['COMPRA_N_PESADO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_N_PESADO']) * (
                                                                 df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_IMP_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_IMP_PESADO'] = df_fc_down_grb['COMPRA_IMP_PESADO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_IMP_PESADO']) * (
                                                                   df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # -------------------------------MEDIANOS
            # COST_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grb['COST_MEDIANO'] = df_fc_down_grb['CARGA_MEDIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_MEDIANO']) * (
                                                         df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_N_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_N_MEDIANO'] = df_fc_down_grb['COMPRA_N_MEDIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_N_MEDIANO']) * (
                                                                  df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COMPRA_IMP_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grb['COST_COMPRA_IMP_MEDIANO'] = df_fc_down_grb['COMPRA_IMP_MEDIANO'] * (
                        df_fc_down_grb['PRECIO_CRUDO_BRENT'] - df_fc_down_grb['DIFERENCIAL_COMPRA_IMP_MEDIANO']) * (
                                                                    df_fc_down_grb['CANT_DIAS_ANIO'] / 1000)

            # COSTOS_FIJOS	  --> Perfil de entrada
            df_fc_down_grb['COSTOS_FIJOS'] = df_fc_down_grb['TARIFA_C_F'] * df_fc_down_grb['CARGA_TOTAL'] * \
                                             df_fc_down_grb['CANT_DIAS_ANIO'] / 1000

            # COSTO_INTERNO_CO2  -->	Emisiones*Precio CO2
            df_fc_down_grb['COSTO_INTERNO_CO2'] = df_fc_down_grb['PRECIO_CO2'] * df_fc_down_grb[
                'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2']

            # Total Costos	Suma
            df_fc_down_grb['TOTAL_COSTOS'] = df_fc_down_grb['COST_LIVIANO'] + df_fc_down_grb['COST_COMPRA_N_LIVIANO'] + \
                                             df_fc_down_grb['COST_COMPRA_IMP_LIVIANO'] + df_fc_down_grb['COST_PESADO'] + \
                                             df_fc_down_grb['COST_COMPRA_N_PESADO'] + df_fc_down_grb[
                                                 'COST_COMPRA_IMP_PESADO'] + df_fc_down_grb['COST_MEDIANO'] + \
                                             df_fc_down_grb['COST_COMPRA_N_MEDIANO'] + df_fc_down_grb[
                                                 'COST_COMPRA_IMP_MEDIANO'] + df_fc_down_grb['COSTOS_FIJOS'] + \
                                             df_fc_down_grb['COSTO_INTERNO_CO2']

            # BA	EBITDA	AR-AZ
            df_fc_down_grb['EBITDA'] = df_fc_down_grb['TOTAL_INGRESOS'] - df_fc_down_grb['TOTAL_COSTOS']
            # ----------------------------------------------------------------

            # BE	Capex --> CAPEX # CAPEX GRB

            # df_capex_grb = pd.DataFrame()
            # df_capex_grb = df_plp_downstream[['ID_PROYECTO', 'MATRICULA_DIGITAL', 'RANGO_ANIO', 'GERENCIA', 'CAPEX']]
            # df_capex_grb['GERENCIA'] = df_capex_grb['GERENCIA'].str.lower()

            # gerencia_filtrar = ['grb']
            # df_capex_grb_filtro1 = df_capex_grb[df_capex_grb['MATRICULA_DIGITAL'].isin(matricula_df)]
            # df_capex_grb =df_capex_grb_filtro1[df_capex_grb_filtro1['GERENCIA'].isin(gerencia_filtrar)]

            # if df_capex_grb.empty:
            df_capex_grb_anio = pd.DataFrame(columns=["RANGO_ANIO", "CAPEX"])
            for año in anios:
                df_capex_grb_anio = df_capex_grb_anio.append({"RANGO_ANIO": año, "CAPEX": 0}, ignore_index=True)
                # else:
            #   df_capex_grb_anio = df_capex_grb.pivot_table(index='GERENCIA', columns='RANGO_ANIO', values='CAPEX', aggfunc='sum')
            #   df_capex_grb_anio = df_capex_grb_anio.T
            #   df_capex_grb_anio.insert(1, 'RANGO_ANIO', df_capex_grb_anio.index)
            #   df_capex_grb_anio.rename(columns={'grb': 'CAPEX'}, inplace=True)
            #   df_capex_grb_anio = df_capex_grb_anio.reset_index(drop=True)

            df_fc_down_grb = pd.merge(df_fc_down_grb, df_capex_grb_anio, on='RANGO_ANIO')

            # BB	DD&A	Línea recta 20 años

            df_depreciacion = calcular_depreciacion(df_capex_grb_anio)

            df_union_ppe = pd.merge(df_capex_grb_anio, df_depreciacion, on=['RANGO_ANIO'], how='inner')

            df_con_ppe = calcular_ppe(df_union_ppe)

            df_fc_down_grb = pd.merge(df_fc_down_grb, df_con_ppe, on=['RANGO_ANIO', 'CAPEX'])

            # # BC	Base gravable 	BA-BB
            df_fc_down_grb['BASE_GRABABLE'] = df_fc_down_grb['EBITDA'] + df_fc_down_grb['COSTO_INTERNO_CO2'] - \
                                              df_fc_down_grb['SUM_DEPRE']

            # # BD	Impuesto	BC*(35%+Sobre tasa)
            df_fc_down_grb['IMPUESTOS'] = df_fc_down_grb['BASE_GRABABLE'] * (
                        df_fc_down_grb['IMPUESTO_RENTA'] + df_fc_down_grb['SOBRETASA'])

            # # BF	Flujo de Caja	BA-BD-BE
            df_fc_down_grb['FLUJO_CAJA'] = df_fc_down_grb['EBITDA'] - df_fc_down_grb['IMPUESTOS'] - df_fc_down_grb[
                'CAPEX']

            # # BG	Utilidad	BA-BD
            df_fc_down_grb['EBIT'] = df_fc_down_grb['EBITDA'] - df_fc_down_grb['IMPUESTOS']

            # # BH	Dividendos	BG*Payout%
            df_fc_down_grb['DIVIDENDO'] = df_fc_down_grb['EBIT'] * df_fc_down_grb['PAYOUT']

            # def calcular_roace(row):
            #   if row['CAPITAL_EMPLEADO'] == 0:
            #     return 0
            #   return (row['EBIT'] / row['CAPITAL_EMPLEADO'])

            # #roace
            # df_fc_down_grb['ROACE']  = df_fc_down_grb.apply(calcular_roace, axis=1)
            # df_fc_down_grb['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
            # df_fc_down_grb['ROACE'] = df_fc_down_grb["ROACE"].fillna(0)

            df_fc_down_grb.insert(0, 'MATRICULA_DIGITAL', 'grb-' + porfafolio)
            df_fc_down_grb.insert(1, 'ID_PROYECTO', '1')
            df_fc_down_grb.insert(2, 'LINEA_DE_NEGOCIO', 'Hidrocarburos')
            df_fc_down_grb.insert(3, 'OPCION_ESTRATEGICA', 'Negocio Tradicional')
            df_fc_down_grb.insert(4, 'CATEGORIA', '')
            df_fc_down_grb.insert(5, 'SUBCATEGORIA', '')
            df_fc_down_grb.insert(6, 'EMPRESA', 'ECP S.A')
            df_fc_down_grb.insert(7, 'SEGMENTO', 'Downstream')
            df_fc_down_grb.insert(8, 'SUBSEGMENTO', 'Downstream')
            df_fc_down_grb.insert(9, 'UN_NEG_REGIONAL', 'Gerencia Refinería Barrancabermeja')
            df_fc_down_grb.insert(10, 'GERENCIA', 'GRB')
            df_fc_down_grb.insert(11, 'ACTIVO', 'GRB')
            df_fc_down_grb.insert(12, 'DEPARTAMENTO', 'Santander')
            df_fc_down_grb.insert(13, 'PORTAFOLIO', porfafolio)
            df_fc_down_grb.insert(14, 'NOMBRE_PROYECTO', 'Operacion refineria barranca')
            df_fc_down_grb.insert(15, 'TIPO_DE_INVERSION', 'Básica')

            # ---------------------------------------

            cargas_grc_total = df_refinacion_cartagena['CARGA_TOTAL']

            # ------------------------------PARAMETROS
            rendimientos_polipropileno_pgr_car = self.flujo_caja_config["parametros"][
                "rendimientos_polipropileno_pgr_car"]
            rendimientos_glp_car = self.flujo_caja_config["parametros"]["rendimientos_glp_car"]
            rendimientos_nafta_car = self.flujo_caja_config["parametros"]["rendimientos_nafta_car"]
            rendimientos_gasolina_car = self.flujo_caja_config["parametros"]["rendimientos_gasolina_car"]
            rendimientos_jet_car = self.flujo_caja_config["parametros"]["rendimientos_jet_car"]
            rendimientos_diesel_car = self.flujo_caja_config["parametros"]["rendimientos_diesel_car"]
            rendimientos_vgo = self.flujo_caja_config["parametros"]["rendimientos_vgo"]
            rendimientos_slurry = self.flujo_caja_config["parametros"]["rendimientos_slurry"]
            rendimientos_brea_asfalto = self.flujo_caja_config["parametros"]["rendimientos_brea_asfalto"]

            # -------------------------------------POLIPROPILENO
            polipropileno_pgr = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_polipropileno_pgr_car))

            df_polipropileno_pgr = pd.DataFrame({'POLIPROPILENO_PGR': polipropileno_pgr})

            # -------------------------------------GLP
            glp = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_glp_car))

            df_glp = pd.DataFrame({'GLP': glp})

            # -------------------------------------NAFTA
            nafta = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_nafta_car))

            df_nafta = pd.DataFrame({'NAFTA': nafta})

            # -------------------------------------GASOLINA
            gasolina = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_gasolina_car))

            df_gasolina = pd.DataFrame({'GASOLINA': gasolina})

            # -------------------------------------JET
            jet = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_jet_car))

            df_jet = pd.DataFrame({'JET': jet})

            # -------------------------------------DIESEL
            diesel = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_diesel_car))

            df_diesel = pd.DataFrame({'DIESEL': diesel})

            # -------------------------------------VGO
            vgo = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_vgo))

            df_vgo = pd.DataFrame({'VGO': vgo})

            # -------------------------------------SLURRY
            slurry = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_slurry))

            df_slurry = pd.DataFrame({'SLURRY': slurry})

            # -------------------------------------BREA_ASFALTO
            brea_asfalto = list(map(lambda x, y: x * y, cargas_grc_total, rendimientos_brea_asfalto))

            df_brea_asfalto = pd.DataFrame({'BREA_ASFALTO': brea_asfalto})

            df_productos_ventas_cartagena = pd.concat(
                [rango_anio, df_polipropileno_pgr, df_glp, df_nafta, df_gasolina, df_jet, df_diesel, df_vgo, df_slurry,
                 df_brea_asfalto], axis=1)

            # -------------------------------------------------------

            df_fc_down_grc = pd.merge(df_productos_ventas_cartagena, df_refinacion_cartagena, on='RANGO_ANIO')

            # -------------------------------------------------------------
            # PARAMETRICAS
            rango_anio_sobretasa = self.flujo_caja_config["parametros"]["rango_anio_sobretasa"]
            historico_proyeccion_precios = self.flujo_caja_config["parametros"]["historico_proyeccion_precios"]
            percentil_1 = self.flujo_caja_config["parametros"]["percentil_1"]
            percentil_2 = self.flujo_caja_config["parametros"]["percentil_2"]
            percentil_3 = self.flujo_caja_config["parametros"]["percentil_3"]
            intervalo_percentil = self.flujo_caja_config["parametros"]["intervalo_percentil"]
            rango_anios = self.flujo_caja_config["parametros"]["rango_anio"]
            precio_crudo = self.flujo_caja_config["parametros"]["precio_crudo"]
            precio_co2 = self.flujo_caja_config["parametros"]["precio_co2"]
            payout = self.flujo_caja_config["parametros"]["payout"]
            impuesto_renta_20 = self.flujo_caja_config["parametros"]["impuesto_renta_20"]

            diferencial_polipropileno_car = self.flujo_caja_config["parametros"]["diferencial_polipropileno_car"]
            diferencial_glp_car = self.flujo_caja_config["parametros"]["diferencial_glp_car"]
            diferencial_nafta_car = self.flujo_caja_config["parametros"]["diferencial_nafta_car"]
            diferencial_gasolina_car = self.flujo_caja_config["parametros"]["diferencial_gasolina_car"]
            diferencial_jet_car = self.flujo_caja_config["parametros"]["diferencial_jet_car"]
            diferencial_diesel_car = self.flujo_caja_config["parametros"]["diferencial_diesel_car"]
            diferencial_vgo_car = self.flujo_caja_config["parametros"]["diferencial_vgo"]
            diferencial_slurry_car = self.flujo_caja_config["parametros"]["diferencial_slurry"]
            diferencial_brea_asfalto_car = self.flujo_caja_config["parametros"]["diferencial_brea_asfalto"]

            diferencial_carga_liviano_car = self.flujo_caja_config["parametros"]["diferencial_carga_liviano_car"]
            diferencial_compra_n_liviano_car = self.flujo_caja_config["parametros"]["diferencial_compra_n_liviano_car"]
            diferencial_compra_imp_liviano_car = self.flujo_caja_config["parametros"][
                "diferencial_compra_imp_liviano_car"]
            diferencial_carga_pesado_car = self.flujo_caja_config["parametros"]["diferencial_carga_pesado_car"]
            diferencial_compra_n_pesado_car = self.flujo_caja_config["parametros"]["diferencial_compra_n_pesado_car"]
            diferencial_compra_imp_pesado_car = self.flujo_caja_config["parametros"][
                "diferencial_compra_imp_pesado_car"]
            diferencial_carga_mediano_car = self.flujo_caja_config["parametros"]["diferencial_carga_mediano_car"]
            diferencial_compra_n_mediano_car = self.flujo_caja_config["parametros"]["diferencial_compra_n_mediano_car"]
            diferencial_compra_imp_mediano_car = self.flujo_caja_config["parametros"][
                "diferencial_compra_imp_mediano_car"]
            diferencial_carga_castilla_car = self.flujo_caja_config["parametros"]["diferencial_carga_castilla_car"]
            diferencial_compra_n_castilla_car = self.flujo_caja_config["parametros"][
                "diferencial_compra_n_castilla_car"]
            diferencial_compra_imp_castilla_car = self.flujo_caja_config["parametros"][
                "diferencial_compra_imp_castilla_car"]
            tarifa_c_f_car = self.flujo_caja_config["parametros"]["tarifa_c_f_car"]
            emisiones_netas_co2_alcance_1_y_2 = activo_grc['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2']

            annio_inicio = rango_anios[0]
            annio_fin = rango_anios[1] + 1
            anios = list(range(annio_inicio, annio_fin))

            lista_anios_dias = []
            for anio in range(annio_inicio, annio_fin):
                cantidad_dias = obtener_cantidad_dias(anio)
                lista_anios_dias.append((cantidad_dias))

            annio_inicio_s = rango_anio_sobretasa[0]
            annio_fin_s = rango_anio_sobretasa[1] + 1
            anioss = list(range(annio_inicio_s, annio_fin_s))

            df_sobretasa = calcular_sobretasa(anioss, historico_proyeccion_precios, percentil_1, percentil_2,
                                              percentil_3, intervalo_percentil, df_precio_crudo, self.flujo_caja_config)

            # Asignacion precio crudo por año
            data = {'RANGO_ANIO': anios,
                    'CANT_DIAS_ANIO': lista_anios_dias,
                    'PRECIO_CRUDO_BRENT': precio_crudo,
                    'PRECIO_CO2': precio_co2,
                    'IMPUESTO_RENTA_20': impuesto_renta_20,
                    'PAYOUT': payout,
                    'DIFERENCIAL_POLIPROPILENO_PGR_CAR': diferencial_polipropileno_car,
                    'DIFERENCIAL_GLP_CAR': diferencial_glp_car,
                    'DIFERENCIAL_NAFTA_CAR': diferencial_nafta_car,
                    'DIFERENCIAL_GASOLINA_CAR': diferencial_gasolina_car,
                    'DIFERENCIAL_JET_CAR': diferencial_jet_car,
                    'DIFERENCIAL_DIESEL_CAR': diferencial_diesel_car,
                    'DIFERENCIAL_VGO_CAR': diferencial_vgo_car,
                    'DIFERENCIAL_SLURRY_CAR': diferencial_slurry_car,
                    'DIFERENCIAL_BREA_ASFALTO_CAR': diferencial_brea_asfalto_car,
                    'DIFERENCIAL_CARGA_LIVIANO_CAR': diferencial_carga_liviano_car,
                    'DIFERENCIAL_COMPRA_N_LIVIANO_CAR': diferencial_compra_n_liviano_car,
                    'DIFERENCIAL_COMPRA_IMP_LIVIANO_CAR': diferencial_compra_imp_liviano_car,
                    'DIFERENCIAL_CARGA_PESADO_CAR': diferencial_carga_pesado_car,
                    'DIFERENCIAL_COMPRA_N_PESADO_CAR': diferencial_compra_n_pesado_car,
                    'DIFERENCIAL_COMPRA_IMP_PESADO_CAR': diferencial_compra_imp_pesado_car,
                    'DIFERENCIAL_CARGA_MEDIANO_CAR': diferencial_carga_mediano_car,
                    'DIFERENCIAL_COMPRA_N_MEDIANO_CAR': diferencial_compra_n_mediano_car,
                    'DIFERENCIAL_COMPRA_IMP_MEDIANO_CAR': diferencial_compra_imp_mediano_car,
                    'DIFERENCIAL_CARGA_CASTILLA_CAR': diferencial_carga_castilla_car,
                    'DIFERENCIAL_COMPRA_N_CASTILLA_CAR': diferencial_compra_n_castilla_car,
                    'DIFERENCIAL_COMPRA_IMP_CASTILLA_CAR': diferencial_compra_imp_castilla_car,
                    'TARIFA_C_F_CAR': tarifa_c_f_car,
                    'EMISIONES_NETAS_CO2_ALCANCE_1_Y_2': emisiones_netas_co2_alcance_1_y_2
                    }

            df_anio_down_grc = pd.DataFrame(data)

            df_fc_down_grc = df_fc_down_grc.merge(df_anio_down_grc, on='RANGO_ANIO', how='left')

            df_fc_down_grc = pd.merge(df_fc_down_grc, df_sobretasa, on=['RANGO_ANIO', 'PRECIO_CRUDO_BRENT'],
                                      how='inner')

            # _-----------------------------------------------------CALCULOS_CARTAGENA_FLUJO_CAJA
            # Flujo de caja GRC
            #	POLIPROPILENO_PGR -->	Z*(Brent + Diferencial) * cantidad dias / 1000
            # ----------POLIPROPILENO_PGR
            df_fc_down_grc['ING_POLIPROPILENO_PGR'] = df_fc_down_grc['POLIPROPILENO_PGR'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_POLIPROPILENO_PGR_CAR']) * (
                                                                  df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_POLIPROPILENO_PGR'] = df_fc_down_grc['ING_POLIPROPILENO_PGR'].fillna(0)

            #	GLP --> 	AA*(Brent + Diferencial) Anual
            # --------------GLP
            df_fc_down_grc['ING_GLP'] = df_fc_down_grc['GLP'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_GLP_CAR']) * (
                                                    df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_GLP'] = df_fc_down_grc['ING_GLP'].fillna(0)

            #	NAFTA -->	AB*(Brent + Diferencial) Anual
            # -----------------NAFTA
            df_fc_down_grc['ING_NAFTA'] = df_fc_down_grc['NAFTA'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_NAFTA_CAR']) * (
                                                      df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_NAFTA'] = df_fc_down_grc['ING_NAFTA'].fillna(0)

            #	GASOLINA -->	AB*(Brent + Diferencial) Anual
            # --------------GASOLINA
            df_fc_down_grc['ING_GASOLINA'] = df_fc_down_grc['GASOLINA'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_GASOLINA_CAR']) * (
                                                         df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_GASOLINA'] = df_fc_down_grc['ING_GASOLINA'].fillna(0)

            #	JET  -->	AC*(Brent + Diferencial) Anual
            # --------------JET
            df_fc_down_grc['ING_JET'] = df_fc_down_grc['JET'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_JET_CAR']) * (
                                                    df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_JET'] = df_fc_down_grc['ING_JET'].fillna(0)

            #	DIESEL --> 	AD*(Brent + Diferencial) Anual
            # --------------DIESEL
            df_fc_down_grc['ING_DIESEL'] = df_fc_down_grc['DIESEL'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_DIESEL_CAR']) * (
                                                       df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_DIESEL'] = df_fc_down_grc['ING_DIESEL'].fillna(0)

            #	VGO --> 	AD*(Brent + Diferencial) Anual
            # --------------VGO
            df_fc_down_grc['ING_VGO'] = df_fc_down_grc['VGO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_VGO_CAR']) * (
                                                    df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_VGO'] = df_fc_down_grc['ING_VGO'].fillna(0)

            #	SLURRY --> 	AD*(Brent + Diferencial) Anual
            # --------------SLURRY
            df_fc_down_grc['ING_SLURRY'] = df_fc_down_grc['SLURRY'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_SLURRY_CAR']) * (
                                                       df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_SLURRY'] = df_fc_down_grc['ING_SLURRY'].fillna(0)

            #	BREA_ASFALTO --> 	AD*(Brent + Diferencial) Anual
            # --------------BREA_ASFALTO
            df_fc_down_grc['ING_BREA_ASFALTO'] = df_fc_down_grc['BREA_ASFALTO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] + df_fc_down_grc['DIFERENCIAL_BREA_ASFALTO_CAR']) * (
                                                             df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['ING_BREA_ASFALTO'] = df_fc_down_grc['ING_BREA_ASFALTO'].fillna(0)

            # Total Ingresos Suma
            df_fc_down_grc['TOTAL_INGRESOS'] = df_fc_down_grc['ING_POLIPROPILENO_PGR'] + df_fc_down_grc['ING_GLP'] + \
                                               df_fc_down_grc['ING_NAFTA'] + df_fc_down_grc['ING_GASOLINA'] + \
                                               df_fc_down_grc['ING_JET'] + df_fc_down_grc['ING_DIESEL'] + \
                                               df_fc_down_grc['ING_VGO'] + df_fc_down_grc['ING_SLURRY'] + \
                                               df_fc_down_grc['ING_BREA_ASFALTO']
            df_fc_down_grc['TOTAL_INGRESOS'] = df_fc_down_grc['TOTAL_INGRESOS'].fillna(0)

            # --------------------------------LIVIANOS_CARTAGENA
            # COST_LIVIANO -->	O*(Brent – Dif)
            df_fc_down_grc['COST_LIVIANO_COMPRA_IMP_LIVIANO'] = df_fc_down_grc['COMPRA_IMP_LIVIANO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_IMP_LIVIANO_CAR']) * (
                                                                            df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_LIVIANO_COMPRA_IMP_LIVIANO'] = df_fc_down_grc[
                'COST_LIVIANO_COMPRA_IMP_LIVIANO'].fillna(0)

            # ------------------------------PESADOS
            # COST_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grc['COST_PESADO'] = df_fc_down_grc['CARGA_PESADO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_CARGA_PESADO_CAR']) * (
                                                        df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_PESADO'] = df_fc_down_grc['COST_PESADO'].fillna(0)

            # COMPRA_N_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_N_PESADO'] = df_fc_down_grc['COMPRA_N_PESADO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_N_PESADO_CAR']) * (
                                                                 df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_N_PESADO'] = df_fc_down_grc['COST_COMPRA_N_PESADO'].fillna(0)

            # COMPRA_IMP_PESADO  -->	Q*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_IMP_PESADO'] = df_fc_down_grc['COMPRA_IMP_PESADO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_IMP_PESADO_CAR']) * (
                                                                   df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_IMP_PESADO'] = df_fc_down_grc['COST_COMPRA_IMP_PESADO'].fillna(0)

            # #-------------------------------MEDIANOS
            # COST_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grc['COST_MEDIANO'] = df_fc_down_grc['CARGA_MEDIANO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_CARGA_MEDIANO_CAR']) * (
                                                         df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_MEDIANO'] = df_fc_down_grc['COST_MEDIANO'].fillna(0)

            # COMPRA_N_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_N_MEDIANO'] = df_fc_down_grc['COMPRA_N_MEDIANO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_N_MEDIANO_CAR']) * (
                                                                  df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_N_MEDIANO'] = df_fc_down_grc['COST_COMPRA_N_MEDIANO'].fillna(0)

            # COMPRA_IMP_MEDIANO	--> P*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_IMP_MEDIANO'] = df_fc_down_grc['COMPRA_IMP_MEDIANO'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_IMP_MEDIANO_CAR']) * (
                                                                    df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_IMP_MEDIANO'] = df_fc_down_grc['COST_COMPRA_IMP_MEDIANO'].fillna(0)

            # --------------------------------CASTILLA
            # COST_CASTILLA -->	O*(Brent – Dif)
            df_fc_down_grc['COST_CASTILLA'] = df_fc_down_grc['CARGA_CAST'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_CARGA_CASTILLA_CAR']) * (
                                                          df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_CASTILLA'] = df_fc_down_grc['COST_CASTILLA'].fillna(0)

            # COMPRA_N_CASTILLA -->	O*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_N_CASTILLA'] = df_fc_down_grc['COMPRA_N_CAST'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc['DIFERENCIAL_COMPRA_N_CASTILLA_CAR']) * (
                                                                   df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_N_CASTILLA'] = df_fc_down_grc['COST_COMPRA_N_CASTILLA'].fillna(0)

            # COMPRA_IMP_CASTILLA -->	O*(Brent – Dif)
            df_fc_down_grc['COST_COMPRA_IMP_CASTILLA'] = df_fc_down_grc['COMPRA_IMP_CAST'] * (
                        df_fc_down_grc['PRECIO_CRUDO_BRENT'] - df_fc_down_grc[
                    'DIFERENCIAL_COMPRA_IMP_CASTILLA_CAR']) * (df_fc_down_grc['CANT_DIAS_ANIO'] / 1000)
            df_fc_down_grc['COST_COMPRA_IMP_CASTILLA'] = df_fc_down_grc['COST_COMPRA_IMP_CASTILLA'].fillna(0)

            # ---------------------------------MATERIA_PRIMA
            df_fc_down_grc['COST_MATERIA_PRIMA'] = df_fc_down_grc['COST_LIVIANO_COMPRA_IMP_LIVIANO'] + df_fc_down_grc[
                'COST_PESADO'] + df_fc_down_grc['COST_COMPRA_N_PESADO'] + df_fc_down_grc['COST_COMPRA_IMP_PESADO'] + \
                                                   df_fc_down_grc['COST_MEDIANO'] + df_fc_down_grc[
                                                       'COST_COMPRA_N_MEDIANO'] + df_fc_down_grc[
                                                       'COST_COMPRA_IMP_MEDIANO'] + df_fc_down_grc['COST_CASTILLA'] + \
                                                   df_fc_down_grc['COST_COMPRA_N_CASTILLA'] + df_fc_down_grc[
                                                       'COST_COMPRA_IMP_CASTILLA']
            df_fc_down_grc['COST_MATERIA_PRIMA'] = df_fc_down_grc['COST_MATERIA_PRIMA'].fillna(0)

            df_fc_down_grc['COST_CASTILLA'] + df_fc_down_grc['COST_COMPRA_N_CASTILLA'] + df_fc_down_grc[
                'COST_COMPRA_IMP_CASTILLA']
            df_fc_down_grc['COST_CASTILLA'] = df_fc_down_grc['COST_CASTILLA'].fillna(0)

            df_fc_down_grc['COSTOS_FIJOS'] = df_fc_down_grc['TARIFA_C_F_CAR'] * df_fc_down_grc['CARGA_TOTAL'] * \
                                             df_fc_down_grc['CANT_DIAS_ANIO'] / 1000
            df_fc_down_grc['COSTOS_FIJOS'] = df_fc_down_grc['COSTOS_FIJOS'].fillna(0)

            # df_fc_down_grc['COSTO_INTERNO_CO2'] = 0
            df_fc_down_grc['COSTO_INTERNO_CO2'] = df_fc_down_grc['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'] * df_fc_down_grc[
                'PRECIO_CO2']

            # Total Costos	Suma
            df_fc_down_grc['TOTAL_COSTOS'] = df_fc_down_grc['COST_MATERIA_PRIMA'] + df_fc_down_grc['COSTOS_FIJOS'] + \
                                             df_fc_down_grc['COSTO_INTERNO_CO2']
            df_fc_down_grc['TOTAL_COSTOS'] = df_fc_down_grc['TOTAL_COSTOS'].fillna(0)

            # BA	EBITDA	AR-AZ
            df_fc_down_grc['EBITDA'] = df_fc_down_grc['TOTAL_INGRESOS'] - df_fc_down_grc['TOTAL_COSTOS']

            # BA	EBITDA	AR-AZ
            df_fc_down_grc['EBITDA'] = df_fc_down_grc['TOTAL_INGRESOS'] - df_fc_down_grc['TOTAL_COSTOS']

            # BB	DD&A	Línea recta 20 años
            # df_fc_down['SUM_DEPRE']

            # BE	Capex --> sumatoria del Capex anual de los proyectos

            # df_capex_grc = pd.DataFrame()
            # df_capex_grc = df_plp_downstream[['ID_PROYECTO', 'MATRICULA_DIGITAL', 'RANGO_ANIO', 'GERENCIA', 'CAPEX']]
            # df_capex_grc['GERENCIA'] = df_capex_grc['GERENCIA'].str.lower()

            # gerencia_filtrar = ['grc','reficar']
            # df_capex_grc_filtro1 = df_capex_grc[df_capex_grc['MATRICULA_DIGITAL'].isin(matricula_df)]
            # df_capex_grc = df_capex_grc_filtro1[df_capex_grb_filtro1['GERENCIA'].isin(gerencia_filtrar)]

            # if df_capex_grc.empty:
            df_sum_capex_anio = pd.DataFrame(columns=["RANGO_ANIO", "CAPEX"])
            for año in anios:
                df_sum_capex_anio = df_sum_capex_anio.append({"RANGO_ANIO": año, "CAPEX": 0}, ignore_index=True)
                # else:
            #   df_sum_capex_anio = df_capex_grc.groupby('RANGO_ANIO')['CAPEX'].sum()
            #   df_sum_capex_anio = pd.DataFrame({'RANGO_ANIO': df_sum_capex_anio.index, 'CAPEX': df_sum_capex_anio.values})

            df_depreciacion_grc = calcular_depreciacion(df_sum_capex_anio)

            df_union_ppe = pd.merge(df_sum_capex_anio, df_depreciacion_grc, on=['RANGO_ANIO'], how='inner')
            df_con_ppe = calcular_ppe(df_union_ppe)
            df_fc_down_grc = pd.merge(df_fc_down_grc, df_con_ppe, on=['RANGO_ANIO'])

            # BC	Base gravable 	BA-BB --> ya esta
            df_fc_down_grc['BASE_GRABABLE'] = df_fc_down_grc['EBITDA'] + df_fc_down_grc['COSTO_INTERNO_CO2'] - \
                                              df_fc_down_grc['SUM_DEPRE']

            # BD	Impuesto BC*(20%) --> verificar
            df_fc_down_grc['IMPUESTOS'] = df_fc_down_grc['BASE_GRABABLE'] * df_fc_down_grc['IMPUESTO_RENTA_20']

            # BF	Flujo de Caja	BA-BD-BE --> ya esta
            df_fc_down_grc['FLUJO_CAJA'] = df_fc_down_grc['EBITDA'] - df_fc_down_grc['CAPEX'] - df_fc_down_grc[
                'IMPUESTOS']

            # BG	Utilidad	BA-BD --> ya esta
            df_fc_down_grc['EBIT'] = df_fc_down_grc['EBITDA'] - df_fc_down_grc['IMPUESTOS']

            # BH	Dividendos	BG*Payout%
            df_fc_down_grc['DIVIDENDO'] = df_fc_down_grc['EBIT'] * df_fc_down_grc['PAYOUT']

            # #roace
            # df_fc_down_grc['ROACE'] = df_fc_down_grc.apply(calcular_roace, axis=1)
            # df_fc_down_grc['ROACE'].replace([np.inf, -np.inf], 0, inplace=True) # Reemplaza infinito con 0 si CAPITAL_EMPLEADO es 0
            # df_fc_down_grc['ROACE'] = df_fc_down_grc["ROACE"].fillna(0)

            df_fc_down_grc.insert(0, 'MATRICULA_DIGITAL', 'grc-' + porfafolio)
            df_fc_down_grc.insert(1, 'ID_PROYECTO', '2')
            df_fc_down_grc.insert(2, 'LINEA_DE_NEGOCIO', 'Hidrocarburos')
            df_fc_down_grc.insert(3, 'OPCION_ESTRATEGICA', 'Negocio Tradicional')
            df_fc_down_grc.insert(4, 'CATEGORIA', '')
            df_fc_down_grc.insert(5, 'SUBCATEGORIA', '')
            df_fc_down_grc.insert(6, 'EMPRESA', 'RCSAS')
            df_fc_down_grc.insert(7, 'SEGMENTO', 'Downstream')
            df_fc_down_grc.insert(8, 'SUBSEGMENTO', 'Downstream')
            df_fc_down_grc.insert(9, 'UN_NEG_REGIONAL', 'Reficar')
            df_fc_down_grc.insert(10, 'GERENCIA', 'Reficar')
            df_fc_down_grc.insert(11, 'ACTIVO', 'Reficar')
            df_fc_down_grc.insert(12, 'DEPARTAMENTO', 'Bolivar')
            df_fc_down_grc.insert(13, 'PORTAFOLIO', porfafolio)
            df_fc_down_grc.insert(14, 'NOMBRE_PROYECTO', 'Operacion refineria reficar')
            df_fc_down_grc.insert(15, 'TIPO_DE_INVERSION', 'Básica')

            # unir flujos de caja de down por activo
            df_union_down_activo = pd.concat([df_fc_down_grb, df_fc_down_grc], axis=0)
            df_union_down_activo = df_union_down_activo.fillna(0)
            df_union_down_activo = df_union_down_activo.groupby('ID_PROYECTO').apply(
                calcular_duracion_proyectos).reset_index(drop=True)

            df_total = df_total.append(df_union_down_activo, ignore_index=True)

        return df_total