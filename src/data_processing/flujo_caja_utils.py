import pandas as pd
import numpy as np
import string
import random

import unicodedata

resultado_anterior = None

def union_flujos(df_union, df_mid, df_down):
    """
    Concatenate and calculate interest rate, discounted cash flow, VPN_CALCULADO, and VPN_PLP_CALCULADO.

    :param df_union: Input DataFrame for the upstream.
    :type df_union: pd.DataFrame
    :param df_mid: Input DataFrame for the midstream.
    :type df_mid: pd.DataFrame
    :param df_down: Input DataFrame for the downstream.
    :type df_down: pd.DataFrame

    :return: Concatenated DataFrame with calculated columns.
    :rtype: pd.DataFrame
    """
    resultado_concat = pd.concat([df_union, df_down, df_mid], axis=0, ignore_index=True)
    resultado_concat['TASA'] = resultado_concat.apply(calcular_tasa, axis=1)

    # --------------------------FACTOR_DCTO
    resultado_anterior = None  
    resultado_concat['FACTOR_DCTO'] = resultado_concat.apply(calcular_fc_descontado, axis=1)
    
    # ----------------------------FC_DESCONTADO
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FLUJO_CAJA'] * resultado_concat['FACTOR_DCTO']
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FC_DESCONTADO'].astype(float)

    resultado_concat['VPN_CALCULADO'] = resultado_concat.groupby('MATRICULA_DIGITAL')['FC_DESCONTADO'].transform('sum')

    resultado_concat['VPN_PLP_CALCULADO'] = resultado_concat.apply(calcular_vpn_real, axis=1)

    return resultado_concat


def get_vpn_calculado(df_union):
    """
    Calculate interest rate, discounted cash flow, VPN_CALCULADO, and VPN_PLP_CALCULADO.

    :param df_union: Input DataFrame.
    :type df_union: pd.DataFrame

    :return: DataFrame with calculated columns.
    :rtype: pd.DataFrame
    """
    resultado_concat = df_union
    resultado_concat['TASA'] = resultado_concat.apply(calcular_tasa, axis=1)
    # --------------------------FACTOR_DCTO
    resultado_anterior = None  
    resultado_concat['FACTOR_DCTO'] = resultado_concat.apply(calcular_fc_descontado, axis=1)
    
    # ----------------------------FC_DESCONTADO
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FLUJO_CAJA'] * resultado_concat['FACTOR_DCTO']
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FC_DESCONTADO'].astype(float)

    resultado_concat['VPN_CALCULADO'] = resultado_concat.groupby('MATRICULA_DIGITAL')['FC_DESCONTADO'].transform('sum')

    resultado_concat['VPN_PLP_CALCULADO'] = resultado_concat.apply(calcular_vpn_real_2, axis=1)

    return resultado_concat


def union_flujos_2(df_union, df_down):
    """
    Union two DataFrames, calculate interest rate, discounted cash flow, and VPN.

    :param df_union: First DataFrame.
    :type df_union: pd.DataFrame
    :param df_down: Second DataFrame.
    :type df_down: pd.DataFrame

    :return: Combined DataFrame with additional calculated columns.
    :rtype: pd.DataFrame
    """
    resultado_concat = pd.concat([df_union, df_down], axis=0, ignore_index=True)
    resultado_concat['TASA'] = resultado_concat.apply(calcular_tasa, axis=1)
    
    # --------------------------FACTOR_DCTO
    resultado_anterior = None 

    resultado_concat['FACTOR_DCTO'] = resultado_concat.apply(calcular_fc_descontado, axis=1)
    
    # ----------------------------FC_DESCONTADO
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FLUJO_CAJA'] * resultado_concat['FACTOR_DCTO']
    resultado_concat['FC_DESCONTADO'] = resultado_concat['FC_DESCONTADO'].astype(float)

    resultado_concat['VPN_CALCULADO'] = resultado_concat.groupby('MATRICULA_DIGITAL')['FC_DESCONTADO'].transform('sum')

    resultado_concat['VPN_PLP_CALCULADO'] = resultado_concat.apply(calcular_vpn_real, axis=1)

    return resultado_concat


def calcular_tasa(row):
    """
    Calculate the interest rate (TASA) based on specific conditions.

    :param row: DataFrame row containing columns 'LINEA_DE_NEGOCIO', 'SEGMENTO'.
    :type row: pd.Series

    :return: Calculated interest rate.
    :rtype: float
    """
    if row['LINEA_DE_NEGOCIO'] == 'Bajas Emisiones' or row['LINEA_DE_NEGOCIO'] == 'Transmisión y Vías':
        return 0.08
    elif row['SEGMENTO'] == 'Midstream':
        return 0.08
    else:
        return 0.10


def calcular_fc_descontado(row):
    """
    Calculate discounted cash flow based on specific conditions.

    :param row: DataFrame row containing columns 'TASA', 'RANGO_ANIO'.
    :type row: pd.Series

    :return: Calculated discounted cash flow.
    :rtype: float
    """
    global resultado_anterior  

    if resultado_anterior is None:
        resultado_anterior = 1 / ((1 + row['TASA']) ** -0.5)

    if row['RANGO_ANIO'] == 2024:
        resultado_actual = 1 / ((1 + row['TASA']) ** -0.5)
        resultado_anterior = resultado_actual  
        return resultado_actual
    else:
        resultado_actual = resultado_anterior / (1 + row['TASA'])
        resultado_anterior = resultado_actual


def calcular_vpn_real(row):
    """
    Calculate real Net Present Value (VPN) based on specific conditions.

    :param row: DataFrame row containing columns 'UN_NEG_REGIONAL', 'PORTAFOLIO', 'SEGMENTO', 'EVPN', 'VPN_CALCULADO', 'VPN'.
    :type row: pd.Series

    :return: Calculated real VPN.
    :rtype: float
    """
    if row['UN_NEG_REGIONAL'] == 'VEX':
        return row['EVPN']
    elif row['PORTAFOLIO'] in ['portafolio_1', 'portafolio_2', 'portafolio_3', 'portafolio_4', 'portafolio_5',
                               'portafolio_6'] or row['SEGMENTO'] in ['Transmisión Y Vías']:
        return row['VPN_CALCULADO']
    else:
        return row['VPN']


def calcular_vpn_real_2(row):
    """
    Calculate an alternative real Net Present Value (VPN) based on specific conditions.

    :param row: DataFrame row containing columns 'UN_NEG_REGIONAL', 'SEGMENTO', 'EVPN', 'VPN_CALCULADO', 'VPN'.
    :type row: pd.Series

    :return: Calculated real VPN.
    :rtype: float
    """
    if row['UN_NEG_REGIONAL'] == 'VEX':
        return row['EVPN']
    elif row['SEGMENTO'] in ['Transmisión Y Vías']:
        return row['VPN_CALCULADO']
    else:
        return row['VPN']

  
def normalize_column_name(column_name):
    """
    Normalize the column name based on the following rules:
    1. Convert to uppercase.
    2. Replace spaces with underscores.
    3. Replace ">" with "_GT".
    4. Replace "<" with "_LT".
    5. Remove parentheses.
    6. Remove diacritical marks, like accents.
    7. Remove non-alphanumeric characters except for underscores.
    8. If the column name starts with an underscore, remove it.
    9. If the column name has two consecutive underscores, remove one.
    10. If the column name ends with an underscore, remove it.

    :param column_name: Original name of the column.
    :type column_name: str
    :return: Normalized column name.
    :rtype: str
    """
    if not (isinstance(column_name, int)):
        column_name = column_name.upper()
        column_name = column_name.replace(" ", "_")
        column_name = column_name.replace(">", "_GT")
        column_name = column_name.replace("<", "_LT")
        column_name = column_name.replace("(", "").replace(")", "")
        column_name = unicodedata.normalize('NFKD', column_name).encode('ASCII', 'ignore').decode('ASCII')
        column_name = ''.join(e for e in column_name if e.isalnum() or e == '_')

        if column_name.startswith("_"):
            column_name = column_name[1:]

        column_name = column_name.replace("__", "_")

        if column_name.endswith("_"):
            column_name = column_name[:-1]

    return column_name


# --------------------------------- upstream

def expand_row(row, start_value, end_value):
    """
    Expand a single row into a DataFrame with a range of years.

    :param row: Dictionary representing a single row.
    :type row: dict
    :param start_value: The starting value for the 'RANGO_ANIO' column.
    :type start_value: int
    :param end_value: The ending value for the 'RANGO_ANIO' column.
    :type end_value: int

    :return: DataFrame with the expanded row and a range of years.
    :rtype: pd.DataFrame
    """
    return pd.DataFrame({**row, 'RANGO_ANIO': range(start_value, end_value)})


def asignar_valor_up(row):
    """
    Assign a value to the 'MATRICULA_DIGITAL' column based on a conditional check.

    :param row: DataFrame row representing a single record.
    :type row: pd.Series

    :return: Value for the 'MATRICULA_DIGITAL' column.
    :rtype: str
    """
    if pd.isna(row['MATRICULA_DIGITAL']):
        return 'UP-' + str(row.name)
    else:
        return row['MATRICULA_DIGITAL']


def peep_update_values_in_sabana_datos(peep_columns, df_sabana_datos, df_peep, new_column_name, annio_inicio,
                                       annio_fin):
    """
    Update values in the DataFrame df_sabana_datos based on data from df_peep.

    :param peep_columns: List of column names in df_peep to use for updating.
    :type peep_columns: list
    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_peep: The DataFrame containing data to update df_sabana_datos.
    :type df_peep: pd.DataFrame
    :param new_column_name: The name of the new column to store updated values.
    :type new_column_name: str
    :param annio_inicio: The starting year for updating.
    :type annio_inicio: int
    :param annio_fin: The ending year for updating.
    :type annio_fin: int

    :return: The updated df_sabana_datos DataFrame.
    :rtype: pd.DataFrame
    """
    df_peep_melted = df_peep.melt(id_vars="Nombre", var_name="FIELD", value_name="temp_value")
    range_anio = [str(year) for year in range(int(annio_inicio), int(annio_fin))]
    df_peep_melted = df_peep_melted[df_peep_melted["Nombre"].astype(str).isin(range_anio)]
    merged_campo = df_sabana_datos.merge(df_peep_melted, left_on=["CAMPO", "RANGO_ANIO"], right_on=["FIELD", "Nombre"], how="inner")
    df_sabana_datos[new_column_name] = merged_campo["temp_value"]
    merged_activo = df_sabana_datos.merge(df_peep_melted, left_on=["ACTIVO", "RANGO_ANIO"], right_on=["FIELD", "Nombre"], how="inner")
    df_sabana_datos.loc[merged_activo.index, new_column_name] = merged_activo["temp_value"]

    return df_sabana_datos

             
def peep_update_values_in_sabana_datos_hocol(peep_columns, df_sabana_datos, df_peep, column_name_update, annio_inicio,
                                             annio_fin):
    """
    Update values in the DataFrame df_sabana_datos for the company 'HOCOL' based on data from df_peep.

    :param peep_columns: List of column names in df_peep to use for updating.
    :type peep_columns: list
    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_peep: The DataFrame containing data to update df_sabana_datos.
    :type df_peep: pd.DataFrame
    :param column_name_update: The name of the column to store updated values.
    :type column_name_update: str
    :param annio_inicio: The starting year for updating.
    :type annio_inicio: int
    :param annio_fin: The ending year for updating.
    :type annio_fin: int

    :return: The updated df_sabana_datos DataFrame.
    :rtype: pd.DataFrame
    """

    df_peep_filtered = df_peep[df_peep['Nombre'].astype(str).isin(map(str, range(int(annio_inicio), int(annio_fin))))]
    df_peep_melted = df_peep_filtered.melt(id_vars="Nombre", var_name="FIELD", value_name="temp_value")
    range_anio = [str(year) for year in range(int(annio_inicio), int(annio_fin))]
    df_peep_melted = df_peep_melted[df_peep_melted["Nombre"].astype(str).isin(range_anio)]
    df_sabana_datos_hocol = df_sabana_datos
    merged_campo = df_sabana_datos_hocol.merge(df_peep_melted, left_on=["CAMPO", "RANGO_ANIO"], right_on=["FIELD", "Nombre"], how="left")
    conditions_campo = (df_sabana_datos['EMPRESA'] == "HOCOL") & (df_sabana_datos['RANGO_ANIO'] == merged_campo['RANGO_ANIO']) & (df_sabana_datos['CAMPO'] == merged_campo['CAMPO'])
    df_sabana_datos.loc[conditions_campo, column_name_update] = merged_campo.loc[conditions_campo, 'temp_value']
    merged_activo = df_sabana_datos_hocol.merge(df_peep_melted, left_on=["ACTIVO", "RANGO_ANIO"],right_on=["FIELD", "Nombre"], how="left")
    conditions_activo = (df_sabana_datos['EMPRESA'] == "HOCOL") & (df_sabana_datos['RANGO_ANIO'] == merged_activo['RANGO_ANIO']) & (df_sabana_datos['ACTIVO'] == merged_activo['ACTIVO'])
    df_sabana_datos.loc[conditions_activo, column_name_update] = merged_activo.loc[conditions_activo, 'temp_value']

    return df_sabana_datos


def peep_update_values_in_sabana_datos_codigo(peep_columns, df_sabana_datos, df_peep, column_name_update, column_revisar, annio_inicio, annio_fin):
    """
    Update values in the DataFrame df_sabana_datos based on data from df_peep with 'Código' as the key.

    :param peep_columns: List of column names in df_peep to use for updating.
    :type peep_columns: list
    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_peep: The DataFrame containing data to update df_sabana_datos.
    :type df_peep: pd.DataFrame
    :param column_name_update: The name of the column to store updated values.
    :type column_name_update: str
    :param column_revisar: The column in df_sabana_datos to compare with 'Código' in df_peep.
    :type column_revisar: str
    :param annio_inicio: The starting year for updating.
    :type annio_inicio: int
    :param annio_fin: The ending year for updating.
    :type annio_fin: int

    :return: The updated df_sabana_datos DataFrame.
    :rtype: pd.DataFrame
    """
    df_peep_filtered = df_peep[df_peep['Código'].astype(str).isin(map(str, range(int(annio_inicio), int(annio_fin))))]
    df_peep_melted = df_peep_filtered.melt(id_vars="Código", var_name="FIELD", value_name="temp_value")
    range_year = [str(year) for year in range(int(annio_inicio), int(annio_fin))]
    df_peep_melted = df_peep_melted[df_peep_melted["Código"].astype(str).isin(range_year)]
    df_sabana_datos_hocol = df_sabana_datos  
    merged_campo = df_sabana_datos_hocol.merge(df_peep_melted, left_on=[column_revisar, "RANGO_ANIO"], right_on=["FIELD", "Código"], how="left") 
    conditions = (df_sabana_datos['RANGO_ANIO'] == merged_campo['Código']) & (df_sabana_datos[column_revisar] == merged_campo[column_revisar])
    df_sabana_datos.loc[conditions, column_name_update] = merged_campo.loc[conditions, 'temp_value']

    return df_sabana_datos


def peep_update_values_in_sabana_datos_hocol_codigo(peep_columns, df_sabana_datos, df_peep, column_name_update, column_revisar, annio_inicio, annio_fin):
    """
    Update values in the DataFrame df_sabana_datos for the company 'HOCOL' based on data from df_peep with 'Código' as the key.

    :param peep_columns: List of column names in df_peep to use for updating.
    :type peep_columns: list
    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_peep: The DataFrame containing data to update df_sabana_datos.
    :type df_peep: pd.DataFrame
    :param column_name_update: The name of the column to store updated values.
    :type column_name_update: str
    :param column_revisar: The column in df_sabana_datos to compare with 'Código' in df_peep.
    :type column_revisar: str
    :param annio_inicio: The starting year for updating.
    :type annio_inicio: int
    :param annio_fin: The ending year for updating.
    :type annio_fin: int

    :return: The updated df_sabana_datos DataFrame.
    :rtype: pd.DataFrame
    """

    df_peep_filtered = df_peep[df_peep['Código'].astype(str).isin(map(str, range(int(annio_inicio), int(annio_fin))))]
    df_peep_melted = df_peep_filtered.melt(id_vars="Código", var_name="FIELD", value_name="temp_value")
    range_year = [str(year) for year in range(int(annio_inicio), int(annio_fin))]
    df_peep_melted = df_peep_melted[df_peep_melted["Código"].astype(str).isin(range_year)]
    df_sabana_datos_hocol = df_sabana_datos
    merged_campo = df_sabana_datos_hocol.merge(df_peep_melted, left_on=[column_revisar, "RANGO_ANIO"], right_on=["FIELD", "Código"], how="left")    
    conditions = (df_sabana_datos[column_name_update] == 0) & (df_sabana_datos['EMPRESA'] == "HOCOL" ) & (df_sabana_datos['RANGO_ANIO'] == merged_campo['Código']) & (df_sabana_datos[column_revisar] == merged_campo[column_revisar])    
    df_sabana_datos.loc[conditions, column_name_update] = merged_campo.loc[conditions, 'temp_value']

    return df_sabana_datos


def peep_gas_values_in_sabana_datos(peep_columns, df_sabana_datos, df_peep, new_column_name, annio_inicio, annio_fin):
    """
    Update gas-related values in the DataFrame df_sabana_datos based on data from df_peep.

    :param peep_columns: List of column names in df_peep to use for updating.
    :type peep_columns: list
    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_peep: The DataFrame containing data to update df_sabana_datos.
    :type df_peep: pd.DataFrame
    :param new_column_name: The name of the new column to store updated values.
    :type new_column_name: str
    :param annio_inicio: The starting year for updating.
    :type annio_inicio: int
    :param annio_fin: The ending year for updating.
    :type annio_fin: int

    :return: The updated df_sabana_datos DataFrame.
    :rtype: pd.DataFrame
    """
    df_peep_filtered = df_peep[df_peep['Product'].astype(str).isin(map(str, range(int(annio_inicio), int(annio_fin))))]

    df_peep_melted = df_peep_filtered.melt(id_vars="Product", var_name="FIELD", value_name="temp_value")

    range_year = [str(year) for year in range(int(annio_inicio), int(annio_fin))]
    df_peep_melted = df_peep_melted[df_peep_melted["Product"].astype(str).isin(range_year)]

    merged_campo = df_sabana_datos.merge(df_peep_melted, left_on=["CAMPO", "RANGO_ANIO"], right_on=["FIELD", "Product"], how="left")
    df_sabana_datos["temp_campo"] = merged_campo["temp_value"]

    df_peep_melted.rename(columns={"FIELD": "ACTIVO"}, inplace=True)

    merged_activo = df_sabana_datos.merge(df_peep_melted, left_on=["ACTIVO", "RANGO_ANIO"], right_on=["ACTIVO", "Product"], how="left")
    df_sabana_datos["temp_activo"] = merged_activo["temp_value"]

    df_sabana_datos[new_column_name] = df_sabana_datos["temp_campo"].combine_first(df_sabana_datos["temp_activo"])

    df_peep_melted.rename(columns={"ACTIVO": "COD_PRECIO_GAS"}, inplace=True)
    merged_cod_precio_gas = df_sabana_datos.merge(df_peep_melted, left_on=["COD_PRECIO_GAS", "RANGO_ANIO"], right_on=["COD_PRECIO_GAS", "Product"], how="left")
    df_sabana_datos['PRECIO_GAS'] = merged_cod_precio_gas["temp_value"]

    df_sabana_datos.drop(columns=["temp_campo", "temp_activo"], inplace=True)

    return df_sabana_datos


def plpl_add_new_column_in_sabana_datos(df_plp_upstream, df_sabana_datos, columnas_agregar_capex, nombre_columna_capex, annio_inicio, annio_fin):
    """
    Add a new column with capital expenditure values from df_plp_upstream to the df_sabana_datos DataFrame.

    :param df_plp_upstream: The DataFrame containing capital expenditure data.
    :type df_plp_upstream: pd.DataFrame
    :param df_sabana_datos: The DataFrame to which the new column will be added.
    :type df_sabana_datos: pd.DataFrame
    :param columnas_agregar_capex: List of column names in df_plp_upstream to add to df_sabana_datos.
    :type columnas_agregar_capex: list
    :param nombre_columna_capex: The name of the new column to store capital expenditure values.
    :type nombre_columna_capex: str
    :param annio_inicio: The starting year for adding the new column.
    :type annio_inicio: int
    :param annio_fin: The ending year for adding the new column.
    :type annio_fin: int

    :return: The df_sabana_datos DataFrame with the new column added.
    :rtype: pd.DataFrame
    """
    df_add = df_plp_upstream[columnas_agregar_capex]

    df_add.columns = list(range(annio_inicio, annio_fin))

    df_add['ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transposed = df_add.set_index('ID_PROYECTO').transpose()

    df_new = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_capex)
    df_new.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_datos = pd.concat([df_sabana_datos, df_new], axis=1)

    return df_sabana_datos


def calculate_factor_dilucion(df_sabana_datos, df_dilucion):
    """
    Calculate and update the 'FACTOR_DILUCION' column in the df_sabana_datos DataFrame based on dilution data in df_dilucion.

    :param df_sabana_datos: The DataFrame to be updated.
    :type df_sabana_datos: pd.DataFrame
    :param df_dilucion: The DataFrame containing dilution data.
    :type df_dilucion: pd.DataFrame

    :return: The updated df_sabana_datos DataFrame with the 'FACTOR_DILUCION' column.
    :rtype: pd.DataFrame
    """
    max_dilucion_campo = df_dilucion.groupby('campo')['dilucion'].max()
    df_sabana_datos['FACTOR_DILUCION'] = df_sabana_datos['CAMPO'].map(max_dilucion_campo)

    missing_indices = df_sabana_datos['FACTOR_DILUCION'].isna()

    if missing_indices.any():
        max_dilucion_activo = df_dilucion.groupby('campo')['dilucion'].max()
        df_sabana_datos.loc[missing_indices, 'FACTOR_DILUCION'] = df_sabana_datos.loc[missing_indices, 'ACTIVO'].map(max_dilucion_activo)

    return df_sabana_datos


def calcular_percentiles(anios, valores, percentil_1, percentil_2, percentil_3, intervalo_percentil):
    """
    Calculate percentiles for a given range of years and values.

    :param anios: List of years.
    :type anios: list
    :param valores: List of values corresponding to each year.
    :type valores: list
    :param percentil_1: The value of the first percentile (0 to 1).
    :type percentil_1: float
    :param percentil_2: The value of the second percentile (0 to 1).
    :type percentil_2: float
    :param percentil_3: The value of the third percentile (0 to 1).
    :type percentil_3: float
    :param intervalo_percentil: The size of the interval for calculating percentiles.
    :type intervalo_percentil: int

    :return: DataFrame with calculated percentiles for each interval of years.
    :rtype: pd.DataFrame
    """
    intervalo = intervalo_percentil
    resultados = []

    for i in range(len(anios) - intervalo + 1):
        anio_inicial = anios[i]
        anio_final = anios[i + intervalo - 1]
        valores_intervalo = valores[i:i + intervalo]

        p1 = percentil_exc(valores_intervalo, percentil_1)
        p2 = percentil_exc(valores_intervalo, percentil_2)
        p3 = percentil_exc(valores_intervalo, percentil_3)

        resultados.append([anio_final + 1, p1, p2, p3])

    df = pd.DataFrame(resultados, columns=["RANGO_ANIO", "PERCENTIL_1", "PERCENTIL_2", "PERCENTIL_3"])

    return df


def percentil_exc(data, percentil):
    """
    Calculate the exclusive percentile of a dataset.

    :param data: List or array of numerical values.
    :type data: list or np.array
    :param percentil: The percentile to calculate (0 to 1).
    :type percentil: float

    :return: The calculated percentile value.
    :rtype: float or str
    """
    if not 0 < percentil < 1:
        return "#NUM!"

    data_sorted = sorted(data)
    rank = percentil * (len(data) + 1) - 1

    if rank < 0:
        return data_sorted[0]
    elif rank >= len(data) - 1:
        return data_sorted[-1]
    else:
        low = int(np.floor(rank))
        high = int(np.ceil(rank))
        return data_sorted[low] + (data_sorted[high] - data_sorted[low]) * (rank - low)


def calcular_sobretasa(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil, df_precio_crudo, flujo_caja_config):
    """
    Calculate surcharge based on crude oil prices percentiles.

    :param anios: List of years.
    :type anios: list
    :param historico_proyeccion_precios: List of historical and projected crude oil prices.
    :type historico_proyeccion_precios: list
    :param percentil_1: Percentile for the first threshold.
    :type percentil_1: float
    :param percentil_2: Percentile for the second threshold.
    :type percentil_2: float
    :param percentil_3: Percentile for the third threshold.
    :type percentil_3: float
    :param intervalo_percentil: Interval for calculating percentiles.
    :type intervalo_percentil: int
    :param df_precio_crudo: DataFrame with crude oil prices.
    :type df_precio_crudo: pd.DataFrame
    :param flujo_caja_config: Configuration parameters for cash flow.
    :type flujo_caja_config: dict

    :return: DataFrame with calculated surcharge for each year.
    :rtype: pd.DataFrame
    """
    df_percentiles = calcular_percentiles(anios, historico_proyeccion_precios, percentil_1, percentil_2, percentil_3, intervalo_percentil)
    
    df_percentiles = df_percentiles.drop(df_percentiles.index[-1])

    df_union = pd.merge(df_percentiles, df_precio_crudo, on='RANGO_ANIO', how='inner')

    percentil_menor_30 = flujo_caja_config["parametros"]["percentil_menor_30"]
    percentil_entre_30_a_45 = flujo_caja_config["parametros"]["percentil_entre_30_a_45"]
    percentil_entre_45_a_60 = flujo_caja_config["parametros"]["percentil_entre_45_a_60"]
    percentil_mayor_60 = flujo_caja_config["parametros"]["percentil_mayor_60"]  

    data = {'RANGO_ANIO': [], 'PRECIO_CRUDO_BRENT': [], 'SOBRETASA': []}
    df_resultados = pd.DataFrame(data)

    for index, row in df_union.iterrows():
        percentil_1 = row['PERCENTIL_1']
        percentil_2 = row['PERCENTIL_2']
        percentil_3 = row['PERCENTIL_3']
        precio = row['PRECIO_CRUDO_BRENT']

        if precio <= percentil_1:
            sobretasa = percentil_menor_30
        elif precio <= percentil_2:
            sobretasa = percentil_entre_30_a_45
        elif precio <= percentil_3:
            sobretasa = percentil_entre_45_a_60
        else:
            sobretasa = percentil_mayor_60

        df_resultados.loc[row['RANGO_ANIO']] = [row['RANGO_ANIO'], precio, sobretasa]

    return df_resultados


def es_bisiesto(anio):
    """
    Check if a year is a leap year.

    :param anio: Year to check.
    :type anio: int

    :return: True if the year is a leap year, False otherwise.
    :rtype: bool
    """
    return anio % 4 == 0 and (anio % 100 != 0 or anio % 400 == 0)


def obtener_cantidad_dias(anio):
    """
    Get the number of days in a year.

    :param anio: Year.
    :type anio: int

    :return: Number of days in the year.
    :rtype: int
    """
    return 366 if es_bisiesto(anio) else 365


def calcular_sobretasa_row(row):
    """
    Calculate the surcharge for a row based on certain conditions.

    :param row: DataFrame row containing 'PROD_GAS_RISKED' and 'SOBRETASA' columns.
    :type row: pd.Series

    :return: Calculated surcharge value.
    :rtype: float
    """
    if row['PROD_GAS_RISKED'] > 0:
        return 0
    return row['SOBRETASA']


def calcular_ingresos_crudo(row):
    """
    Calculate crude oil revenues based on specific conditions.

    :param row: DataFrame row containing 'EMPRESA', 'PRECIO_CRUDO_INTERNACIONAL', 'VOLUMEN_CRUDO_DESPUES_R',
                'PRECIO_CRUDO_BRENT', and 'DESCUENTO_CALIDAD' columns.
    :type row: pd.Series

    :return: Calculated crude oil revenues.
    :rtype: float
    """
    if row['EMPRESA'] in ['Ecopetrol Permian', 'Ecopetrol América Inc']:
        return row['PRECIO_CRUDO_INTERNACIONAL'] * row['VOLUMEN_CRUDO_DESPUES_R']
    else:
        return (row['PRECIO_CRUDO_BRENT'] + row['DESCUENTO_CALIDAD']) * row['VOLUMEN_CRUDO_DESPUES_R']


def agregar_prefijo_columnas(dataframe, prefijo):
    """
    Add a prefix to all column names in a DataFrame.

    :param dataframe: Input DataFrame.
    :type dataframe: pd.DataFrame
    :param prefijo: Prefix to add to column names.
    :type prefijo: str

    :return: DataFrame with column names prefixed.
    :rtype: pd.DataFrame
    """
    columnas_actuales = dataframe.columns
    nuevos_nombres = {col: prefijo + str(col) for col in columnas_actuales}
    dataframe.rename(columns=nuevos_nombres, inplace=True)

    return dataframe


def procesar_amortizacion_depreciacion(df_data):
    """
    Process amortization and depreciation based on input DataFrame.

    :param df_data: Input DataFrame containing columns 'RANGO_ANIO', 'PROD_ACUM', 'CAPEX',
                    and 'TOTAL_VOLUMENES_PROD_DESPUES_R'.
    :type df_data: pd.DataFrame

    :return: DataFrames for amortization factor, depreciation, and sum of depreciation.
    :rtype: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
    """
    rango_anio = df_data['RANGO_ANIO']

    df_factor_amortizacion = pd.DataFrame(index=rango_anio, columns=rango_anio)
    df_depreciacion = pd.DataFrame(index=rango_anio, columns=rango_anio)
    df_depreciacion = df_depreciacion.fillna(0)

    for anio in rango_anio:
        prod_acum = df_data[df_data['RANGO_ANIO'] == anio]['PROD_ACUM'].values[0]
        capex = df_data[df_data['RANGO_ANIO'] == anio]['CAPEX'].values[0]

        factor = df_data[df_data['RANGO_ANIO'] >= anio]['TOTAL_VOLUMENES_PROD_DESPUES_R'] / prod_acum
        df_factor_amortizacion.loc[anio, anio:] = factor.values

        depreciacion = factor * capex
        df_depreciacion.loc[anio, anio:] = depreciacion.values

    df_sum_columnas = df_depreciacion.sum(axis=0)
    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_factor_amortizacion = agregar_prefijo_columnas(df_factor_amortizacion, 'FAC_AMORT_')
    df_depreciacion = agregar_prefijo_columnas(df_depreciacion, 'VLR_A_AMORTIZAR_')

    return df_factor_amortizacion, df_depreciacion, sum_df


def calcular_ppe(df):
    """
    Calculate Property, Plant, and Equipment (PPE) based on input DataFrame.

    :param df: Input DataFrame containing 'CAPEX' column.
    :type df: pd.DataFrame

    :return: DataFrame with added 'CAPITAL_EMPLEADO' column representing PPE.
    :rtype: pd.DataFrame
    """
    df['CAPITAL_EMPLEADO'] = df['CAPEX'].cumsum() / (df.index + 1)
    return df


def procesar_grupo(grupo):
    """
    Process a group by calculating cumulative production, amortization, depreciation, and PPE.

    :param grupo: Input DataFrame for a specific group.
    :type grupo: pd.DataFrame

    :return: DataFrame with added columns for cumulative production, amortization, depreciation, and PPE.
    :rtype: pd.DataFrame
    """
    grupo['PROD_ACUM'] = grupo['TOTAL_VOLUMENES_PROD_DESPUES_R'][::-1].cumsum()[::-1]
    df_amortizacion, df_depreciacion, df_sum_amor_depre = procesar_amortizacion_depreciacion(grupo)
    
    df_union = pd.concat([df_amortizacion, df_depreciacion], axis='columns')
    merged_df = pd.merge(grupo, df_union, on='RANGO_ANIO', how='inner')
    merged_df1 = pd.merge(merged_df, df_sum_amor_depre, on='RANGO_ANIO', how='inner')
    
    return calcular_ppe(merged_df1)


def calcular_impuestos(row):
    """
    Calculate taxes based on input row values.

    :param row: DataFrame row containing columns 'EMPRESA', 'UN_NEG_REGIONAL', 'PROD_GAS_RISKED',
                'BASE_GRABABLE', 'IMPUESTO_RENTA_20', 'SOBRETASA', 'IMPUESTO_INTERNACIONAL', 'IMPUESTO_RENTA'.
    :type row: pd.Series

    :return: Calculated tax amount.
    :rtype: float
    """
    if row['EMPRESA'] in ['ECAS']:
        return row['BASE_GRABABLE'] * (row['IMPUESTO_RENTA_20'] + row['SOBRETASA'])
    elif row['UN_NEG_REGIONAL'] in ['PERMIAN']:
        return row['BASE_GRABABLE'] * row['IMPUESTO_INTERNACIONAL']
    elif row['PROD_GAS_RISKED'] > 0:
        return row['BASE_GRABABLE'] * (row['IMPUESTO_RENTA'])
    else:
        return row['BASE_GRABABLE'] * (row['IMPUESTO_RENTA'] + row['SOBRETASA'])


def calcular_duracion_proyectos(grupo):
    """
    Calculate the duration of projects within a group based on the non-zero cash flow.

    Args:
        grupo (pd.DataFrame): DataFrame containing relevant columns for project duration calculation.

    Returns:
        pd.DataFrame: DataFrame with added columns 'ANIO_INICIAL', 'ANIO_FINAL', and 'DURACION' representing the
                      start year, end year, and duration of projects, respectively.

    Example:
        proyecto_duracion = calcular_duracion_proyectos(grupo_data)
    """
    mask = grupo['FLUJO_CAJA'] != 0

    if not mask.any():
        grupo['ANIO_INICIAL'] = None
        grupo['ANIO_FINAL'] = None
        grupo['DURACION'] = None
        return grupo

    anio_inicial = grupo.loc[mask, 'RANGO_ANIO'].min()
    anio_final = grupo.loc[mask, 'RANGO_ANIO'].max()

    grupo['ANIO_INICIAL'] = anio_inicial
    grupo['ANIO_FINAL'] = anio_final
    grupo['DURACION'] = anio_final - anio_inicial + 1

    return grupo


# --------------------------- bajas_emisiones

def asignar_valor_be(row):
    """
    Assigns values to a new column 'MATRICULA_DIGITAL' based on conditions.

    :param row: Row in the DataFrame.
    :type row: pd.Series

    :return: Assigned value for 'MATRICULA_DIGITAL'.
    :rtype: str
    """
    if pd.isna(row['MATRICULA_DIGITAL']):
        return 'BE-' + str(row.name)
    else:
        return row['MATRICULA_DIGITAL']


def plpl_add_new_column_in_sabana_datos_bajas_emisiones(df_baja_emisiones, df_sabana_emisiones, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column with values from 'df_baja_emisiones' to 'df_sabana_emisiones' for the specified range of years.

    :param df_baja_emisiones: DataFrame containing data for the 'bajas_emisiones' segment.
    :type df_baja_emisiones: pd.DataFrame
    :param df_sabana_emisiones: DataFrame representing the 'sabana_emisiones' segment.
    :type df_sabana_emisiones: pd.DataFrame
    :param lista_columnas: List of columns to be added from 'df_baja_emisiones' to 'df_sabana_emisiones'.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column to be added in 'df_sabana_emisiones'.
    :type nombre_columna_nueva: str
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame 'df_sabana_emisiones' with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    df_add = df_baja_emisiones[lista_columnas]

    df_add.columns = range_anio
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transposed = df_add.set_index('ID_PROYECTO').transpose()

    df_new = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    df_new.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_emisiones = pd.concat([df_sabana_emisiones, df_new], axis=1)
    return df_sabana_emisiones


def calcular_depreciacion_hidrogeno(dataframe, porcentaje_depreciacion_anio, annio_inicio, annio_fin):
    """
    Calculate depreciation values for a hydrogen project over a specified range of years.

    :param dataframe: DataFrame containing project data.
    :type dataframe: pd.DataFrame
    :param porcentaje_depreciacion_anio: Percentage of annual depreciation.
    :type porcentaje_depreciacion_anio: float
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame with calculated depreciation values for each project and year.
    :rtype: pd.DataFrame
    """
    result = pd.DataFrame(columns=['ID_PROYECTO', 'RANGO_ANIO', 'SUM_DEPRE', 'CAPEX_TOTAL_ANIO'])

    for index, row in dataframe.iterrows():
        id_proyecto = row['ID_PROYECTO']
        capex_total = row['SUMA_CAPEX']
        anio_inicio = row['ANIO_PRODUCCION']

        for year in range(annio_inicio, annio_fin):
            if year < anio_inicio or year > anio_inicio + 2:
                valor_depreciado = 0
                capex_total_anio = 0
            elif year == anio_inicio:
                valor_depreciado = capex_total / porcentaje_depreciacion_anio
                capex_total_anio = row['SUMA_CAPEX']
            else:
                valor_depreciado = capex_total / porcentaje_depreciacion_anio
                capex_total_anio = 0

            result = result.append({'ID_PROYECTO': id_proyecto, 'RANGO_ANIO': year, 'SUM_DEPRE': valor_depreciado,
                                    'CAPEX_TOTAL_ANIO': capex_total_anio}, ignore_index=True)

    return result


def calcular_depreciacion_ccus(df, porcentaje_depreciacion_anio, annio_inicio, annio_fin):
    """
    Calculate depreciation values for CCUS projects over a specified range of years.

    :param df: DataFrame containing CCUS project data.
    :type df: pd.DataFrame
    :param porcentaje_depreciacion_anio: Percentage of annual depreciation.
    :type porcentaje_depreciacion_anio: float
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame with calculated depreciation values for each CCUS project and year.
    :rtype: pd.DataFrame
    """
    result = pd.DataFrame(columns=['ID_PROYECTO', 'RANGO_ANIO', 'SUM_DEPRE'])

    for index, row in df.iterrows():
        id_proyecto = row['ID_PROYECTO']
        capex_total = row['CAPEX']
        anio_inicio = row['ANIO_EMISIONES']

        for year in range(annio_inicio, annio_fin):
            if year < anio_inicio or year > anio_inicio + 2:
                valor_depreciado = 0
            elif year == anio_inicio:
                valor_depreciado = capex_total / porcentaje_depreciacion_anio
            else:
                valor_depreciado = capex_total / porcentaje_depreciacion_anio

            result = result.append({'ID_PROYECTO': id_proyecto, 'RANGO_ANIO': year, 'SUM_DEPRE': valor_depreciado}, ignore_index=True)

    return result


def calcular_depreciacion_energia(dataframe, porcentaje_depreciacion_anio, annio_inicio, annio_fin):
    """
    Calculate depreciation values for energy-related projects over a specified range of years.

    :param dataframe: DataFrame containing energy-related project data.
    :type dataframe: pd.DataFrame
    :param porcentaje_depreciacion_anio: Percentage of annual depreciation.
    :type porcentaje_depreciacion_anio: float
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame with calculated depreciation values for each energy-related project and year.
    :rtype: pd.DataFrame
    """
    result = pd.DataFrame(columns=['ID_PROYECTO', 'RANGO_ANIO', 'SUM_DEPRE', 'CAPEX_TOTAL_ANIO'])

    for index, row in dataframe.iterrows():
        id_proyecto = row['ID_PROYECTO']
        capex_total = row['SUMA_CAPEX']
        anio_inicio = row['ANIO_ENERGIA']

        for year in range(annio_inicio, annio_fin):
            if year < anio_inicio or year > anio_inicio + 2:
                valor_depreciado = 0
                capex_total_anio = 0
            elif year == anio_inicio:
                valor_depreciado = capex_total * porcentaje_depreciacion_anio
                capex_total_anio = row['SUMA_CAPEX']
            else:
                valor_depreciado = capex_total * porcentaje_depreciacion_anio
                capex_total_anio = 0

            result = result.append({'ID_PROYECTO': id_proyecto, 'RANGO_ANIO': year, 'SUM_DEPRE': valor_depreciado, 'CAPEX_TOTAL_ANIO': capex_total_anio}, ignore_index=True)

    return result


# ------------------------transmision_vias

def plpl_add_new_column_in_sabana_transmision(df_trans_vias, df_sabana_transmision, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the 'df_sabana_transmision' DataFrame based on data from 'df_trans_vias'.

    :param df_trans_vias: DataFrame containing data for transmission projects.
    :type df_trans_vias: pd.DataFrame
    :param df_sabana_transmision: DataFrame representing the 'df_sabana_transmision' to which the new column will be added.
    :type df_sabana_transmision: pd.DataFrame
    :param lista_columnas: List of column names from 'df_trans_vias' to be used for the new column.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column to be added to 'df_sabana_transmision'.
    :type nombre_columna_nueva: str
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame 'df_sabana_transmision' with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    df_add = df_trans_vias[lista_columnas]

    df_add.columns = range_anio
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transpuesto = df_add.set_index('ID_PROYECTO').transpose()

    df_nuevo = df_transpuesto.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    df_nuevo.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_transmision = pd.concat([df_sabana_transmision, df_nuevo], axis=1)
    return df_sabana_transmision


# ----------------------icos_downstream

def plpl_add_new_column_in_sabana_datos_icos_downstream(df_plp_downstream, df_sabana_downstream, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the 'df_sabana_downstream' DataFrame based on data from 'df_plp_downstream'.

    :param df_plp_downstream: DataFrame containing data for downstream projects.
    :type df_plp_downstream: pd.DataFrame
    :param df_sabana_downstream: DataFrame representing the 'df_sabana_downstream' to which the new column will be added.
    :type df_sabana_downstream: pd.DataFrame
    :param lista_columnas: List of column names from 'df_plp_downstream' to be used for the new column.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column to be added to 'df_sabana_downstream'.
    :type nombre_columna_nueva: str
    :param annio_inicio: Start year for the range of years.
    :type annio_inicio: int
    :param annio_fin: End year for the range of years.
    :type annio_fin: int

    :return: DataFrame 'df_sabana_downstream' with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    df_add = df_plp_downstream[lista_columnas]

    df_add.columns = range_anio
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transpuesto = df_add.set_index('ID_PROYECTO').transpose()

    df_nuevo = df_transpuesto.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    df_nuevo.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_downstream = pd.concat([df_sabana_downstream, df_nuevo], axis=1)
    return df_sabana_downstream


def calcular_depreciacion_icos_downstream(dataframe, cantidad_anios_depreciacion):
    """
    Calculate depreciation for downstream projects based on the given DataFrame.

    :param dataframe: DataFrame containing information about downstream projects.
    :type dataframe: pd.DataFrame
    :param cantidad_anios_depreciacion: Number of years for depreciation.
    :type cantidad_anios_depreciacion: int

    :return: DataFrame with the calculated depreciation for downstream projects.
    :rtype: pd.DataFrame
    """
    df_depreciacion = pd.DataFrame(index=dataframe['RANGO_ANIO'], columns=dataframe['RANGO_ANIO'])
    df_depreciacion = df_depreciacion.fillna(0)

    for indice_fila, fila in df_depreciacion.iterrows():
        for columna, valor in fila.items():
            if columna == indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == columna]['CAPEX'].values[0] / cantidad_anios_depreciacion
                df_depreciacion.loc[indice_fila, columna] = factor
            elif columna > indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == indice_fila]['CAPEX'].values[0] / cantidad_anios_depreciacion
                df_depreciacion.loc[indice_fila, columna] = float(factor)

    df_sum_columnas = df_depreciacion.sum(axis=0)
    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_resultado = sum_df

    return df_resultado


def calcular_impuestos_icos(row):
    """
    Calculate taxes for ICOs (Initial Coin Offerings) based on the provided row information.

    :param row: Row of a DataFrame containing information about ICOs.
    :type row: pd.Series

    :return: Calculated taxes for the ICO.
    :rtype: float
    """
    if row['GERENCIA'] in ['GRC', 'Reficar']:
        return row['BASE_GRABABLE'] * row['IMPUESTO_RENTA_20']
    else:
        return row['BASE_GRABABLE'] * (row['IMPUESTO_RENTA'] + row['SOBRETASA'])


def generate_random_alphanumeric(length):
    """
    Generate a random alphanumeric string of the specified length.

    :param length: Length of the generated string.
    :type length: int

    :return: Random alphanumeric string.
    :rtype: str
    """
    alphanumeric_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_characters) for _ in range(length))


def fill_alphanumeric_for_nan_or_empty(value):
    """
    Fill missing or empty values with a random alphanumeric string.

    :param value: Input value to check.
    :type value: str or pd.Series

    :return: Original value if not missing or empty, otherwise a random alphanumeric string.
    :rtype: str
    """
    if pd.isna(value) or value == '':
        return generate_random_alphanumeric(8)
    else:
        return value


# -------------------------------------------------Midstream

def asignar_valor_mid(row):
    """
    Assign a value for the 'MATRICULA_DIGITAL' column based on whether it's NaN or not.

    :param row: Row of a DataFrame.
    :type row: pd.Series

    :return: Assigned value for 'MATRICULA_DIGITAL'.
    :rtype: str
    """
    if pd.isna(row['MATRICULA_DIGITAL']):
        return 'MID-' + str(row.name)  
    else:
        return row['MATRICULA_DIGITAL']


def plpl_add_new_column_in_df_sabana_midstream(df_plp_midstream, df_sabana_midstream, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the DataFrame 'df_sabana_midstream' based on data from 'df_plp_midstream'.

    :param df_plp_midstream: DataFrame containing data for midstream projects.
    :type df_plp_midstream: pd.DataFrame
    :param df_sabana_midstream: DataFrame where the new column will be added.
    :type df_sabana_midstream: pd.DataFrame
    :param lista_columnas: List of column names to extract from 'df_plp_midstream'.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column in 'df_sabana_midstream'.
    :type nombre_columna_nueva: str
    :param annio_inicio: Starting year for the range of columns in 'df_plp_midstream'.
    :type annio_inicio: int
    :param annio_fin: Ending year for the range of columns in 'df_plp_midstream'.
    :type annio_fin: int

    :return: Updated DataFrame 'df_sabana_midstream' with the new column.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    df_add = df_plp_midstream[lista_columnas]

    df_add.columns = range_anio
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transposed = df_add.set_index('ID_PROYECTO').transpose()

    df_nuevo = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    df_nuevo.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_midstream = pd.concat([df_sabana_midstream, df_nuevo], axis=1)
    return df_sabana_midstream


def calcular_depreciacion(dataframe):
    """
    Calculate depreciation for each year based on the provided DataFrame.

    :param dataframe: DataFrame containing the necessary data.
    :type dataframe: pd.DataFrame

    :return: DataFrame with the calculated depreciation for each year.
    :rtype: pd.DataFrame
    """
    df_depreciacion = pd.DataFrame(index=dataframe['RANGO_ANIO'], columns=dataframe['RANGO_ANIO'])
    df_depreciacion = df_depreciacion.fillna(0)

    for indice_fila, fila in df_depreciacion.iterrows():
        for columna, valor in fila.items():
            if columna == indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == columna]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = factor
            elif columna > indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == indice_fila]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = float(factor)

    df_sum_columnas = df_depreciacion.sum(axis=0)

    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_resultado = sum_df

    return df_resultado


#--------------------------------------------------accorp

def asignar_valor_aacorp(row):
    """
    Assign a value to the AACORP column based on the MATRICULA_DIGITAL column.

    :param row: DataFrame row.
    :type row: pd.Series

    :return: Value for the AACORP column.
    :rtype: str
    """
    if pd.isna(row['MATRICULA_DIGITAL']):
        return 'AACORP' + str(row.name)
    else:
        return row['MATRICULA_DIGITAL']


def plpl_add_new_column_in_sabana_aacorp(df_plp_aacorp, df_sabana_aacorp, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the DataFrame df_sabana_aacorp based on data from df_plp_aacorp.

    :param df_plp_aacorp: DataFrame containing data to be added.
    :type df_plp_aacorp: pd.DataFrame
    :param df_sabana_aacorp: DataFrame to which the new column will be added.
    :type df_sabana_aacorp: pd.DataFrame
    :param lista_columnas: List of column names to be added.
    :type lista_columnas: List[str]
    :param nombre_columna_nueva: Name of the new column to be added.
    :type nombre_columna_nueva: str
    :param annio_inicio: Start year for the new column.
    :type annio_inicio: int
    :param annio_fin: End year for the new column.
    :type annio_fin: int

    :return: DataFrame with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    
    df_add = df_plp_aacorp[lista_columnas]

    df_add.columns = range_anio
    
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transposed = df_add.set_index('ID_PROYECTO').transpose()

    df_nuevo = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    
    df_nuevo.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_sabana_aacorp = pd.concat([df_sabana_aacorp, df_nuevo], axis=1)

    return df_sabana_aacorp


def calcular_depreciacion_up(dataframe):
    """
    Calculate depreciation based on the provided DataFrame.

    :param dataframe: DataFrame containing information for depreciation calculation.
    :type dataframe: pd.DataFrame

    :return: DataFrame with calculated depreciation values.
    :rtype: pd.DataFrame
    """
    df_depreciacion = pd.DataFrame(index=dataframe['RANGO_ANIO'], columns=dataframe['RANGO_ANIO'])
    df_depreciacion = df_depreciacion.fillna(0)

    for indice_fila, fila in df_depreciacion.iterrows():
        for columna, valor in fila.items():
            if columna == indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == columna]['CAPEX'].values[0] / 10
                df_depreciacion.loc[indice_fila, columna] = factor
            elif columna > indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == indice_fila]['CAPEX'].values[0] / 10
                df_depreciacion.loc[indice_fila, columna] = float(factor)
                
    df_sum_columnas = df_depreciacion.sum(axis=0)

    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_resultado = sum_df

    return df_resultado


def calcular_depreciacion_be(dataframe, porcentaje_depreciacion_anio, annio_inicio, annio_fin):
    """
    Calculate depreciation for a given DataFrame based on specified parameters.

    :param dataframe: DataFrame containing information for depreciation calculation.
    :type dataframe: pd.DataFrame
    :param porcentaje_depreciacion_anio: Percentage of depreciation per year.
    :type porcentaje_depreciacion_anio: float
    :param annio_inicio: Start year for depreciation calculation.
    :type annio_inicio: int
    :param annio_fin: End year for depreciation calculation.
    :type annio_fin: int

    :return: DataFrame with calculated depreciation values.
    :rtype: pd.DataFrame
    """
    result = pd.DataFrame(columns=['ID_PROYECTO', 'RANGO_ANIO', 'SUM_DEPRE', 'CAPEX_TOTAL_ANIO'])

    for index, row in dataframe.iterrows():
        id_proyecto = row['ID_PROYECTO']
        capex_total = row['SUMA_CAPEX']
        anio_inicio = row['ANIO_PRODUCCION']

        for year in range(annio_inicio, annio_fin):
            if year < anio_inicio or year > anio_inicio + 2:
                valor_depreciado = 0
                capex_total_anio = 0
            elif year == anio_inicio:
                valor_depreciado = capex_total * porcentaje_depreciacion_anio
                capex_total_anio = row['SUMA_CAPEX']
            else:
                valor_depreciado = capex_total * porcentaje_depreciacion_anio
                capex_total_anio = 0

            result = result.append({'ID_PROYECTO': id_proyecto, 'RANGO_ANIO': year, 'SUM_DEPRE': valor_depreciado, 'CAPEX_TOTAL_ANIO': capex_total_anio}, ignore_index=True)

    return result


def calcular_depreciacion_down(dataframe):
    """
    Calculate depreciation for a given DataFrame.

    :param dataframe: DataFrame containing information for depreciation calculation.
    :type dataframe: pd.DataFrame

    :return: DataFrame with calculated depreciation values.
    :rtype: pd.DataFrame
    """
    df_depreciacion = pd.DataFrame(index=dataframe['RANGO_ANIO'], columns=dataframe['RANGO_ANIO'])
    df_depreciacion = df_depreciacion.fillna(0)

    for indice_fila, fila in df_depreciacion.iterrows():
        for columna, valor in fila.items():
            if columna == indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == columna]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = factor
            elif columna > indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == indice_fila]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = float(factor)

    df_sum_columnas = df_depreciacion.sum(axis=0)

    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_resultado = sum_df

    return df_resultado


def calcular_depreciacion_mid(dataframe):
    """
    Calculate depreciation for a given DataFrame.

    :param dataframe: DataFrame containing information for depreciation calculation.
    :type dataframe: pd.DataFrame

    :return: DataFrame with calculated depreciation values.
    :rtype: pd.DataFrame
    """
    df_depreciacion = pd.DataFrame(index=dataframe['RANGO_ANIO'], columns=dataframe['RANGO_ANIO'])
    df_depreciacion = df_depreciacion.fillna(0)

    for indice_fila, fila in df_depreciacion.iterrows():
        for columna, valor in fila.items():
            if columna == indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == columna]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = factor
            elif columna > indice_fila:
                factor = dataframe[dataframe['RANGO_ANIO'] == indice_fila]['CAPEX'].values[0] / 20
                df_depreciacion.loc[indice_fila, columna] = float(factor)

    df_sum_columnas = df_depreciacion.sum(axis=0)

    sum_df = pd.DataFrame({'RANGO_ANIO': df_sum_columnas.index, 'SUM_DEPRE': df_sum_columnas.values})

    df_resultado = sum_df

    return df_resultado


def agregar_prefijo_columnas(dataframe, prefijo):
    """
    Add a prefix to the column names of a DataFrame.

    :param dataframe: DataFrame to modify.
    :type dataframe: pd.DataFrame
    :param prefijo: Prefix to add to the column names.
    :type prefijo: str

    :return: DataFrame with modified column names.
    :rtype: pd.DataFrame
    """
    columnas_actuales = dataframe.columns
    nuevos_nombres = {col: prefijo + str(col) for col in columnas_actuales}
    dataframe.rename(columns=nuevos_nombres, inplace=True)

    return dataframe


def procesar_grupo_mid(grupo):
    """
    Process a DataFrame for midstream projects by calculating depreciation and PPE.

    :param grupo: DataFrame containing information for midstream projects.
    :type grupo: pd.DataFrame

    :return: Resulting DataFrame after processing.
    :rtype: pd.DataFrame
    """
    df_depreciacion = calcular_depreciacion(grupo)
    merged_df = pd.merge(grupo, df_depreciacion, on='RANGO_ANIO', how='inner')
    result_df = calcular_ppe(merged_df)

    return result_df


#--------------------------dowstream_base

def plpl_add_new_column_in_sabana_datos_downstream_base(df_plp_activo_downstream, df_activo_downstream, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the DataFrame containing downstream base project assets.

    :param df_plp_activo_downstream: DataFrame containing information from PLP for downstream base project assets.
    :type df_plp_activo_downstream: pd.DataFrame
    :param df_activo_downstream: DataFrame containing downstream base project asset information.
    :type df_activo_downstream: pd.DataFrame
    :param lista_columnas: List of columns to include in the new DataFrame.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column to be added.
    :type nombre_columna_nueva: str
    :param annio_inicio: Starting year for the range of columns to be added.
    :type annio_inicio: int
    :param annio_fin: Ending year for the range of columns to be added.
    :type annio_fin: int

    :return: DataFrame with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))

    df_add = df_plp_activo_downstream[lista_columnas]
    df_add.columns = range_anio

    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)

    df_transposed = df_add.set_index('ID_PROYECTO').transpose()

    df_new = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)

    df_new.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)

    df_activo_downstream = pd.concat([df_activo_downstream, df_new], axis=1)

    return df_activo_downstream


def calcular_carga_livianos(row):
    """
    Calculate the effective load of light oil based on specified limits.

    :param row: DataFrame row containing information about light oil load and limits.
    :type row: pd.Series

    :return: Calculated effective light oil load.
    :rtype: float
    """
    if row['LIVIANO'] < row['L_MAX_LIVIANO'] and row['LIVIANO'] > row['L_MIN_LIVIANO']:
        return row['LIVIANO']
    elif row['LIVIANO'] > row['L_MAX_LIVIANO']:
        return row['L_MAX_LIVIANO']
    else:
        return row['LIVIANO']


def calcular_compra_n_liviano(row):
    """
    Calculate the amount of additional light oil to be purchased based on specified conditions.

    :param row: DataFrame row containing information about light oil requirements and limits.
    :type row: pd.Series

    :return: Amount of additional light oil to be purchased.
    :rtype: float
    """
    if row['REQUIERE_COMPRA_LIVIANO'] == 1:
        if row['LIVIANO_NAL'] + row['CARGA_LIVIANO'] > row['L_MIN_LIVIANO']:
            return row['L_MIN_LIVIANO'] - row['CARGA_LIVIANO']
        else:
            return row['LIVIANO_NAL']
    else:
        return 0


def calcular_carga_mediano(row):
    """
    Calculate the effective load of medium oil based on specified limits.

    :param row: DataFrame row containing information about medium oil load and limits.
    :type row: pd.Series

    :return: Calculated effective medium oil load.
    :rtype: float
    """
    if row['PESADO'] < row['L_MAX_PESADO'] and row['PESADO'] > row['L_MIN_PESADO']:
        return row['PESADO']
    elif row['PESADO'] > row['L_MAX_PESADO']:
        return row['L_MAX_PESADO']
    else:
        return row['PESADO']


def calcular_compra_n_pesado_barranca(row):
    """
    Calculate the amount of heavy oil to purchase in Barranca based on specified conditions.

    :param row: DataFrame row containing information about heavy oil load and purchase conditions.
    :type row: pd.Series

    :return: Amount of heavy oil to purchase in Barranca.
    :rtype: float
    """
    if row['REQUIERE_COMPRA_PESADO'] == 1:
        if row['PESADO_NAL'] + row['CARGA_PESADO'] > row['L_MIN_PESADO']:
            return row['L_MIN_PESADO'] - row['CARGA_PESADO']
        else:
            return row['PESADO_NAL']
    else:
        return 0


def calcular_carga_cast(row):
    """
    Calculate the effective load of Castilla oil based on specified limits.

    :param row: DataFrame row containing information about Castilla oil load and limits.
    :type row: pd.Series

    :return: Calculated effective Castilla oil load.
    :rtype: float
    """
    if row['CASTILLA'] < row['L_MAX_CAST'] and row['CASTILLA'] > row['L_MIN_CAST']:
        return row['CASTILLA']
    elif row['CASTILLA'] > row['L_MAX_CAST']:
        return row['L_MAX_CAST']
    else:
        return row['CASTILLA']


def calcular_compra_n_cast(row):
    """
    Calculate the amount of Castilla oil to purchase based on specified conditions.

    :param row: DataFrame row containing information about Castilla oil load and purchase conditions.
    :type row: pd.Series

    :return: Amount of Castilla oil to purchase.
    :rtype: float
    """
    if row['REQUIERE_COMPRA_CAST'] == 1:
        if row['CAST_NAL'] + row['CARGA_CAST'] > row['L_MIN_CAST']:
            return row['L_MIN_CAST'] - row['CARGA_CAST']
        else:
            return row['CAST_NAL']
    else:
        return 0


def calcular_compra_n_pesado_car(row):
    """
    Calculate the amount of heavy oil to purchase for CAR conditions.

    :param row: DataFrame row containing information about heavy oil load and purchase conditions for CAR.
    :type row: pd.Series

    :return: Amount of heavy oil to purchase for CAR.
    :rtype: float
    """
    if row['REQUIERE_COMPRA_PESADO'] == 0:
        return 0
    if row['CARGA_PESADO'] + row['PESADO_NAL'] > row['L_MIN_PESADO']:
        return row['L_MIN_PESADO']
    else:
        return row['PESADO_NAL']


def calcular_compra_n_mediano(row):
    """
    Calculate the amount of medium oil to purchase based on specified conditions.

    :param row: DataFrame row containing information about medium oil load and purchase conditions.
    :type row: pd.Series

    :return: Amount of medium oil to purchase.
    :rtype: float
    """
    if row['REQUIERE_COMPRA_MEDIANO'] == 1:
        if row['MEDIANO_NAL'] < row['GAP'] - row['CARGA_MEDIANO']:
            return row['MEDIANO_NAL']
        else:
            return row['GAP'] - row['CARGA_MEDIANO']
    else:
        return 0


#-------------------------mid_base

def plpl_add_new_column_in_activo_mid(df_plp_activo_midstream, df_midstream_activo, lista_columnas, nombre_columna_nueva, annio_inicio, annio_fin):
    """
    Add a new column to the DataFrame containing midstream project assets.

    :param df_plp_activo_midstream: DataFrame containing information from PLP for midstream project assets.
    :type df_plp_activo_midstream: pd.DataFrame
    :param df_midstream_activo: DataFrame containing midstream project asset information.
    :type df_midstream_activo: pd.DataFrame
    :param lista_columnas: List of columns to include in the new DataFrame.
    :type lista_columnas: list
    :param nombre_columna_nueva: Name of the new column to be added.
    :type nombre_columna_nueva: str
    :param annio_inicio: Starting year for the range of columns to be added.
    :type annio_inicio: int
    :param annio_fin: Ending year for the range of columns to be added.
    :type annio_fin: int

    :return: DataFrame with the new column added.
    :rtype: pd.DataFrame
    """
    range_anio = list(range(annio_inicio, annio_fin))
    df_add = df_plp_activo_midstream[lista_columnas]
    df_add.columns = range_anio
    df_add.loc[:, 'ID_PROYECTO'] = range(1, len(df_add) + 1)
    df_transposed = df_add.set_index('ID_PROYECTO').transpose()
    df_new = df_transposed.reset_index().melt(id_vars=['index'], var_name='ID_PROYECTO', value_name=nombre_columna_nueva)
    df_new.rename(columns={'index': 'RANGO_ANIO'}, inplace=True)
    df_midstream_activo = pd.concat([df_midstream_activo, df_new], axis=1)
    return df_midstream_activo

