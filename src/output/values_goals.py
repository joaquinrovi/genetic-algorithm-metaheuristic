import numpy as np
import pandas as pd
from itertools import chain

from utils.model_utils import (
    calc_fitness_water,
    calc_fitness_co2,
    calc_fitness_energy,
    calc_fitness_ebitda,
    calc_fitness_low_emissions,
    calc_fitness_global_nation,
    calc_fitness_gross,
    calc_fitness_cti,
    calc_fitness_not_oil_jobs,
    calc_fitness_students,
    calc_fitness_km,
    calc_fitness_access_water,
    calc_fitness_access_gas,
    calc_fitness_vpn
)

class ValuesGoals:
    """
    Class to get the values and goals of a certain portfolio
    """
    def __init__(self, data, results, config, pb_parameters):
        """
        Constructor of ValuesGoals class, here several configurations are read as well as the data needed

        :param data: Data that includes all projects taken into account by the model
        :type data: DataFrame
        :param results: The six portfolios produced by the model
        :type results: DataFrame
        :param config: Config from the model, including model_config.json and excel configurations
        :type config: Dictionary
        :param pb_parameters: Config entered from powerBI
        :type pb_parameters: Dictionary
        """
        self.pb_parameters = pb_parameters
        self.config = config
        self.data = data.loc[~data["PORTAFOLIO"].isin(["portafolio_1", "portafolio_2","portafolio_3","portafolio_4","portafolio_5","portafolio_6"])]
        # self.data["APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI"] = 40 / self.data.shape[0]
        # self.data["EMPLEOS_NP"] = 460 / self.data.shape[0]
        # self.data["ESTUDIANTES"] = 4 / self.data.shape[0]
        # self.data["ACCESO_AGUA_POTABLE"] = 40 / self.data.shape[0]
        # self.data["ACCESO_GAS"] = 460 / self.data.shape[0]
        # self.data["KM"] = 4 / self.data.shape[0]
        self.results = results
        self.columns = [
            "1. Cambio climatico",
            "2. Gestion integral del agua",
            "3. Energia renovables",
            "4. Negocios de bajas emisiones",
            "5. EBITDA",
            "6. Transferencias a la nacion",
            "7. DEUDA",
            "8. CTI",
            "9. EMPLEOS_NO_PETROLEROS",
            "10. ESTUDIANTES",
            "11. Agua potable",
            "12. Gas Natural",
            "13. Km",
            "14. VPN"
        ]
        (
            self.down_base_capex,
            self.down_base_ebitda,
            self.down_base_emissions,
            self.down_base_global,
            self.down_base_flow
        ) = self.get_down_projects_params(data, self.config["records_per_project"])


    def get_output(self):
        """
        Method that returns the values and goals of the portfolios created by the model

        :return: df_values, df_goals: dataframes containing goals and values
        :rtype: tuple(DataFrame, DataFrame)
        """
        df_goals = self.init_results_table()
        df_values = self.init_results_table()
        df_axis = self.init_axis_table()
        sostec_list = []
        finan_list = []
        for i in range(6):
            portfolio_label = f"PORTAFOLIO_{i + 1}"
            portfolio = self.results.loc[self.results[f"PORTAFOLIO_{i + 1}"] == 1, "MATRICULA_DIGITAL"].unique()
            values = self.get_portfolio_values(portfolio, self.down_base_ebitda[i], self.down_base_emissions[i], self.down_base_global[i])
            goals = self.get_goals(values)
            values_list = list(map(float, chain.from_iterable(values)))
            goal_list = list(map(float, chain.from_iterable(goals)))
            axis_list = self.get_axis(goals)
            sostec_list.append(axis_list[0])
            finan_list.append(axis_list[1])
            df_goals[portfolio_label] = goal_list
            df_values[portfolio_label] = values_list
        df_axis["Sostec"] = sostec_list
        df_axis["Finan"] = finan_list
        return df_values, df_goals, df_axis

    def get_portfolio_values(self, portfolio, down_base_ebitda, down_base_emissions, down_base_global):
        """
        Method that gets the values of a certain portfolio
        :param portfolio: portfolios given by the model
        :type portfolio: DataFrame
        :param down_base_ebitda: values of ebitda in down
        :type down_base_ebitda: np.Array
        :param down_base_emissions:  values of low_emissions in down
        :type down_base_emissions: np.Array
        :param down_base_global: values of global_nation in down
        :type down_base_global: np.Array

        :return: list containing the values
        :rtype: list
        """
        df_range = (
            self.data.loc[
                (self.data['RANGO_ANIO'] >= self.data['ANIO_INICIAL']) & (self.data['RANGO_ANIO'] < (self.data['ANIO_INICIAL'] + self.data['DURACION']))])
        portfolio_projects = df_range.loc[df_range['MATRICULA_DIGITAL'].isin(portfolio)]
        portfolio_low = portfolio_projects.loc[(portfolio_projects['LINEA_DE_NEGOCIO'] == 'Bajas Emisiones')|
                                                 (portfolio_projects['LINEA_DE_NEGOCIO'] == 'Transmisión y Vías')]
        portfolio_cti = portfolio_projects.loc[(portfolio_projects['SEGMENTO'] == 'Corporativo')]

        portfolio_low_year = portfolio_low.groupby('RANGO_ANIO')
        portfolio_year = portfolio_projects.groupby('RANGO_ANIO')
        portfolio_cti_year = portfolio_cti.groupby('RANGO_ANIO')

        water = (portfolio_year['AGUA_NEUTRALIDAD'].sum()).to_numpy()
        co2 = ((portfolio_year['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'].sum()) +
               down_base_emissions).to_numpy()
        energy = (portfolio_year['MW_INCORPORADOS'].sum()).to_numpy()
        ebitda = ((portfolio_year['EBITDA'].sum()) + down_base_ebitda).to_numpy()
        low_emissions = (portfolio_low_year['EBITDA'].sum()).to_numpy()
        global_nation = (((portfolio_year['TOTAL_R_NACION'].sum()) +
                         (portfolio_year['IMPUESTOS'].sum()) +
                         (portfolio_year['DIVIDENDO'].sum())) + down_base_global).to_numpy()
        debt = np.divide(
            self.config['debt'],
            ebitda,
            out=np.zeros_like(ebitda),
            where=ebitda != 0,
        )
        if len(low_emissions) == 0:
            low_emissions = np.zeros(self.config["records_per_project"])

        # cti = (potafolio_year['APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI'].sum()).to_numpy()
        cti = (portfolio_cti_year['EBITDA'].sum()).to_numpy()
        if len(cti) == 0:
            cti = np.zeros(self.config["records_per_project"])
        not_oil_jobs = (portfolio_year['EMPLEOS_NP'].sum()).to_numpy()
        students = (portfolio_year['ESTUDIANTES'].sum()).to_numpy()
        access_water = (portfolio_year['ACCESO_AGUA_POTABLE'].sum()).to_numpy()
        access_gas = (portfolio_year['ACCESO_GAS'].sum()).to_numpy()
        km = (portfolio_year['KM'].sum()).to_numpy()
        vpn = portfolio_year["VPN_PLP_CALCULADO"].sum().to_numpy()

        return [co2, water, energy, low_emissions, ebitda, global_nation, debt, cti, not_oil_jobs, students, access_water, access_gas, km, vpn]

    def get_goals(self, values):
        """
        Method that gets the goals of a certain portfolio
        :param values: values obtained from a certain portfolio
        :type values: list

        :return: list containing the goals for the entered values
        :rtype: list
        """
        water = 1 + calc_fitness_water(values[1], self.pb_parameters["neutralidad_agua_actual"], 1)
        co2 = 1 + calc_fitness_co2(values[0], self.pb_parameters["emisiones_netas_co2_alcance_1_y_2_actual"], 1)
        energy = calc_fitness_energy(values[2], self.pb_parameters["mwh_goal"], 1)
        ebitda = calc_fitness_ebitda(values[4], self.pb_parameters["ebitda_min"], 1)
        low_emissions = calc_fitness_low_emissions(values[4], values[3], self.pb_parameters["low_emissions_min"], 1, True)
        global_nation = calc_fitness_global_nation(values[5], self.pb_parameters["trans_nacion_min"], 1)
        debt = calc_fitness_gross(values[4], self.config['debt'], self.pb_parameters["deuda_bruta_ratio"], 1)
        cti = calc_fitness_cti(values[7], self.pb_parameters["cti_min"], 1)
        not_oil_jobs = calc_fitness_not_oil_jobs(values[8], self.pb_parameters["not_oil_jobs_goal"], 1)
        students = calc_fitness_students(values[9], self.pb_parameters["students_goal"], 1)
        access_water = calc_fitness_access_water(values[10], self.pb_parameters["agua_potable_actual"], 1)
        access_gas = calc_fitness_access_gas(values[11], self.pb_parameters["natural_gas_actual"], 1)
        km = calc_fitness_km(values[12], self.pb_parameters["km_actual"], 1)
        vpn = calc_fitness_vpn(values[13], self.pb_parameters["vpn_goal"], 1)
        return [co2, water, energy, low_emissions, ebitda, global_nation, debt, cti, not_oil_jobs, students, access_water, access_gas, km, vpn]


    def get_axis(self, goals):
        """
        Method to get sostec and finan axis per portfolio

        :param goals: goals obtained from a certain portfolio
        :type goals: list

        :return: list containing the sostec and finan axis
        :rtype: list

        """
        average = np.average(goals, axis=1).tolist()
        for i in range(len(average)):
            if average[i] > 1:
                average[i] = 1
            if average[i] < 0:
                average[i] = 0
        sostec = (average[0] + average[1] + average[2] + average[3])/4
        finan = (average[4] + average[5] + average[6] + average[13])/4
        return [sostec, finan]

    def get_down_projects_params(self, df_down, num_years):
        """
        Function that gets downstream refineries attributes

        :param df_down: df containing info about refineries
        :type df_down: DataFrame
        :param num_years: the number of years taken into account by the model
        :type num_years: int

        :return: attributes of the refineries by year
        :rtype: tuple(np.Array, np.Array, np.Array, np.Array, np.Array)
        """
        columns = ['MATRICULA_DIGITAL',
                   "EBITDA",
                   "CAPEX",
                   "EMISIONES_NETAS_CO2_ALCANCE_1_Y_2",
                   "FLUJO_CAJA",
                   "DIVIDENDO",
                   "IMPUESTOS",
                   "LINEA_DE_NEGOCIO",
                   "ACTIVO",
                   "EMPRESA",
                   "RANGO_ANIO",
                   "PORTAFOLIO"
                   ]

        df = df_down[columns]
        emissions_arr = np.zeros((6, num_years))
        ebitda_arr = np.zeros((6, num_years))
        global_nation_arr = np.zeros((6, num_years))
        capex_arr = np.zeros((6, num_years))
        flow_arr = np.zeros((6, num_years))
        df_portfolios = [
            df.loc[df['PORTAFOLIO'] == 'portafolio_1'],
            df.loc[df['PORTAFOLIO'] == 'portafolio_2'],
            df.loc[df['PORTAFOLIO'] == 'portafolio_3'],
            df.loc[df['PORTAFOLIO'] == 'portafolio_4'],
            df.loc[df['PORTAFOLIO'] == 'portafolio_5'],
            df.loc[df['PORTAFOLIO'] == 'portafolio_6']
        ]
        for i, df_port in enumerate(df_portfolios):
            df_year = df_port.groupby('RANGO_ANIO')
            emitions = df_year['EMISIONES_NETAS_CO2_ALCANCE_1_Y_2'].sum().to_numpy()
            ebitda = df_year['EBITDA'].sum().to_numpy()
            global_nation = (df_year['DIVIDENDO'].sum() + df_year['IMPUESTOS'].sum()).to_numpy()
            capex = df_year['CAPEX'].sum().to_numpy()
            flow = df_year['FLUJO_CAJA'].sum().to_numpy()
            emissions_arr[i] = emitions
            ebitda_arr[i] = ebitda
            global_nation_arr[i] = global_nation
            capex_arr[i] = capex
            flow_arr[i] = flow

        return capex_arr, ebitda_arr, emissions_arr, global_nation_arr, flow_arr

    def init_results_table(self):
        """
        Method that initialize the dataframe where the values or goals for each year will be saved.

        :usage:
            init_results_table([1,2])

        :return: DataFrame with the values(names of variables) and years replicated
        :rtype: Dataframe
        """

        year = [
            i
            for j in range(len(self.columns))
            for i in range(self.config["start_year"], self.config["start_year"] + self.config["records_per_project"])
        ]
        valor = [
            self.columns[i]
            for i in range(len(self.columns))
            for j in range(self.config["records_per_project"])
        ]
        return pd.DataFrame(
            {
                "VALOR": valor,
                "ANIO": year,
            }
        )

    def init_axis_table(self):
        """
        Method that initialize the dataframe where the axis per Portfolio will be saved

        :usage:
            init_results_table([1,2])

        :return: DataFrame with the values and years replicated
        :rtype: Dataframe
        """
        return pd.DataFrame(
            {
                "PORTAFOLIO":
                    ["PORTAFOLIO_1",
                    "PORTAFOLIO_2",
                    "PORTAFOLIO_3",
                    "PORTAFOLIO_4",
                    "PORTAFOLIO_5",
                    "PORTAFOLIO_6"]
            }
        )
