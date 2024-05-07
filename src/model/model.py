import random as rd
from itertools import chain, repeat

import pygad
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from utils.model_utils import (
    append_column_solution,
    filter_projects,
    get_project_params,
    get_capex_portfolio,
    get_projects,
    get_flujo_portfolio,
    get_indexes_turn_off,
    get_indexes_isa,
    get_off_actives,
    get_semi_mandatory_on,
    get_down_projects_params,
    get_mid_projects_params,
    generate_initial_population,
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
    calc_fitness_capex,
    calc_fitness_vpn,
    check_isa,
    check_selected_projects,
    check_capex
)
from utils.logger import Logger

from model.clusterization import Clusterization

logger = Logger(__name__).get_logger()


class Model:
    """
    This is a wrapper of the pygad class, where several problem specific functionalities are added such as
    reading data from csv, custom fitness function, and others

    :Usage:

    from model.model import Model

    model = Model(config)
    model.run_generations()
    """

    def __init__(
        self, config_dict: dict, pb_parameters, input_data, down_data, mid_data, initial=False
    ):
        """
        Class constructor where all the data is read, consolidated and normalized
        Also new data may be written depending on the configurations

        :param config_dict: dict containing all configs from model_config.json file and excel
        :type config_dict: Dictionary
        :param pb_parameters: dict containing all the configs that come from the powerBI
        :type pb_parameters: Dictionary
        :param input_data: DataFrame containing flujo_union projects
        :type input_data: DataFrame
        :param down_data: DataFrame containing down projects (refinerias)
        :type down_data: DataFrame
        :param mid_data: DataFrame containing mid projects
        :type mid_data: DataFrame
        :param initial: helper boolean that tells if the execution of the model is to initialize flujo_down and
        flujo_mid or if it is a normal run (defaults to False which means a normal run). The initialization run is
        made without taking into account flujo_down and flujo_mid since they wouldn't exist, this run is made to later
        create those two tables for the first time
        :type initial: bool
        """
        self.pb_parameters = pb_parameters
        self.config = config_dict
        (
            self.down_base_capex,
            self.down_base_ebitda,
            self.down_base_emitions,
            self.down_base_global,
            self.down_base_flow,
        ) = get_down_projects_params(down_data, initial, self.config)
        (
            self.mid_base_capex,
            self.mid_base_ebitda,
            self.mid_base_emitions,
            self.mid_base_flow,
            self.mid_base_global
        ) = get_mid_projects_params(mid_data, initial,  self.config)
        (
            self.df_projects,
            self.df_vpn_zero_projects,
            self.df_mandatory_projects,
            self.df_semi_mandatory,
        ) = filter_projects(
            input_data,
            self.pb_parameters["vpn_zero_restriction"],
            self.pb_parameters["phase_restriction"],
            self.pb_parameters["be_restriction"],
            self.config,
            self.pb_parameters
        )
        (
            self.mandatory_water,
            self.mandatory_co2,
            self.mandatory_energy,
            self.mandatory_ebitda,
            self.mandatory_ebitda_low_emission,
            self.mandatory_global_nation,
            self.mandatory_cti,
            self.mandatory_not_oil_jobs,
            self.mandatory_students,
            self.mandatory_access_water,
            self.mandatory_access_gas,
            self.mandatory_km,
            self.mandatory_vpn
        ) = get_project_params(self.df_mandatory_projects, self.config)
        self.mandatory_capex = get_capex_portfolio(self.df_mandatory_projects, self.config)
        self.mandatory_flujo = get_flujo_portfolio(self.df_mandatory_projects, self.config)
        self.indexes_isa = get_indexes_isa(self.df_projects)
        combined_df = pd.concat(
            [self.df_projects, self.df_mandatory_projects, self.df_semi_mandatory]
        )
        self.max_fit_y, self.min_fit_y = self.get_max_min_fitness_year(combined_df)
        # self.check_project()
        self.portfolios = np.zeros(
            (
                5 * (self.pb_parameters["generations"] - 20),
                int(self.df_projects.shape[0] / self.config["records_per_project"]),
            )
        )
        # self.portfolios_fitness = np.zeros(
        #     (5 * (self.pb_parameters["generations"] - 20))
        # )
        self.portfolios_fitness = np.full(
            (5 * (self.pb_parameters["generations"] - 20)), (-1000000000)
        )
        self.components_portfolio = np.zeros(
            (5 * (self.pb_parameters["generations"] - 20), 15)
        )

        self.initial_pop = generate_initial_population(
            self.pb_parameters["sol_per_pop"],
            int(self.df_projects.shape[0] / self.config["records_per_project"]),
            self.df_projects,
            self.df_semi_mandatory,
            self.indexes_isa,
            self.mandatory_capex,
            self.config
        )
        logger.info(f"Maximo capex:{self.config['capex_yearly_budget']}")

    def get_fitness_attributes(
        self,
        water,
        co2,
        energy,
        ebitda,
        ebitda_low_emission,
        global_nation,
        cti,
        not_oil_jobs,
        students,
        access_water,
        access_gas,
        km,
        capex,
        vpn
    ):
        """
        Method that calculates the fitness attributes of each variable passed to it

        :param water: np.Array containing water values for each year
        :type water: np.Array
        :param co2: np.Array containing co2 values for each year
        :type co2: np.Array
        :param energy: np.Array containing energy values for each year
        :type energy: np.Array
        :param ebitda: np.Array containing ebitda values for each year
        :type ebitda: np.Array
        :param ebitda_low_emission: np.Array containing ebitda_low_emissions values for each year
        :type ebitda_low_emission: np.Array
        :param global_nation: np.Array containing global_nation values for each year
        :type global_nation: np.Array
        :param cti: np.Array containing cti values for each year
        :type cti: np.Array
        :param not_oil_jobs: np.Array containing not_oil_jobs values for each year
        :type not_oil_jobs: np.Array
        :param students: np.Array containing students values for each year
        :type students: np.Array
        :param access_water: np.Array containing acess_water values for each year
        :type access_water: np.Array
        :param access_gas: np.Array containing access_gas values for each year
        :type access_gas: np.Array
        :param km: np.Array containing km values for each year
        :type km: np.Array
        :param capex: np.Array containing capex values for each year
        :type capex: np.Array
        :param vpn: np.Array containing capex values for each year
        :type vpn: np.Array

        :return: Returns what the input function returns.
        :rtype: tuple(np.Array, np.Array, np.Array, np.Array, np.Array, np.Array, np.Array, np.Array, np.Array,
        np.Array, np.Array, np.Array, np.Array, np.Array)

        """
        energy_fit = calc_fitness_energy(
            energy,
            self.pb_parameters["mwh_goal"],
            self.pb_parameters["mwh_weight"],
        )
        co2_fit = calc_fitness_co2(
            co2,
            self.pb_parameters["emisiones_netas_co2_alcance_1_y_2_actual"],
            self.pb_parameters["emisiones_netas_co2_alcance_1_y_2_weight"],
        )
        water_fit = calc_fitness_water(
            water,
            self.pb_parameters["neutralidad_agua_actual"],
            self.pb_parameters["neutralidad_agua_weight"],
        )
        gross_fit = calc_fitness_gross(
            ebitda,
            self.config["debt"],
            self.pb_parameters["deuda_bruta_ratio"],
            self.pb_parameters["deuda_bruta_weight"],
        )
        ebitda_fit = calc_fitness_ebitda(
            ebitda,
            self.pb_parameters["ebitda_min"],
            self.pb_parameters["ebitda_weight"],
        )
        low_fit = calc_fitness_low_emissions(
            ebitda,
            ebitda_low_emission,
            self.pb_parameters["low_emissions_min"],
            self.pb_parameters["low_emissions_weight"],
        )
        nation_fit = calc_fitness_global_nation(
            global_nation,
            self.pb_parameters["trans_nacion_min"],
            self.pb_parameters["trans_nacion_weight"],
        )
        cti_fit = calc_fitness_cti(
            cti,
            self.pb_parameters["cti_min"],
            self.pb_parameters["cti_weight"],
        )
        not_oil_jobs_fit = calc_fitness_not_oil_jobs(
            not_oil_jobs,
            self.pb_parameters["not_oil_jobs_goal"],
            self.pb_parameters["not_oil_jobs_weight"],
        )
        students_fit = calc_fitness_students(
            students,
            self.pb_parameters["students_goal"],
            self.pb_parameters["students_weight"],
        )
        km_fit = calc_fitness_km(
            km,
            self.pb_parameters["km_actual"],
            self.pb_parameters["km_weight"],
        )
        access_water_fit = calc_fitness_access_water(
            access_water,
            self.pb_parameters["agua_potable_actual"],
            self.pb_parameters["agua_potable_weight"],
        )
        access_gas_fit = calc_fitness_access_gas(
            access_gas,
            self.pb_parameters["natural_gas_actual"],
            self.pb_parameters["natural_gas_weight"],
        )
        capex_fit = calc_fitness_capex(
            capex
        )
        vpn_fit = calc_fitness_vpn(
            vpn,
            self.pb_parameters["vpn_goal"],
            self.pb_parameters["vpn_weight"],
        )

        return (
            water_fit,
            energy_fit,
            co2_fit,
            ebitda_fit,
            low_fit,
            gross_fit,
            nation_fit,
            cti_fit,
            not_oil_jobs_fit,
            students_fit,
            access_water_fit,
            access_gas_fit,
            km_fit,
            capex_fit,
            vpn_fit
        )

    def plot_fitness(self, ga_instance):
        """
        Method that creates dataframe which contains the fitness of the best solution of each population

        :Usage:

        self.plot_solution(instance)

        :param ga_instance: instance of pygad model
        :type ga_instance: instance of GA class (pygad class)

        :return: Dataframe containing fitness
        :rtype: Dataframe
        """
        solutions = ga_instance.best_solutions
        fitness_history = ga_instance.best_solutions_fitness

        x = range(0, len(fitness_history))
        y = list(fitness_history)
        data = {"GENERACION": x, "FITNESS": y}

        return pd.DataFrame(data)

    def total_benefit(
        self,
        water,
        co2,
        energy,
        ebitda,
        ebitda_low_emission,
        global_nation,
        cti,
        not_oil_jobs,
        students,
        access_water,
        access_gas,
        km,
        capex,
        vpn
    ):
        """
        Method to calculate the fitness for each variable and then normalize it

        :Usage:

        self.total_benefit(vpn,
                    water,
                    co2,
                    energy,
                    ebitda,
                    ebitda_low_emission,
                    ebitda,
                    ebitda_low_emission,
                    global_nation,
                    )

        :param water: np.Array containing water values for each year
        :type water: np.Array
        :param co2: np.Array containing co2 values for each year
        :type co2: np.Array
        :param energy: np.Array containing energy values for each year
        :type energy: np.Array
        :param ebitda: np.Array containing ebitda values for each year
        :type ebitda: np.Array
        :param ebitda_low_emission: np.Array containing ebitda_low_emissions values for each year
        :type ebitda_low_emission: np.Array
        :param global_nation: np.Array containing global_nation values for each year
        :type global_nation: np.Array
        :param cti: np.Array containing cti values for each year
        :type cti: np.Array
        :param not_oil_jobs: np.Array containing not_oil_jobs values for each year
        :type not_oil_jobs: np.Array
        :param students: np.Array containing students values for each year
        :type students: np.Array
        :param access_water: np.Array containing acess_water values for each year
        :type access_water: np.Array
        :param access_gas: np.Array containing access_gas values for each year
        :type access_gas: np.Array
        :param km: np.Array containing km values for each year
        :type km: np.Array
        :param capex: np.Array containing capex values for each year
        :type capex: np.Array
        :param vpn: np.Array containing vpn values for each year
        :type vpn: np.Array

        :return: np.Array containing the normalized fitness of each variable
        :rtype: np.Array
        """

        (
            fitness_water,
            fitness_co2,
            fitness_energy,
            fitness_ebitda,
            fitness_low_ebitda,
            fitness_gross,
            fitness_global_nation,
            fitness_cti,
            fitness_not_oil_jobs,
            fitness_students,
            fitness_access_water,
            fitness_access_gas,
            fitness_km,
            fitness_capex,
            fitness_vpn
        ) = self.get_fitness_attributes(
            water,
            co2,
            energy,
            ebitda,
            ebitda_low_emission,
            global_nation,
            cti,
            not_oil_jobs,
            students,
            access_water,
            access_gas,
            km,
            capex,
            vpn
        )

        normalized_fitness = self.normalize_fitness(
            fitness_water,
            fitness_co2,
            fitness_energy,
            fitness_ebitda,
            fitness_low_ebitda,
            fitness_gross,
            fitness_global_nation,
            fitness_cti,
            fitness_not_oil_jobs,
            fitness_students,
            fitness_access_water,
            fitness_access_gas,
            fitness_km,
            fitness_capex,
            fitness_vpn

        )
        benefit = np.array([
            np.average(normalized_fitness[0]),
            np.average(normalized_fitness[1]),
            np.average(normalized_fitness[2]),
            np.average(normalized_fitness[3]),
            np.average(normalized_fitness[4]),
            np.average(normalized_fitness[5]),
            np.average(normalized_fitness[6]),
            np.average(normalized_fitness[7]),
            np.average(normalized_fitness[8]),
            np.average(normalized_fitness[9]),
            np.average(normalized_fitness[10]),
            np.average(normalized_fitness[11]),
            np.average(normalized_fitness[12]),
            np.average(normalized_fitness[13]),
            np.average(normalized_fitness[14]),
        ])

        # benefit = normalized_fitness.sum(axis=1)

        return benefit

    def normalize_fitness(
        self,
        fitness_water,
        fitness_co2,
        fitness_energy,
        fitness_ebitda,
        fitness_low_ebitda,
        fitness_gross,
        fitness_global_nation,
        fitness_cti,
        fitness_not_oil_jobs,
        fitness_students,
        fitness_access_water,
        fitness_access_gas,
        fitness_km,
        fitness_capex,
        fitness_vpn
    ):
        """
        Method to normalize each years fitness, performing a min_max normalization

        :param fitness_water: np.Array containing water fitness for each year
        :type fitness_water: np.Array
        :param fitness_co2: np.Array containing co2 fitness for each year
        :type fitness_co2: np.Array
        :param fitness_energy: np.Array containing energy fitness for each year
        :type fitness_energy: np.Array
        :param fitness_ebitda: np.Array containing ebitda fitness for each year
        :type fitness_ebitda: np.Array
        :param fitness_low_ebitda: np.Array containing ebitda_low_emissions fitness for each year
        :type fitness_low_ebitda: np.Array
        :param fitness_gross: np.Array containing gross debt fitness for each year
        :type fitness_gross: np.Array
        :param fitness_global_nation: np.Array containing global_nation fitness for each year
        :type fitness_global_nation: np.Array
        :param fitness_cti: np.Array containing cti fitness for each year
        :type fitness_cti: np.Array
        :param fitness_not_oil_jobs: np.Array containing not_oil_jobs fitness for each year
        :type fitness_not_oil_jobs: np.Array
        :param fitness_students: np.Array containing students fitness for each year
        :type fitness_students: np.Array
        :param fitness_access_water: np.Array containing acess_water fitness for each year
        :type fitness_access_water: np.Array
        :param fitness_access_gas: np.Array containing access_gas fitness for each year
        :type fitness_access_gas: np.Array
        :param fitness_km: np.Array containing km fitness for each year
        :type fitness_km: np.Array
        :param fitness_capex: np.Array containing capex fitness for each year
        :type fitness_capex: np.Array
        :param fitness_vpn: np.Array containing vpn fitness for each year
        :type fitness_vpn: np.Array

        :return: np.Array contianing all the normalized fitness np.Arrays
        :rtype: np.Array
        """

        fitness_attributes = np.array(
            [
                fitness_water,
                fitness_co2,
                fitness_energy,
                fitness_low_ebitda,
                fitness_ebitda,
                fitness_gross,
                fitness_global_nation,
                fitness_cti,
                fitness_not_oil_jobs,
                fitness_students,
                fitness_access_water,
                fitness_access_gas,
                fitness_km,
                fitness_capex,
                fitness_vpn
            ]
        )
        normalized_fitness = np.zeros(
            (
                15,
                self.config["records_per_project"],
            )
        )
        for i, x in enumerate(fitness_attributes):
            if i == 3:
                normalized_fitness[i] = x
            else:
                normalized_fitness[i] = np.divide(
                    (x - self.min_fit_y[i]),
                    (self.max_fit_y[i] - self.min_fit_y[i]),
                    out=np.zeros_like(x),
                    where=self.max_fit_y[i] - self.min_fit_y[i] != 0,
                )
        return normalized_fitness

    def get_max_min_values_year(self, df):
        """
        Method that gets the maximum and minimum possible value of a certain variable. This is done to later
        implement the min_max normalization

        :param df: Dataframe containing projects to be considered
        :type df: DataFrame

        :return: tuple of two np.Arrays containing maximum and minimum possible values of each variable
        :rtype: tuple(np.Array, np.Array)
        """
        df = df.loc[
            (df["RANGO_ANIO"] >= df["ANIO_INICIAL"])
            & (df["RANGO_ANIO"] <= (df["ANIO_FINAL"]))
        ]
        values = [
            "AGUA_NEUTRALIDAD",
            "EMISIONES_NETAS_CO2_ALCANCE_1_Y_2",
            "MW_INCORPORADOS",
            "EBITDA",
            "BAJAS_EMISIONES",
            "TRANSFERENCIAS",
            "APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI",
            "EMPLEOS_NP",
            "ESTUDIANTES",
            "ACCESO_AGUA_POTABLE",
            "ACCESO_GAS",
            "KM",
            "CAPEX",
            "VPN_PLP_CALCULADO"
        ]
        max_values = np.zeros(14)
        min_values = np.zeros(14)
        for i, x in enumerate(max_values):
            if i == 4:
                max_values[i] = 0
                min_values[i] = 0
            elif i == 5:
                max_values[i] = max(
                    df.loc[(df["TOTAL_R_NACION"] + df["DIVIDENDO"] + df["IMPUESTOS"]) > 0]
                    .groupby("RANGO_ANIO")[["DIVIDENDO", "TOTAL_R_NACION", "IMPUESTOS"]]
                    .sum()[["DIVIDENDO", "TOTAL_R_NACION", "IMPUESTOS"]]
                    .sum()
                )
                min_values[i] = min(
                    df.loc[(df["TOTAL_R_NACION"] + df["DIVIDENDO"] + df["IMPUESTOS"]) <= 0]
                    .groupby("RANGO_ANIO")[["DIVIDENDO", "TOTAL_R_NACION", "IMPUESTOS"]]
                    .sum()[["DIVIDENDO", "TOTAL_R_NACION", "IMPUESTOS"]]
                    .sum()
                )
            elif i == 6:
                df_corp = df.loc[(df["SEGMENTO"] == "Corporativo")]
                try:
                    max_values[i] = max(
                        df.loc[((df["EBITDA"]) >= 0) & (df["SEGMENTO"] == "Corporativo")]
                        .groupby("RANGO_ANIO")["EBITDA"]
                        .sum()
                    )
                except ValueError:
                    max_values[i] = 0
                try:
                    min_values[i] = min(
                        df.loc[((df["EBITDA"]) <= 0) & (df["SEGMENTO"] == "Corporativo")]
                        .groupby("RANGO_ANIO")["EBITDA"]
                        .sum()
                    )
                except ValueError:
                    min_values[i] = 0
            else:
                max_values[i] = max(
                    df.loc[df[values[i]] >= 0].groupby("RANGO_ANIO")[values[i]].sum()
                )
                try:
                    min_values[i] = min(
                        df.loc[df[values[i]] <= 0]
                        .groupby("RANGO_ANIO")[values[i]]
                        .sum()
                    )
                except ValueError:
                    min_values[i] = 0
        return max_values, min_values

    def get_max_min_fitness_year(self, df):
        """
        Method that gets the maximum and minimum possible fitness value of a certain variable. This is done to later
        implement the min_max normalization.

        :param df: Dataframe containing projects to be considered
        :type df: DataFrame

        :return: tuple of two np.Arrays containing maximum and minimum possible fitness values of each variable
        :rtype: tuple(np.Array, np.Array)
        """

        max_val, min_val = self.get_max_min_values_year(df)

        (
            max_water,
            max_co2,
            max_energy,
            max_ebitda,
            max_low_ebitda,
            max_gross,
            max_global_nation,
            max_cti,
            max_not_oil_jobs,
            max_students,
            max_access_water,
            max_access_gas,
            max_km,
            max_capex,
            max_vpn
        ) = self.get_fitness_attributes(
            max_val[0],
            max_val[1],
            max_val[2],
            max_val[3],
            max_val[4],
            max_val[5],
            max_val[6],
            max_val[7],
            max_val[8],
            max_val[9],
            max_val[10],
            max_val[11],
            max_val[12],
            max_val[13]
        )

        (
            min_water,
            min_co2,
            min_energy,
            min_ebitda,
            min_low_ebitda,
            min_gross,
            min_global_nation,
            min_cti,
            min_not_oil_jobs,
            min_students,
            min_access_water,
            min_access_gas,
            min_km,
            min_capex,
            min_vpn
        ) = self.get_fitness_attributes(
            min_val[0],
            min_val[1],
            min_val[2],
            min_val[3],
            min_val[4],
            min_val[5],
            min_val[6],
            min_val[7],
            min_val[8],
            min_val[9],
            min_val[10],
            min_val[11],
            min_val[12],
            min_val[13]
        )

        max_fit = np.array(
            [
                max_water,
                max_co2,
                max_energy,
                max_low_ebitda,
                max_ebitda,
                max_gross,
                max_global_nation,
                max_cti[0],
                max_not_oil_jobs,
                max_students,
                max_access_water,
                max_access_gas,
                max_km,
                max_capex,
                max_vpn
            ]
        )

        min_fit = np.array(
            [
                min_water,
                min_co2,
                min_energy,
                min_low_ebitda,
                min_ebitda,
                min_gross,
                min_global_nation,
                min_cti[0],
                min_not_oil_jobs,
                min_students,
                min_access_water,
                min_access_gas,
                min_km,
                min_capex,
                min_vpn
            ]
        )

        for i, x in enumerate(max_fit):
            if max_fit[i] < min_fit[i]:
                temp = max_fit[i]
                max_fit[i] = min_fit[i]
                min_fit[i] = temp

        return max_fit, min_fit

    def check_cash_flow(self, portfolio, verbose=False):
        """
        Method that checks if the cash flow of a certain portfolio is below the limit or not

        :param portfolio: np.Array with projects where each index has a 1 or 0 where 1 means on and 0 means off
        :type portfolio: np.Array
        :param verbose: helper bool to debug method, it defaults to false (don't debug)
        :type verbose: bool

        :return: int containing the number of years where the cash flow of the portfolio is lower than the limit
        :rtype: int

        """
        df_projects_on = get_projects(self.df_projects, portfolio)
        df_semi_mandatory = get_semi_mandatory_on(
            self.df_semi_mandatory, get_off_actives(self.df_projects, portfolio)
        )
        flujo = get_flujo_portfolio(df_projects_on, self.config)
        flujo_semi_mandatory = get_flujo_portfolio(df_semi_mandatory, self.config)
        flujo = (
                flujo + self.mandatory_flujo + flujo_semi_mandatory + self.down_base_flow + self.mid_base_flow
        )
        bad_years = np.where(flujo < self.pb_parameters["cash_flow_restriction"])
        if verbose:
            logger.info(flujo)
        return bad_years[0].shape[0]

    def calc_fitness(self, portfolio):
        """
        Method that calculates the fitness of a certain portfolio by calculating the normalized fitness
        of each variable

        :param portfolio: np.Array with projects where each index has a 1 or 0 where 1 means on and 0 means off
        :type portfolio: np.Array

        :return: np.Array containing the normalized fitness of each variable
        :rtype: np.Array

        """
        # check flow
        # if self.check_cash_flow(portfolio):
        #     return np.zeros(18)

        bad_flow = self.check_cash_flow(portfolio)
        if bad_flow > 1:
            return np.full(15, bad_flow*-100)

        # get phase 4
        actives_off = get_off_actives(self.df_projects, portfolio)
        df_semi_mandatory = get_semi_mandatory_on(self.df_semi_mandatory, actives_off)
        # calculate fitness
        water_semi_mandatory = np.zeros(self.config["records_per_project"])
        co2_semi_mandatory = np.zeros(self.config["records_per_project"])
        energy_semi_mandatory = np.zeros(self.config["records_per_project"])
        ebitda_semi_mandatory = np.zeros(self.config["records_per_project"])
        ebitda_low_emission_semi_mandatory = np.zeros(
            self.config["records_per_project"]
        )
        global_nation_semi_mandatory = np.zeros(self.config["records_per_project"])
        cti_semi_mandatory = np.zeros(self.config["records_per_project"])
        not_oil_jobs_semi_mandatory = np.zeros(self.config["records_per_project"])
        students_semi_mandatory = np.zeros(self.config["records_per_project"])
        access_water_semi_mandatory = np.zeros(self.config["records_per_project"])
        access_gas_semi_mandatory = np.zeros(self.config["records_per_project"])
        km_semi_mandatory = np.zeros(self.config["records_per_project"])
        vpn_semi_mandatory = np.zeros(self.config["records_per_project"])
        if self.pb_parameters["phase_restriction"]:
            (
                water_semi_mandatory,
                co2_semi_mandatory,
                energy_semi_mandatory,
                ebitda_semi_mandatory,
                ebitda_low_emission_semi_mandatory,
                global_nation_semi_mandatory,
                cti_semi_mandatory,
                not_oil_jobs_semi_mandatory,
                students_semi_mandatory,
                access_water_semi_mandatory,
                access_gas_semi_mandatory,
                km_semi_mandatory,
                vpn_semi_mandatory
            ) = get_project_params(df_semi_mandatory, self.config)

        (
            water,
            co2,
            energy,
            ebitda,
            ebitda_low_emission,
            global_nation,
            cti,
            not_oil_jobs,
            students,
            access_water,
            access_gas,
            km,
            vpn
        ) = get_project_params(get_projects(self.df_projects, portfolio), self.config)

        df_projects_on = get_projects(self.df_projects, portfolio)
        capex = get_capex_portfolio(df_projects_on, self.config)

        fitness = self.total_benefit(
            water + water_semi_mandatory + self.mandatory_water,
            co2 + co2_semi_mandatory + self.down_base_emitions + self.mandatory_co2 + self.mid_base_emitions,
            energy + energy_semi_mandatory + self.mandatory_energy,
            ebitda
            + ebitda_semi_mandatory
            + self.down_base_ebitda
            + self.mid_base_ebitda
            + self.mandatory_ebitda,
            (
                ebitda_low_emission
                + ebitda_low_emission_semi_mandatory
                + self.mandatory_ebitda_low_emission
            ),
            global_nation
            + global_nation_semi_mandatory
            + self.down_base_global
            + self.mid_base_global
            + self.mandatory_global_nation,
            cti + cti_semi_mandatory + self.mandatory_cti,
            not_oil_jobs + not_oil_jobs_semi_mandatory + self.mandatory_not_oil_jobs,
            students + students_semi_mandatory + self.mandatory_students,
            access_water + access_water_semi_mandatory + self.mandatory_access_water,
            access_gas + access_gas_semi_mandatory + self.mandatory_access_gas,
            km + km_semi_mandatory + self.mandatory_km,
            capex,
            vpn + vpn_semi_mandatory + self.mandatory_vpn
        )

        return fitness

    def run_generations(self):
        """
        Method that start the main execution of the model

        :return: tuple of 2, containing the df that has the results of the 6 scenarios that the model created and the
        df with the fitness values

        :rtype: tuple(DataFrame, DataFrame)
        """

        pyga_instance = self.init_pygad()
        pyga_instance.run()
        df_fitness = self.plot_fitness(pyga_instance)

        # get 5 portfolios
        mask = self.portfolios_fitness > -100
        # 0 before changes
        # portfolios = self.portfolios[mask]
        components_portfolio = self.components_portfolio[mask, :]
        df_components = pd.DataFrame(
            components_portfolio,
            columns=[
                "WATER",
                "CO2",
                "ENERGY",
                "LOW_EMISSION",
                "EBITDA",
                "GROSS",
                "GLOBAL_NATION",
                "CTI",
                "EMPLEOS_NO_P",
                "ESTUDIANTES",
                "ACCESO_AGUA",
                "ACCESO_GAS",
                "KM",
                "CAPEX",
                "VPN"
            ],
        )
        cluster = Clusterization(df_components)
        try:
            idx = cluster.cluster()
        except ValueError as e:
            raise ValueError(f"Revise los parametros ingresados, no se han podido encontrar portafolios optimos. {e}")
        idx = idx[:5]
        portfolios = self.portfolios[idx]

        portfolios_semi_mandatory = np.empty(
            (
                portfolios.shape[0],
                self.df_semi_mandatory.MATRICULA_DIGITAL.unique().shape[0],
            )
        )
        for j, portfolio in enumerate(portfolios):
            df_semi_mandatory_on = get_semi_mandatory_on(
                self.df_semi_mandatory, get_off_actives(self.df_projects, portfolio)
            )
            portfolios_semi_mandatory[j] = np.in1d(
                self.df_semi_mandatory["MATRICULA_DIGITAL"].unique(),
                df_semi_mandatory_on["MATRICULA_DIGITAL"].unique(),
                True,
            ).astype("int")

        df_model_results = append_column_solution(
            self.df_projects,
            self.df_mandatory_projects,
            self.df_vpn_zero_projects,
            self.df_semi_mandatory,
            portfolios,
            portfolios_semi_mandatory,
        )
        logger.info(f"model successfully executed")

        return df_model_results, df_fitness

    def init_pygad(self):
        """
        method made to initialize the pygad instance

        :Usage:

        self.init_pygad()

        :return: ga_instance with all the specified params
        :rtype: class GA ga_instance (PYGAD class)
        """

        def fitness_func(ga_instance, solution, solution_idx):
            """
            Method for the fitness calculation required by pygad

            :param ga_instance: pygad instance
            :type ga_instance: instance of GA class (pygad class)
            :param solution: a specific solution (portfolio) to calculate the fitness on
            :type solution: np.Array
            :param solution_idx: index of the solution in the population
            :type solution_idx: int

            :return: float containing the final fitness of the solution passed to the method
            :rtype: float
            """
            fit = self.calc_fitness(solution) # calc_fitness es una función que consolida varias otras funciones fitness, es imprecindible esta función
            return fit.sum()

        def mutation(offsprings, ga_instance):
            """
            Method to perform mutation on the offspring solutions.
            include the contraceptive method to ensure no over-budget (capex restriction)

            :param offsprings: A 2D NumPy array representing the offspring solutions.
            :type offsprings: numpy.ndarray

            :param ga_instance: pygad instance containing the parameters and configuration.
            :type ga_instance: pygad instance

            :return: A new 2D NumPy array containing the mutated offspring solutions.
            :rtype: numpy.ndarray

            """

            if len(offsprings.shape) != 2:
                raise ValueError("offsprings array must be 2-dimensional.")

            mutants = offsprings.copy() # Copiar los descendientes para evitar modificar el original

            for i in range(mutants.shape[0]):
                # Aplicar la mutación genética normalmente
                mutation_rate = self.pb_parameters["mutation_ind"]
                gen_mutation_rate = self.pb_parameters["mutation_gen"]
                random_values = np.random.rand(len(mutants[i]))

                # Aplicar mutación a los genes según la tasa de mutación
                mutation_indices = np.where(random_values < mutation_rate)[0]
                mutants[i, mutation_indices] = 1 - mutants[i, mutation_indices]

                # Apagar proyectos según las restricciones
                actives_off = get_off_actives(self.df_projects, mutants[i])
                indexes_turn_off = get_indexes_turn_off(self.df_projects, actives_off)
                mutants[i, indexes_turn_off] = 0

                # Verificar la restricción de ISA
                check_isa(mutants[i], self.indexes_isa)

                # Verificar la restricción de CAPEX
                check_capex(mutants[i], self.df_projects, self.df_semi_mandatory, self.mandatory_capex, actives_off, self.config)

                # Verificar la restricción de DEPENDENCIAS
                available_projects = check_selected_projects(self.df_projects) # Verificar la carga de datos acá (columnas del archivo PLP)
                for j in range(mutants[i].shape[0]):
                    if self.df_projects.iloc[j]['Proyecto'] not in available_projects:
                        mutants[i, j] = 0

                '''
                En esta función actualizada, después de aplicar la mutación genética habitual, se apagan los proyectos según las restricciones de ISA y CAPEX, y luego se verifica 
                la restricción adicional de proyectos disponibles utilizando la función check_selected_projects() que se definió en model_utils.py. Si un proyecto no está en la lista 
                de proyectos disponibles, se apaga en la solución. Es importante tener en cuenta que esta implementación asume que las funciones 
                get_off_actives(), get_indexes_turn_off(), check_isa() y check_capex() están definidas y funcionando correctamente para las necesidades específicas. 
                Una vez que se haya integrado esta actualización en el código, el algoritmo genético debería tener en cuenta las restricciones adicionales al generar y mutar las soluciones. 
                '''

                return mutants

        def on_generation(ga_instance):
            """
            Method to execute each time a generation is completed, here the portfolios with good fitness are
            saved for later decisions on the 6 best portfolios

            :param ga_instance: pygad instance containing the parameters and configuration.
            :type ga_instance: pygad instance

            """
            # si es la primera vez reviso la población inicial
            logger.info(f"Generation: {ga_instance.generations_completed}")
            if ga_instance.generations_completed > 20:
                fitnesses = ga_instance.last_generation_fitness[
                    self.config["pygad"]["keep_elitism"] :
                ]
                portfolios = ga_instance.last_generation_offspring_mutation
                best_5 = np.argpartition(fitnesses, -5)[-5:]
                for i, index in enumerate(best_5):
                    if not np.any(np.all(self.portfolios == portfolios[index], axis=1)):
                        min_index = np.argmin(self.portfolios_fitness)
                        if fitnesses[index] > self.portfolios_fitness[min_index]:
                            self.portfolios[min_index] = portfolios[index]
                            self.portfolios_fitness[min_index] = fitnesses[index]
                            self.components_portfolio[min_index] = self.calc_fitness(
                                portfolios[index]
                            )

        fitness_function = fitness_func
        num_genes = int(self.df_projects.shape[0] / self.config["records_per_project"])

        num_generations = self.pb_parameters["generations"]
        num_parents_mating = self.config["pygad"]["num_parents_mating"]

        sol_per_pop = self.pb_parameters["sol_per_pop"]

        initial_population = self.initial_pop

        gene_type = int

        parent_selection_type = self.config["pygad"]["parent_selection_type"]

        crossover_type = self.config["pygad"]["crossover_type"]
        keep_elitism = self.config["pygad"]["keep_elitism"]

        ga_instance = pygad.GA(
            initial_population=initial_population,
            num_generations=num_generations,
            num_parents_mating=num_parents_mating,
            fitness_func=fitness_function,
            sol_per_pop=sol_per_pop,
            num_genes=num_genes,
            parent_selection_type=parent_selection_type,
            keep_elitism=keep_elitism,
            crossover_type=crossover_type,
            mutation_type=mutation,
            gene_type=gene_type,
            save_best_solutions=False,
            on_generation=on_generation,
        )

        return ga_instance
