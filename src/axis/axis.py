import pandas as pd
import numpy as np

goals = [
    "CAMBIO_CLIMATICO",  # 0
    "GESTION_INTEGRAL_DEL_AGUA",  # 1
    "ENERGIAS_RENOVABLES",  # 2
    "NEGOCIOS_DE_BAJAS_EMISIONES",  # 3
    "EBITDA_META",  # 4
    "TRANSFERENCIAS_A_LA_NACION",  # 5
    "DEUDA",  # 6
    "VPN",  # 7
    "CT+I",  # 8
    "EMPLEOS_NO_PETROLEROS",  # 9
    "ESTUDIANTES",  # 10
    "AGUA_POTABLE",  # 11
    "GAS_NATURAL",  # 12
    "KM_META",  # 13
]

usecols = [
    "MATRICULA_DIGITAL",  # 0
    "FASE_EN_CURSO",  # 1
    "MW_INCORPORADOS",  # 2
    "RANGO_ANIO",  # 3
    "EBITDA",  # 4
    "EMISIONES_NETAS_CO2_ALCANCE_1_Y_2",  # 5
    "AGUA_NEUTRALIDAD",  # 6
    "CAPEX",  # 7
    "TOTAL_R_NACION",  # 8
    "DIVIDENDO",  # 9
    "IMPUESTOS",  # 10
    "ANIO_INICIAL",  # 11
    "ANIO_FINAL",  # 12
    "VPN_PLP_CALCULADO",  # 13
    "NOMBRE_PROYECTO",  # 14
    "LINEA_DE_NEGOCIO",  # 15
    "EBIT",  # 16
    "CAPITAL_EMPLEADO",  # 17
    "TIPO_DE_INVERSION",  # 18
    "OPCION_ESTRATEGICA",  # 19
    "FLUJO_CAJA",  # 20
    "ACTIVO",  # 21
    "EMPRESA",  # 22
    "SEGMENTO",  # 23
    "EMPLEOS_NP",  # 24
    "ESTUDIANTES",  # 25
    "ACCESO_AGUA_POTABLE",  # 26
    "ACCESO_GAS",  # 27
    "KM",  # 28
    "APORTE_EBITDA_ACUM_2040_PROYECTOS_ASOCIADOS_A_CTI",  # 29
    "PORTAFOLIO",  # 30
]


class Axis:
    """
    This is a  class, to calculate metrics per project, the sostec and finance axis

    :Usage:

    from axis.axis import Axis

    axis_instance = Axis(df_input,parameters)
    axis_instance.axis()
    """

    def __init__(self, df_up, params, config):
        """
        Class constructor where all the data is read,

        :param df_up: dataframe with the projects to calculate axis and metrics
        :param parameters: dataframe with downstream base projects
        """

        self.params = params
        self.goals_arr = np.array(
            [
                -params["emisiones_netas_co2_alcance_1_y_2_actual"],
                -params["neutralidad_agua_actual"],
                params["mwh_goal"],
                params["low_emissions_min"],
                params["ebitda_min"],
                params["trans_nacion_min"],
                1 / params["deuda_bruta_ratio"],
                1,
                params["cti_min"],
                params["not_oil_jobs_goal"],
                params["students_goal"],
                params["agua_potable_actual"],
                params["natural_gas_actual"],
                params["km_actual"],
            ]
        )
        self.debt = config["debt"]
        self.df = self.__calc_metrics_proyect_year(df_up)

    def __calc_metrics_proyect_year(self, df):
        """
        This method calculate the metrics per year and project
        that can be calculated without portfolios
        :param df: DataFrame with the projects per year information
        :return: DataFrame with the metrics per year and project
        """
        df.fillna(0, inplace=True)
        debt_anio = np.repeat(self.debt,int(df.shape[0] / 17))
        water_metric = -(df.loc[:, usecols[6]].to_numpy() / self.params["neutralidad_agua_actual"])

        co2_metric = -(
            df.loc[:, usecols[5]].to_numpy()
            / self.params["emisiones_netas_co2_alcance_1_y_2_actual"]
        )

        energy_metric = df.loc[:, usecols[2]].to_numpy() / self.params["mwh_goal"]

        ebitda_metric = df.loc[:, usecols[4]] .to_numpy()/ self.params["ebitda_min"]


        global_nation = (
            df.loc[:, usecols[8]].to_numpy() + df.loc[:, usecols[9]].to_numpy() + df.loc[:, usecols[10]].to_numpy()
        )
        gn_metric = global_nation / self.params["trans_nacion_min"]

        le_ebitda = (df.loc[:, usecols[15]] == "Bajas Emisiones") * df.loc[
            :, usecols[4]
        ].to_numpy()

        gross_metric = (
            self.params["deuda_bruta_ratio"] * df.loc[:, usecols[4]].to_numpy()
        ) / debt_anio

        cti_metric = df.loc[:, usecols[29]].to_numpy() / self.params["cti_min"]

        noj_metric = df.loc[:, usecols[24]].to_numpy() / self.params["not_oil_jobs_goal"]

        students_metric = df.loc[:, usecols[25]].to_numpy() / self.params["students_goal"]

        potable_water_metric = (
            df.loc[:, usecols[26]].to_numpy() / self.params["agua_potable_actual"]
        )

        gas_metric = df.loc[:, usecols[27]].to_numpy() / self.params["natural_gas_actual"]

        km_metric = df.loc[:, usecols[28]].to_numpy() / self.params["km_actual"]

        if np.any(df.loc[:, usecols[30]] == "PORTAFOLIO_CASO_BASE"):
            capex = 0
        else:
            capex = df.loc[:, usecols[7]].to_numpy()

        vpn = df.loc[:,usecols[13]].to_numpy() / self.params["vpn_goal"]
        df2 = pd.DataFrame(
            {
                usecols[0]: df.loc[:, usecols[0]],
                usecols[14]: df.loc[:, usecols[14]],
                usecols[3]: df.loc[:, usecols[3]],
                usecols[11]: df.loc[:, usecols[11]],
                usecols[12]: df.loc[:, usecols[12]],
                goals[0]: co2_metric,
                goals[1]: water_metric,
                goals[2]: energy_metric,
                "EBITDA_BAJAS_EMISIONES": le_ebitda,
                goals[4]: ebitda_metric,
                goals[5]: gn_metric,
                goals[6]: gross_metric,
                goals[7]: vpn,
                usecols[16]: df.loc[:, usecols[16]],
                usecols[17]: df.loc[:, usecols[17]],
                goals[8]: cti_metric,
                goals[9]: noj_metric,
                goals[10]: students_metric,
                goals[11]: potable_water_metric,
                goals[12]: gas_metric,
                goals[13]: km_metric,
                usecols[4]: df.loc[:, usecols[4]],
                usecols[13]: df.loc[:, usecols[13]],
                usecols[18]: df.loc[:, usecols[18]],
                usecols[1]: df.loc[:, usecols[1]],
                usecols[7]: capex,
                usecols[20]: df.loc[:, usecols[20]],
                usecols[30]: df.loc[:, usecols[30]],
                usecols[21]: df.loc[:, usecols[21]],
                usecols[22]: df.loc[:, usecols[22]],
                usecols[19]: df.loc[:, usecols[19]],
            }
        )

        df2.fillna(0, inplace=True)

        return df2

    def get_outputs(self, df_results):
        df_project_goals = self.project_goals_initial_metrics(self.df, usecols[0])

        for i in range(6):  # range is number of portfolios generated by model
            # filter projects on
            projects = df_results.MATRICULA_DIGITAL[
                df_results.loc[:, f"PORTAFOLIO_{i+1}"].to_numpy(dtype="bool")
            ]

            # get goals of the portfolio
            df_goals_portfolio = pd.merge(
                left=self.df,
                right=self.get_metrics_depend_projects_on(self.df, projects),
                how="inner",
                on=[usecols[0], usecols[3]],
            )

            # goals per proyect of specific portfolio
            projects_goals_add = self.project_goals_depend_on_portfolio(
                df_goals_portfolio, i + 1, usecols[0]
            )
            df_project_goals = pd.merge(
                left=df_project_goals,
                right=projects_goals_add,
                how="left",
                on=[usecols[0]],
            ).fillna(0)
            df_axis = self.axis(df_project_goals, i + 1, usecols[0])
            df_project_goals = pd.merge(
                left=df_project_goals,
                right=df_axis,
                how="left",
                on=[usecols[0]],
            ).fillna(0)

        # end for

        return df_project_goals

    def get_metrics_depend_projects_on(self, df, projects):
        """
        This function calculate the metrics that depend on the projects of a portfolio
        :param df: DataFrame with the metrics per year and project to add the new metrics
        :param projects: list with the 'MATRICULA_DIGITAL' of projects from a portfolio
        :return: DataFrame with additional metrics per year and project
        """
        df2 = df.loc[df.loc[:, usecols[0]].isin(projects)]
        df_range = df2.loc[
            (df2.loc[:, usecols[3]] >= df2.loc[:, usecols[11]])
            & (df2.loc[:, usecols[3]] <= (df2.loc[:, usecols[12]]))
        ]
        df_group = df_range.groupby(usecols[3])

        ebitda_year = (
            df_group[usecols[4]].sum() * self.params["low_emissions_min"]
        ).tolist() * int(df2.shape[0] / 17)
        le_metric = df2.loc[:, "EBITDA_BAJAS_EMISIONES"] / (ebitda_year)

        df3 = pd.DataFrame(
            {
                usecols[0]: df2.loc[:, usecols[0]],
                usecols[3]: df2.loc[:, usecols[3]],
                goals[3]: le_metric
            }
        )

        return df3

    def project_goals_initial_metrics(self, df, column):
        """
        This function calculate the goals per project in the initial operation
        :param df: DataFrame with metrics per year and project
        :return: DataFrame with goals per projects
        """
        df_range = df.loc[
            (df.loc[:, usecols[3]] >= df.loc[:, usecols[11]])
            & (df.loc[:, usecols[3]] <= (df.loc[:, usecols[12]]))
            & (
                ~df.loc[:, usecols[0]].isin(
                    ["grc-PORTAFOLIO_CASO_BASE", "grb-PORTAFOLIO_CASO_BASE"]
                )
            )
        ].copy()

        cols = goals[:3] + goals[4:6] + goals[7:]

        if column == usecols[0]:
            matricula_not_duration = df.loc[
                (df.loc[:, usecols[11]] == 0) | (df.loc[:, usecols[12]] == 0),
                usecols[0],
            ].unique()
            df_not_duration = pd.DataFrame({usecols[0]: matricula_not_duration})
            df_range[cols] = df_range.loc[:, cols] * (100 / 3)
            df_range[goals[6]] = df_range.loc[:,goals[6]] * (3/100)
            df_temp = (
                df.groupby(usecols[0])[[usecols[19], usecols[21]]].min().reset_index()
            )
        cols.append(goals[6])
        df2 = df_range.groupby([usecols[3], column])[cols].sum().reset_index()

        df_group = df2.groupby(column)

        df2 = df_group[goals[:3]].mean()
        df3 = df_group[goals[4:8]].mean()
        cti = df_group[goals[8]].sum()
        df4 = df_group[goals[9:]].mean()

        df5 = df3.copy()
        df5.loc[df5.loc[:, goals[5]] < 0] = 0
        # Finance axle
        finance_axle_cols = goals[4:8]
        finance_axle = df5[finance_axle_cols].sum(axis=1) / 4
        finance_axle[finance_axle > 1] = 1
        finance_axle[finance_axle < 0] = 0

        df_goals = pd.concat(
            [
                df2,
                df3,
                cti.to_frame(),
                df4,
                pd.DataFrame({"EJE_FINANCIERO": finance_axle}),
            ],
            axis=1,
        )
        df_goals.reset_index(inplace=True)
        if column == usecols[0]:
            df_goals = pd.concat([df_goals, df_not_duration], ignore_index=True).fillna(
                0
            )
            df_goals = pd.merge(
                left=df_goals, right=df_temp, on=usecols[0], how="inner"
            )

        df_goals[goals[:2]] = 1 + df_goals.loc[:, goals[:2]]

        return df_goals

    def project_goals_depend_on_portfolio(self, df, i, column):
        """
        This function calculate the project goals that depend on portfolio
        :param df: DataFrame with metrics per year and project
        :param i: portfolio number in which goals are calculated
        :return: DataFrame with goals per projects
        """
        df_range = df.loc[
            (df.loc[:, usecols[3]] >= df.loc[:, usecols[11]])
            & (df.loc[:, usecols[3]] <= (df.loc[:, usecols[12]]))
        ].copy()
        df_range[goals[3]] = df_range.loc[:,goals[3]] * (100/3)
        df2 = df_range.groupby([usecols[3],column])[goals[3]].sum().reset_index()

        df_group = df_range.groupby(column)

        le = df_group[goals[3]].mean()
        # roace = df_group[goals[7]].mean()

        df2 = pd.DataFrame(
            {
                column: df_range.loc[:, column].unique(),
                f"{goals[3]}_{i}": le,
                # f"{goals[7]}_{i}": roace,
            }
        )

        return df2.reset_index(drop=True)

    def axis(self, df, i, column):
        """
        This function calculate the project axis of a portfolio
        :param df: DataFrame with metrics per project
        :param i: portfolio number in which goals are calculated
        :return: DataFrame with axis per project
        """

        # sostec axle per project
        sostec_axle_cols = goals[:3] + [f"{goals[3]}_{i}"]
        sostec_axle = df[sostec_axle_cols].sum(axis=1) / 4
        sostec_axle[sostec_axle > 1] = 1
        sostec_axle[sostec_axle < 0] = 0

        # create dataframe output
        df2 = pd.DataFrame(
            {
                column: df.loc[:, column],
                f"EJE_SOSTEC_{i}": sostec_axle,
            }
        )

        return df2
