
"""
This module does sanity checks for eGon100RE scenario where a percentage
error is given to showcase difference in output and input values. Please note that there are missing input technologies in the supply tables.
 Authors:  @dana
"""

import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand.temporal import insert_cts_load
import egon.data.config


class SanityChecks(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SanityChecks",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                #sanitycheck_eGon100RE_electricity,
                #sanitycheck_eGon100RE_electricity_storage,
                #sanitycheck_eGon100RE_heat_generator,
                #sanitycheck_eGon100RE_heat_link,
            ),
        )


def sanitycheck_eGon100RE_electricity():

    """Returns sanity checks for eGon100RE scenario.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    carriers_electricity = ["onwind", "solar", "solar rooftop", "ror"]
    for carrier in carriers_electricity:
        sum_output = db.select_dataframe(
            f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
                      FROM grid.egon_etrago_generator
                      WHERE scn_name = 'eGon100RE'
                      AND carrier IN ('{carrier}')
                      GROUP BY (scn_name);
            """
        )
        if carrier == "onwind":
            carrier = "wind_onshore"

        elif carrier == "solar rooftop":
            carrier = "solar_rooftop"

        elif carrier == "solar":
            carrier = "solar"
        elif carrier == "ror":
            carrier = "run_of_river"

        sum_input = db.select_dataframe(
            f""" SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
                    FROM supply.egon_scenario_capacities
                    WHERE carrier= '{carrier}'
                    AND scenario_name IN ('eGon100RE')
                    GROUP BY (carrier);
            """
        )

        sum_input["Error"] = (
            (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
            / sum_input["input_capacity_mw"]
        ) * 100

        g1 = sum_input["Error"].values[0]
        g = round(g1, 2)

        print(f"The target values for {carrier} differ by {g}  %")

    # For_offwind_total

    carriers_electricity = ["offwind-dc", "offwind-ac"]
    for carrier in carriers_electricity:
        if carrier == "offwind-dc" or "offwind-ac":
            sum_output = db.select_dataframe(
            """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
                  FROM grid.egon_etrago_generator
                  WHERE scn_name = 'eGon100RE'
                  AND carrier IN ('offwind-dc','offwind-ac')
                  GROUP BY (scn_name);
            """
            )

        carrier = "wind_offshore"
        sum_input = db.select_dataframe(
            """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
                FROM supply.egon_scenario_capacities
                WHERE carrier= ('wind_offshore')
                AND scenario_name IN ('eGon100RE')
                GROUP BY (carrier);
            """
        )

        sum_input["Error"] = (
            (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
            / sum_input["input_capacity_mw"]
        ) * 100

        g1 = sum_input["Error"].values[0]
        g = round(g1, 2)

        print(f"The target values for {carrier} differ by {g}  %")


def sanitycheck_eGon100RE_electricity_storage():

    """Returns sanity checks for heat.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    carriers_Heating_storage_units = ["hydro", "PHS"]
    for carrier in carriers_Heating_storage_units:
        sum_output = db.select_dataframe(
            f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
                  FROM grid.egon_etrago_storage
                  WHERE scn_name = 'eGon100RE'
                  AND carrier IN ('{carrier}')
                  GROUP BY (scn_name);
            """
        )
    if carrier == "hydro":
        carrier = "hydro"

    elif carrier == "PHS":
        carrier = "pumped_hydro"

    sum_input = db.select_dataframe(
        f"""SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
              FROM supply.egon_scenario_capacities
              WHERE carrier= '{carrier}'
              AND scenario_name IN ('eGon100RE')
              GROUP BY (carrier);
        """
    )

    sum_input["error"] = (
        (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
        / sum_input["input_capacity_mw"]
    ) * 100

    g1 = sum_input["error"].values[0]
    g = round(g1, 2)
    print(f"The target values for {carrier} differ by {g}  %")

    # Sanity_checks_eGon100RE_Heating


def sanitycheck_eGon100RE_heat_generator():

    """Returns sanity checks for heat.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    # Urban_central_solar_thermal

    sum_output_urban_central_solar_thermal = db.select_dataframe(
        """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
            FROM grid.egon_etrago_generator
            WHERE scn_name = 'eGon100RE'
            AND carrier IN ('urban central solar thermal')
            GROUP BY (scn_name);
      """
    )

    sum_input_urban_central_solar_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= ('urban_central_solar_thermal')
            AND scenario_name IN ('eGon100RE')
            GROUP BY (carrier);
        """
    )

    sum_input_urban_central_solar_thermal["Error"] = (
        (
            sum_output_urban_central_solar_thermal["output_capacity_mw"]
            - sum_input_urban_central_solar_thermal["input_capacity_mw"]
        )
        / sum_input_urban_central_solar_thermal["input_capacity_mw"]
    ) * 100

    g1 = sum_input_urban_central_solar_thermal["Error"].values[0]
    g = round(g1, 2)
    print(
        f"The target values for urban central solar thermal differ by {g}  %"
    )

    # Urban_central_Geo_thermal

    sum_output_urban_central_geo_thermal = db.select_dataframe(
        """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
            FROM grid.egon_etrago_generator
            WHERE scn_name = 'eGon100RE'
            AND carrier IN ('urban central geo thermal')
            GROUP BY (scn_name);
        """
    )

    sum_input_urban_central_geo_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= ('urban_central_geo_thermal')
            AND scenario_name IN ('eGon100RE')
            GROUP BY (carrier);
        """
    )

    sum_input_urban_central_geo_thermal["Error"] = (
        (
            sum_output_urban_central_geo_thermal["output_capacity_mw"]
            - sum_input_urban_central_geo_thermal["input_capacity_mw"]
        )
        / sum_input_urban_central_geo_thermal["input_capacity_mw"]
    ) * 100

    g1 = sum_input_urban_central_geo_thermal["Error"].values[0]
    g = round(g1, 2)

    print(f"The target values for urban central geo thermal differ by {g}  %")

    # For_residential_rural_solar_thermal+service_rural_solar_thermal=rural_solar_thermal

    sum_output_rural_solar_thermal = db.select_dataframe(
        """
        SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
        FROM grid.egon_etrago_generator
        WHERE scn_name = 'eGon100RE'
        AND carrier IN ('residential rural solar thermal', 'service rural solar thermal')
        GROUP BY (scn_name);
        """
    )

    sum_input_rural_solar_thermal = db.select_dataframe(
        """
        SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
        FROM supply.egon_scenario_capacities
        WHERE carrier= ('rural_solar_thermal)
        AND scenario_name IN ('eGon100RE')
        GROUP BY (carrier);
        """
    )

    sum_input_rural_solar_thermal["Error"] = (
        (
            sum_output_rural_solar_thermal["output_capacity_mw"]
            - sum_input_rural_solar_thermal["input_capacity_mw"]
        )
        / sum_input_rural_solar_thermal["input_capacity_mw"]
    ) * 100

    g1 = sum_input_rural_solar_thermal["Error"].values[0]
    g = round(g1, 2)

    print(f"The target values for rural solar thermal differ by {g}  %")


def sanitycheck_eGon100RE_heat_link():

    """Returns sanity checks for heat.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    carriers_Heating_link = [
        "urban central air heat pump",
        "urban central resistive heater",
        "services rural resistive heater",
        "urban_gas",
    ]
    for carrier in carriers_Heating_link:
        sum_output = db.select_dataframe(
            f"""
                  SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
                  FROM grid.egon_etrago_link
                  WHERE scn_name = 'eGon100RE'
                  AND carrier IN ('{carrier}')
                  GROUP BY (scn_name);
              """
        )
    if sum_output.shape[0] == 0:
        print(f"{carrier} is not distributed correctly, please revise")

    elif carrier == "urban central air heat pump":
        carrier = "urban_central_air_heat_pump"

    elif carrier == "urban central resistive heater":
        carrier = "urban_central_resistive_heater"

    elif carrier == "services rural resistive heater":
        carrier = "rural_resistive_heater"

    sum_input = db.select_dataframe(
        f"""
            SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= '{carrier}'
            AND scenario_name IN ('eGon100RE')
            GROUP BY (carrier);
        """
    )

    sum_output["Error"] = (
        (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
        / sum_input["input_capacity_mw"]
    ) * 100

    g1 = sum_output["Error"].values[0]
    g = round(g1, 2)

    print(f"The target values for {carrier} differ by {g}  %")

    # Heat_Pump_to_be_added

    sum_output_heat_pump = db.select_dataframe(
        """
        SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
        FROM grid.egon_etrago_link
        WHERE scn_name = 'eGon100RE'
        AND carrier IN ('heat_pump')
        GROUP BY (scn_name);
    """
    )

    sum_input_heat_pump = db.select_dataframe(
        """
        SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
        FROM supply.egon_scenario_capacities
        WHERE carrier= ('rural_heat_pump')
        AND scenario_name IN ('eGon100RE')
        GROUP BY (carrier);
        """
    )

    sum_input_heat_pump["Error"] = (
        (
            sum_output_heat_pump["output_capacity_mw"]
            - sum_input_heat_pump["input_capacity_mw"]
        )
        / sum_input_heat_pump["input_capacity_mw"]
    ) * 100

    g1 = sum_input_heat_pump["Error"].values[0]
    g = round(g1, 2)

    print(f"The target values for rural heat pump differ by {g}  %")