"""
The module containing all code allocating power plants of different conventional
technologies (oil, gas, other_non_renewable) based on
data from MaStR and NEP.
"""

import pandas as pd
import geopandas as gpd
import egon.data.config
from egon.data import db, config
from egon.data.datasets.power_plants import (
    assign_voltage_level,
    EgonPowerPlants,
)
from egon.data.datasets.chp.match_nep import map_carrier_matching
from sqlalchemy.orm import sessionmaker


def select_nep_power_plants(carrier):
    """ Select waste power plants with location from NEP's list of power plants

    Parameters
    ----------
    carrier : str
        Name of energy carrier

    Returns
    -------
    pandas.DataFrame
        Waste power plants from NEP list

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # Select plants with geolocation from list of conventional power plants
    nep = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, carrier, capacity, postcode, city,
        federal_state, c2035_capacity
        FROM {cfg['sources']['nep_conv']}
        WHERE carrier = '{carrier}'
        AND chp = 'Nein'
        AND c2035_chp = 'Nein'
        AND c2035_capacity > 0
        AND postcode != 'None';
        """
    )

    # Removing plants out of Germany
    nep["postcode"] = nep["postcode"].astype(str)
    nep = nep[~nep["postcode"].str.contains("A")]
    nep = nep[~nep["postcode"].str.contains("L")]
    nep = nep[~nep["postcode"].str.contains("nan")]

    # Remove the subunits from the bnetza_id
    nep["bnetza_id"] = nep["bnetza_id"].str[0:7]

    return nep


def select_no_chp_combustion_mastr(carrier):
    """ Select power plants of a certain carrier from MaStR data which excludes
    all power plants used for allocation of CHP plants.

    Parameters
    ----------
    carrier : str
        Name of energy carrier

    Returns
    -------
    pandas.DataFrame
        Power plants from NEP list

    """
    cfg = egon.data.config.datasets()["power_plants"]
    # import data for MaStR
    mastr = db.select_geodataframe(
        f"""
        SELECT  "EinheitMastrNummer",
                el_capacity,
                ST_setSRID(geometry, 4326) as geometry,
                carrier
            FROM {cfg['sources']['mastr_combustion_without_chp']}
            WHERE carrier = '{carrier}';
        """,
        index_col=None,
        geom_col="geometry",
        epsg=4326,
    )

    # Temporary code to add plz and city to mastr df from bnetza_mastr_combustion_cleaned.csv

    sources = config.datasets()["chp_location"]["sources"]
    # Read-in data from MaStR
    MaStR_konv = pd.read_csv(
        sources["mastr_combustion"],
        delimiter=",",
        usecols=["EinheitMastrNummer", "Postleitzahl", "Ort"],
    )

    # Rename columns
    MaStR_konv = MaStR_konv.rename(
        columns={"Postleitzahl": "plz", "Ort": "city"}
    )

    # Merge data together
    mastr.EinheitMastrNummer = mastr.EinheitMastrNummer.str.slice(stop=15)
    mastr = mastr.merge(
        MaStR_konv,
        how="left",
        left_on=["EinheitMastrNummer"],
        right_on=["EinheitMastrNummer"],
    )

    # Import geodataframe on federal states and their borders

    fs = db.select_geodataframe(
        f"""
        SELECT  gen,
                geometry
            FROM {cfg['sources']['geom_federal_states']}
            WHERE gf = 4;
        """,
        index_col=None,
        geom_col="geometry",
        epsg=4326,
    )

    # Spatial join to add name of federal state to dataframe containing MaStR data

    mastr = (
        gpd.sjoin(mastr, fs, how="inner", op="intersects")
        .drop(columns=["index_right"])
        .rename(columns={"gen": "federal_state"})
    )

    return mastr


def match_nep_no_chp(
    nep,
    mastr,
    matched,
    buffer_capacity=0.1,
    consider_location="plz",
    consider_carrier=True,
    consider_capacity=True,
):
    """ Match Power plants (no CHP) from MaStR to list of power plants from NEP

    Parameters
    ----------
    nep : pandas.DataFrame
        Power plants (no CHP) from NEP which are not matched to MaStR
    mastr : pandas.DataFrame
        Power plants (no CHP) from MaStR which are not matched to NEP
    matched : pandas.DataFrame
        Already matched power plants
    buffer_capacity : float, optional
        Maximum difference in capacity in p.u. The default is 0.1.

    Returns
    -------
    matched : pandas.DataFrame
        Matched CHP
    mastr : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    nep : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR

    """

    list_federal_states = pd.Series(
        {
            "Hamburg": "HH",
            "Sachsen": "SN",
            "Mecklenburg-Vorpommern": "MV",
            "Thüringen": "TH",
            "Schleswig-Holstein": "SH",
            "Bremen": "HB",
            "Saarland": "SL",
            "Bayern": "BY",
            "Baden-Württemberg": "BW",
            "Brandenburg": "BB",
            "Hessen": "HE",
            "Nordrhein-Westfalen": "NW",
            "Berlin": "BE",
            "Niedersachsen": "NI",
            "Sachsen-Anhalt": "ST",
            "Rheinland-Pfalz": "RP",
        }
    )

    for ET in nep["carrier"].unique():

        map_carrier = map_carrier_matching()

        for index, row in nep[
            (nep["carrier"] == ET) & (nep["postcode"] != "None")
        ].iterrows():

            # Select plants from MaStR that match carrier, PLZ
            # and have a similar capacity
            # Create a copy of all power plants from MaStR
            selected = mastr.copy()

            # Set capacity constraint using buffer
            if consider_capacity:
                selected = selected[
                    (
                        selected.el_capacity
                        <= row["capacity"] * (1 + buffer_capacity)
                    )
                    & (
                        selected.el_capacity
                        >= row["capacity"] * (1 - buffer_capacity)
                    )
                ]

            # Set geographic constraint, either choose power plants
            # with the same postcode, city or federal state
            if consider_location == "plz":
                selected = selected[
                    selected.plz.astype(int).astype(str) == row["postcode"]
                ]
            elif consider_location == "city":
                selected = selected[
                    selected.city == row.city.replace("\n", " ")
                ]
            elif consider_location == "federal_state":
                selected.loc[:, "federal_state"] = list_federal_states[
                    selected.federal_state
                ].values
                selected = selected[
                    selected.federal_state == row.federal_state
                ]

            # Set capacity constraint if selected
            if consider_carrier:
                selected = selected[selected.carrier.isin(map_carrier[ET])]

            # If a plant could be matched, add this to matched
            if len(selected) > 0:
                matched = matched.append(
                    gpd.GeoDataFrame(
                        data={
                            "source": "MaStR scaled with NEP 2021 list",
                            "MaStRNummer": selected.EinheitMastrNummer.head(1),
                            "carrier": ET,
                            "el_capacity": row.c2035_capacity,
                            "scenario": "eGon2035",
                            "geometry": selected.geometry.head(1),
                            "voltage_level": selected.voltage_level.head(1),
                        }
                    )
                )

                # Drop matched power plant from nep
                nep = nep.drop(index)

                # Drop matched powerplant from MaStR list if the location is accurate
                if consider_capacity & consider_carrier:
                    mastr = mastr.drop(selected.index)

    return matched, mastr, nep


def allocate_conventional_non_chp_power_plants():

    carrier = ["oil", "gas", "other_non_renewable"]

    cfg = egon.data.config.datasets()["power_plants"]

    for carrier in carrier:

        nep = select_nep_power_plants(carrier)
        mastr = select_no_chp_combustion_mastr(carrier)

        # Assign voltage level to MaStR
        mastr["voltage_level"] = assign_voltage_level(
            mastr.rename({"el_capacity": "Nettonennleistung"}, axis=1), cfg
        )

        # Initalize DataFrame for matching power plants
        matched = gpd.GeoDataFrame(
            columns=[
                "carrier",
                "el_capacity",
                "scenario",
                "geometry",
                "MaStRNummer",
                "source",
                "voltage_level",
            ]
        )

        # Match combustion plants of a certain carrier from NEP list
        # using PLZ and capacity
        matched, mastr, nep = match_nep_no_chp(
            nep, mastr, matched, buffer_capacity=0.1, consider_carrier=False
        )

        # Match plants from NEP list using city and capacity
        matched, mastr, nep = match_nep_no_chp(
            nep,
            mastr,
            matched,
            buffer_capacity=0.1,
            consider_carrier=False,
            consider_location="city",
        )

        # Match plants from NEP list using plz,
        # neglecting the capacity
        matched, mastr, nep = match_nep_no_chp(
            nep,
            mastr,
            matched,
            consider_location="plz",
            consider_carrier=False,
            consider_capacity=False,
        )

        # Match plants from NEP list using city,
        # neglecting the capacity
        matched, mastr, nep = match_nep_no_chp(
            nep,
            mastr,
            matched,
            consider_location="city",
            consider_carrier=False,
            consider_capacity=False,
        )

        # Match remaining plants from NEP using the federal state
        matched, mastr, nep = match_nep_no_chp(
            nep,
            mastr,
            matched,
            buffer_capacity=0.1,
            consider_location="federal_state",
            consider_carrier=False,
        )

        # Match remaining plants from NEP using the federal state
        matched, mastr, nep = match_nep_no_chp(
            nep,
            mastr,
            matched,
            buffer_capacity=0.7,
            consider_location="federal_state",
            consider_carrier=False,
        )

        print(f"{matched.el_capacity.sum()} MW of {carrier} matched")
        print(f"{nep.c2035_capacity.sum()} MW of {carrier} not matched")

        matched.crs = "EPSG:4326"

        # Assign bus_id
        # Load grid district polygons
        mv_grid_districts = db.select_geodataframe(
            f"""
        SELECT * FROM {cfg['sources']['mv_grid_districts']}
        """,
            epsg=4326,
        )

        ehv_grid_districts = db.select_geodataframe(
            f"""
        SELECT * FROM {cfg['sources']['ehv_voronoi']}
        """,
            epsg=4326,
        )

        # Perform spatial joins for plants in ehv and hv level seperately
        power_plants_hv = gpd.sjoin(
            matched[matched.voltage_level >= 3],
            mv_grid_districts[["bus_id", "geom"]],
            how="left",
        ).drop(columns=["index_right"])
        power_plants_ehv = gpd.sjoin(
            matched[matched.voltage_level < 3],
            ehv_grid_districts[["bus_id", "geom"]],
            how="left",
        ).drop(columns=["index_right"])

        # Combine both dataframes
        power_plants = pd.concat([power_plants_hv, power_plants_ehv])

        # Delete existing CHP in the target table
        db.execute_sql(
            f""" DELETE FROM {cfg ['target']['schema']}.{cfg ['target']['table']}
            WHERE carrier IN ('gas', 'other_non_renewable', 'oil')
            AND scenario='eGon2035';"""
        )

        # Insert into target table
        session = sessionmaker(bind=db.engine())()
        for i, row in power_plants.iterrows():
            entry = EgonPowerPlants(
                sources={"el_capacity": row.source},
                source_id={"MastrNummer": row.MaStRNummer},
                carrier=row.carrier,
                el_capacity=row.el_capacity,
                voltage_level=row.voltage_level,
                bus_id=row.bus_id,
                scenario=row.scenario,
                geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
            )
            session.add(entry)
        session.commit()
