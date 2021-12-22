# -*- coding: utf-8 -*-
"""
Module containing the definition of the open cycle gas turbine links
"""
from geoalchemy2.types import Geometry
from scipy.spatial import cKDTree
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.etrago_setup import link_geom_from_buses


def insert_open_cycle_gas_turbines(scn_name="eGon2035"):
    """Insert gas turbine links in egon_etrago_link table.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.
    """

    # Connect to local database
    engine = db.engine()

    # create bus connections
    gdf = map_buses(scn_name)

    # create topology column (linestring)
    gdf = link_geom_from_buses(gdf, scn_name)
    gdf["p_nom_extendable"] = False
    carrier = "OCGT"
    gdf["carrier"] = carrier

    buses = tuple(db.select_dataframe(
        f"""SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn_name}' AND country = 'DE';
        """
    )['bus_id'])

    # Delete old entries
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_link WHERE "carrier" = '{carrier}'
        AND scn_name = '{scn_name}'
        AND bus0 IN {buses} AND bus1 IN {buses};
        """
    )

    # Select next id value
    new_id = db.next_etrago_id("link")
    gdf["link_id"] = range(new_id, new_id + len(gdf))

    # Insert data to db
    gdf.to_postgis(
        "egon_etrago_link",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )


def map_buses(scn_name):
    """Map OCGT AC buses to nearest CH4 bus.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with connected buses.
    """
    # Create dataframes containing all gas buses and all the HV power buses
    sql_AC = f"""SELECT bus_id, el_capacity as p_nom, geom
                FROM supply.egon_power_plants
                WHERE carrier = 'gas' AND scenario = '{scn_name}';
                """
    sql_gas = f"""SELECT bus_id, scn_name, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'CH4' AND scn_name = '{scn_name}'
                AND country = 'DE';"""

    gdf_AC = db.select_geodataframe(sql_AC, epsg=4326)
    gdf_gas = db.select_geodataframe(sql_gas, epsg=4326)

    # Associate each power plant AC bus to nearest CH4 bus
    n_gas = np.array(list(gdf_gas.geometry.apply(lambda x: (x.x, x.y))))
    n_AC = np.array(list(gdf_AC.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(n_gas)
    dist, idx = btree.query(n_AC, k=1)
    gd_gas_nearest = (
        gdf_gas.iloc[idx]
        .rename(columns={"bus_id": "bus0", "geom": "geom_gas"})
        .reset_index(drop=True)
    )
    gdf = pd.concat([gdf_AC.reset_index(drop=True), gd_gas_nearest,], axis=1,)

    return gdf.rename(columns={"bus_id": "bus1"}).drop(
        columns=["geom", "geom_gas"]
    )
