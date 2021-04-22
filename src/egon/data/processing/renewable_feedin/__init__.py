"""
Central module containing all code dealing with processing era5 weather data.
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import egon.data.config
from egon.data import db
from egon.data.importing.era5 import import_cutout

def turbine_per_weather_cell():
    """Assign wind onshore turbine types to weather cells

    Returns
    -------
    weather_cells : GeoPandas.GeoDataFrame
        Weather cells in Germany including turbine type

    """

    cfg = egon.data.config.datasets()['renewable_feedin']['sources']

    # Select representative onshore wind turbines per federal state
    map_federal_states_turbines = {
        'Schleswig-Holstein': 'E-126',
        'Bremen': 'E-126',
        'Hamburg': 'E-126',
        'Mecklenburg-Vorpommern': 'E-126',
        'Niedersachsen': 'E-126',
        'Berlin': 'E-141',
        'Brandenburg': 'E-141',
        'Hessen': 'E-141',
        'Nordhrein-Westfalen': 'E-141',
        'Sachsen': 'E-141',
        'Sachsen-Anhalt': 'E-141',
        'Thüringen': 'E-141',
        'Baden-Württemberg': 'E-141',
        'Bayern': 'E-141',
        'Rheinland-Pfalz': 'E-141',
        'Saarland': 'E-141'
        }

    # Select weather cells and ferear states from database
    weather_cells = db.select_geodataframe(
        f"""SELECT w_id, geom_point
        FROM {cfg['weather_cells']['schema']}.
        {cfg['weather_cells']['table']}""",
        geom_col='geom_point', index_col='w_id', epsg=4326)

    federal_states = db.select_geodataframe(
        f"""SELECT gen, geometry
        FROM {cfg['vg250_lan_union']['schema']}.
        {cfg['vg250_lan_union']['table']}""",
        geom_col='geometry', index_col='gen', epsg=4326)

    # Map federal state and onshore wind turbine to weather cells
    weather_cells['federal_state'] = gpd.sjoin(
        weather_cells, federal_states).index_right

    weather_cells['wind_turbine'] = weather_cells['federal_state'].map(
        map_federal_states_turbines)

    return weather_cells

def feedin_per_turbine():
    """ Calculate feedin timeseries per turbine type and weather cell

    Returns
    -------
    gdf : GeoPandas.GeoDataFrame
        Feed-in timeseries per turbine type and weather cell

    """

    # Select weather data for Germany
    cutout = import_cutout(boundary='Germany')

    gdf = gpd.GeoDataFrame(geometry=cutout.grid_cells(), crs=4326)

    # Calculate feedin-timeseries for E-141
    # source: https://openenergy-platform.org/dataedit/view/supply/wind_turbine_library
    turbine_e141 = {
        'name': 'E141 4200 kW',
        'hub_height': 129,
        'P': 4.200,
        'V': np.arange(1, 26, dtype=float),
        'POW': np.array([
            0., 0.022, 0.104, 0.26, 0.523, 0.92, 1.471, 2.151, 2.867,
            3.481, 3.903, 4.119, 4.196, 4.2, 4.2, 4.2, 4.2,
            4.2, 4.2, 4.2, 4.2, 4.2, 4.2, 4.2, 4.2])
        }
    ts_e141 = cutout.wind(turbine_e141,
                          per_unit=True, shapes=cutout.grid_cells())

    gdf['E-141'] = ts_e141.to_pandas().values.tolist()

    # Calculate feedin-timeseries for E-126
    # source: https://openenergy-platform.org/dataedit/view/supply/wind_turbine_library
    turbine_e126 = {
        'name': 'E126 4200 kW',
        'hub_height': 159,
        'P': 4.200,
        'V': np.arange(1, 26, dtype=float),
        'POW': np.array([
            0., 0., 0.058, 0.185, 0.4, 0.745, 1.2, 1.79, 2.45,
            3.12, 3.66, 4.0, 4.15, 4.2, 4.2, 4.2, 4.2,
            4.2, 4.2, 4.2, 4.2, 4.2, 4.2, 4.2, 4.2])
        }
    ts_e126 = cutout.wind(turbine_e126,
                          per_unit=True, shapes=cutout.grid_cells())

    gdf['E-126'] = ts_e126.to_pandas().values.tolist()

    return gdf

def wind_feedin_per_weather_cell():
    """ Insert feed-in timeseries for wind onshore turbines to database

    Returns
    -------
    None.

    """

    cfg = egon.data.config.datasets()['renewable_feedin']['targets']

    # Get weather cells with turbine type
    weather_cells = turbine_per_weather_cell()
    weather_cells = weather_cells[weather_cells.wind_turbine.notnull()]

    # Calculate feedin timeseries per turbine and weather cell
    timeseries_per_turbine = feedin_per_turbine()

    # Join weather cells and feedin-timeseries
    timeseries = gpd.sjoin(
        weather_cells, timeseries_per_turbine)[['E-141', 'E-126']]

    df = pd.DataFrame(index=weather_cells.index,
                      columns=['weather_year', 'carrier', 'feedin'],
                      data={'weather_year':2011, 'carrier':'wind_onshore'})

    # Insert feedin for selected turbine per weather cell
    for turbine in ['E-126', 'E-141']:
        idx = weather_cells.index[weather_cells.wind_turbine==turbine]
        df.loc[idx, 'feedin'] = timeseries.loc[idx, turbine].values

    # Insert values into database
    df.to_sql(cfg['feedin_table']['table'],
              schema=cfg['feedin_table']['schema'],
              con=db.engine(),
              if_exists='append')
