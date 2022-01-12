"""The central module containing code to create CH4 and H2 voronoi polygones

"""
from numpy import dtype
import geopandas as gpd
from geovoronoi import voronoi_regions_from_coords

from egon.data import db
from egon.data.datasets import Dataset
from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, Integer, Sequence, Text

class GasAreas(Dataset):
     def __init__(self, dependencies):
         super().__init__(
             name="GasAreas",
             version="0.0.1",
             dependencies=dependencies,
             tasks=(create_voronoi),
         )

def get_voronoi_geodataframe(buses, boundary):
    """
    Creates voronoi polygons for the passed buses within the boundaries

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containting the bus_ids and the respective voronoi
        polygons.

    """
    buses = buses[buses.geometry.intersects(boundary)]

    coords = buses[["x", "y"]].values # coordinates of the respective buses

    region_polys, region_pts = voronoi_regions_from_coords(coords, boundary)

    gpd_input_dict= {
        'bus_id': [], # original bus_id in the buses dataframe
        'geometry': [] # voronoi object
    }

    for pt, poly in region_pts.items():
        gpd_input_dict['geometry'] += [region_polys[pt]]
        gpd_input_dict['bus_id'] += [buses.iloc[poly[0]]["bus_id"]]

    return gpd.GeoDataFrame(gpd_input_dict)


def create_voronoi(scn_name='eGon2035'):
    """
    Creates voronoi polygons for all gas carriers

    Returns
    -------
    None.
    """
    boundary = db.select_geodataframe(
        f"""
            SELECT id, geometry
            FROM boundaries.vg250_sta_union;
        """,
        geom_col="geometry"
    ).to_crs(epsg=4326)

    engine = db.engine()

    for carrier in ['CH4', 'H2_grid', 'H2_saltcavern']:
        db.execute_sql(
            f"""
            DROP TABLE IF EXISTS grid.egon_voronoi_{carrier.lower()} CASCADE;
            """
        )

        buses = db.select_geodataframe(
            f"""
                SELECT bus_id, geom
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}'
                AND country = 'DE'
                AND carrier = '{carrier}';
            """,
        ).to_crs(epsg=4326)

        buses['x'] = buses.geometry.x
        buses['y'] = buses.geometry.y
        # generate voronois
        gdf = get_voronoi_geodataframe(buses, boundary.geometry.iloc[0])
        # set scn_name
        gdf["scn_name"] = scn_name

        # Insert data to db
        gdf.set_crs(epsg=4326).to_postgis(
            f"egon_voronoi_{carrier.lower()}",
            engine,
            schema="grid",
            index=False,
            if_exists="append",
            dtype={"geometry": Geometry}
        )
