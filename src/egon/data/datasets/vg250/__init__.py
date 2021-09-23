"""The central module containing all code dealing with VG250 data.

This module either directly contains the code dealing with importing VG250
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from pathlib import Path
from urllib.request import urlretrieve
import time
import datetime
import codecs
import json
import os

from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
import egon.data.config
from egon.data.metadata import (context,
                                meta_metadata,
                                licenses_datenlizenz_deutschland)


def download_files():
    """Download VG250 (Verwaltungsgebiete) shape files."""
    data_config = egon.data.config.datasets()
    vg250_config = data_config["vg250"]["original_data"]

    download_directory = Path(".") / "vg250"
    # Create the folder, if it does not exists already
    if not os.path.exists(download_directory):
        os.mkdir(download_directory)

    target_file = download_directory / vg250_config["target"]["file"]

    if not os.path.isfile(target_file):
        urlretrieve(vg250_config["source"]["url"], target_file)


def to_postgres():

    # Get information from data configuraiton file
    data_config = egon.data.config.datasets()
    vg250_orig = data_config["vg250"]["original_data"]
    vg250_processed = data_config["vg250"]["processed"]

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {vg250_processed['schema']};")

    zip_file = Path(".") / "vg250" / vg250_orig["target"]["file"]
    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in vg250_processed["file_table_map"].items():
        # Open files and read .shp (within .zip) with geopandas
        data = gpd.read_file(
            f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
            f"vg250_ebenen_0101/{filename}"
        )

        boundary = settings()["egon-data"]["--dataset-boundary"]
        if boundary != "Everything":
            # read-in borders of federal state Schleswig-Holstein
            data_sta = gpd.read_file(
                f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
                f"vg250_ebenen_0101/VG250_LAN.shp"
            ).query(f"GEN == '{boundary}'")
            data_sta.BEZ = "Bundesrepublik"
            data_sta.NUTS = "DE"
            # import borders of Schleswig-Holstein as borders of state
            if table == "vg250_sta":
                data = data_sta
            # choose only areas in Schleswig-Holstein
            else:
                data = data[
                    data.within(data_sta.dissolve(by="GEN").geometry.values[0])
                ]

        # Set index column and format column headings
        data.index.set_names("id", inplace=True)
        data.columns = [x.lower() for x in data.columns]

        # Drop table before inserting data
        db.execute_sql(
            f"DROP TABLE IF EXISTS "
            f"{vg250_processed['schema']}.{table} CASCADE;"
        )

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema=vg250_processed["schema"],
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        db.execute_sql(
            f"ALTER TABLE {vg250_processed['schema']}.{table} "
            f"ADD PRIMARY KEY (id);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON "
            f"{vg250_processed['schema']}.{table} USING gist (geometry);"
        )


def add_metadata():
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    vg250_config = egon.data.config.datasets()["vg250"]

    title_and_description = {
        "vg250_sta": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Staat (STA)",
            "description": "Staatsgrenzen der Bundesrepublik Deutschland",
        },
        "vg250_lan": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Länder (LAN)",
            "description": "Landesgrenzen der Bundesländer in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_rbz": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Regierungsbezirke "
            "(RBZ)",
            "description": "Grenzen der Regierungsbezirke in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_krs": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Kreise (KRS)",
            "description": "Grenzen der Landkreise in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_vwg": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - "
            "Verwaltungsgemeinschaften (VWG)",
            "description": "Grenzen der Verwaltungsgemeinschaften in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_gem": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Gemeinden (GEM)",
            "description": "Grenzen der Gemeinden in der "
            "Bundesrepublik Deutschland",
        },
    }

    url = vg250_config["original_data"]["source"]["url"]

    # Insert metadata for each table
    licenses = [licenses_datenlizenz_deutschland(
        attribution=f"© GeoBasis-DE / BKG (2020)"
    )]

    vg250_source = {
        "title": "Verwaltungsgebiete 1:250 000 (Ebenen)",
        "description":
            "Der Datenbestand umfasst sämtliche Verwaltungseinheiten der "
            "hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden "
            "mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der "
            "Verwaltungseinheit sowie die spezifische Bezeichnung der "
            "Verwaltungsebene des jeweiligen Landes.",
        "path": url,
        "licenses": licenses
    }

    resource_fields = [
        {'description': 'Index', 'name': 'gid', 'type': 'integer', 'unit': None},
        {'description': 'Administrative level', 'name': 'ade', 'type': 'integer', 'unit': None},
        {'description': 'Geofactor', 'name': 'gf', 'type': 'integer', 'unit': None},
        {'description': 'Particular areas', 'name': 'bsg', 'type': 'integer', 'unit': None},
        {'description': 'Territorial code', 'name': 'ars', 'type': 'string', 'unit': None},
        {'description': 'Official Municipality Key', 'name': 'ags', 'type': 'string', 'unit': None},
        {'description': 'Seat of the administration (territorial code)', 'name': 'sdv_ars', 'type': 'string', 'unit': None},
        {'description': 'Geographical name', 'name': 'gen', 'type': 'string', 'unit': None},
        {'description': 'Designation of the administrative unit', 'name': 'bez', 'type': 'string', 'unit': None},
        {'description': 'Identifier', 'name': 'ibz', 'type': 'integer', 'unit': None},
        {'description': 'Note', 'name': 'bem', 'type': 'string', 'unit': None},
        {'description': 'Name generation', 'name': 'nbd', 'type': 'string', 'unit': None},
        {'description': 'Land (state)', 'name': 'sn_l', 'type': 'string', 'unit': None},
        {'description': 'Administrative district', 'name': 'sn_r', 'type': 'string', 'unit': None},
        {'description': 'District', 'name': 'sn_k', 'type': 'string', 'unit': None},
        {'description': 'Administrative association – front part', 'name': 'sn_v1', 'type': 'string', 'unit': None},
        {'description': 'Administrative association – rear part', 'name': 'sn_v2', 'type': 'string', 'unit': None},
        {'description': 'Municipality', 'name': 'sn_g', 'type': 'string', 'unit': None},
        {'description': 'Function of the 3rd key digit', 'name': 'fk_s3', 'type': 'string', 'unit': None},
        {'description': 'European statistics key', 'name': 'nuts', 'type': 'string', 'unit': None},
        {'description': 'Filled territorial code', 'name': 'ars_0', 'type': 'string', 'unit': None},
        {'description': 'Filled Official Municipality Key', 'name': 'ags_0', 'type': 'string', 'unit': None},
        {'description': 'Effectiveness', 'name': 'wsk', 'type': 'string', 'unit': None},
        {'description': 'DLM identifier', 'name': 'debkg_id', 'type': 'string',
         'unit': None},
        {'description': 'Territorial code (deprecated column)', 'name': 'rs', 'type': 'string', 'unit': None},
        {'description': 'Seat of the administration (territorial code, deprecated column)', 'name': 'sdv_rs', 'type': 'string', 'unit': None},
        {'description': 'Filled territorial code (deprecated column)', 'name': 'rs_0', 'type': 'string', 'unit': None},
        {'description': 'Geometry of areas as WKB',
         'name': 'geometry',
         'type': "Geometry(Polygon, srid=4326)",
         'unit': None}]

    for table in vg250_config["processed"]["file_table_map"].values():
        schema_table = ".".join([vg250_config["processed"]["schema"], table])
        meta = {
            "name": schema_table,
            "title": title_and_description[table]["title"],
            "id": "WILL_BE_SET_AT_PUBLICATION",
            "description": title_and_description[table]["title"],
            "language": ["de-DE"],
            "publicationDate": datetime.date.today().isoformat(),
            "context": context(),
            "spatial": {
                "location": None,
                "extent": "Germany",
                "resolution": "1:250000",
            },
            "temporal": {
                "referenceDate": "2020-01-01",
                "timeseries": {
                    "start": None,
                    "end": None,
                    "resolution": None,
                    "alignment": None,
                    "aggregationType": None,
                },
            },
            "sources": [vg250_source],
            "licenses": licenses,
            "contributors": [
                {
                    "title": "Guido Pleßmann",
                    "email": "http://github.com/gplssm",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": None,
                    "comment": "Imported data",
                },
                {
                    "title": "Jonathan Amme",
                    "email": "http://github.com/nesnoj",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": None,
                    "comment": "Metadata extended",
                }
            ],
            "resources": [
                {
                    "profile": "tabular-data-resource",
                    "name": schema_table,
                    "path": None,
                    "format": "PostgreSQL",
                    "encoding": "UTF-8",
                    "schema": {
                        "fields": resource_fields,
                        "primaryKey": ["gid"],
                        "foreignKeys": None
                    },
                    "dialect": {
                        "delimiter": None,
                        "decimalSeparator": "."
                    }
                }
            ],
            "metaMetadata": meta_metadata(),
        }

        meta_json = "'" + json.dumps(meta) + "'"

        db.submit_comment(
            meta_json, vg250_config["processed"]["schema"], table
        )


def nuts_mview():

    db.execute_sql_script(
        os.path.join(os.path.dirname(__file__), "vg250_lan_nuts_id_mview.sql")
    )


def cleaning_and_preperation():

    db.execute_sql_script(
        os.path.join(os.path.dirname(__file__), "cleaning_and_preparation.sql")
    )


class Vg250(Dataset):

    filename = egon.data.config.datasets()["vg250"]["original_data"]["source"][
        "url"
    ]

    def __init__(self, dependencies):
        super().__init__(
            name="VG250",
            version=self.filename + "-0.0.3",
            dependencies=dependencies,
            tasks=(
                download_files,
                to_postgres,
                nuts_mview,
                add_metadata,
                cleaning_and_preperation,
            ),
        )
