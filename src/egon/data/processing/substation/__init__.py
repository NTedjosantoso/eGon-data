"""The central module containing code to create substation tables

"""

import egon.data.config
from egon.data import db
from sqlalchemy import Column, Float, Integer, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry

Base = declarative_base()


class EgonEhvSubstation(Base):
    __tablename__ = 'egon_ehv_substation'
    __table_args__ = {'schema': 'grid'}
    subst_id = Column(Integer, Sequence('ehv_id_seq'), primary_key=True)
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry('POINT', 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)


class EgonHvmvSubstation(Base):
    __tablename__ = 'egon_hvmv_substation'
    __table_args__ = {'schema': 'grid'}
    subst_id = Column(Integer, Sequence('hvmv_id_seq'), primary_key=True)
    lon = Column(Float(53))
    lat = Column(Float(53))
    point = Column(Geometry('POINT', 4326), index=True)
    polygon = Column(Geometry)
    voltage = Column(Text)
    power_type = Column(Text)
    substation = Column(Text)
    osm_id = Column(Text)
    osm_www = Column(Text)
    frequency = Column(Text)
    subst_name = Column(Text)
    ref = Column(Text)
    operator = Column(Text)
    dbahn = Column(Text)
    status = Column(Integer)

def create_tables():
    """Create tables for substation data
    Returns
    -------
    None.
    """
    cfg_ehv = egon.data.config.datasets()['ehv_substation']
    cfg_hvmv = egon.data.config.datasets()['hvmv_substation']
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg_ehv['processed']['schema']};")

    db.execute_sql(f"""DROP TABLE IF EXISTS {cfg_ehv['processed']['schema']}.
                   {cfg_ehv['processed']['table']} CASCADE;""")

    db.execute_sql(f"""DROP TABLE IF EXISTS {cfg_hvmv['processed']['schema']}.
                   {cfg_hvmv['processed']['table']} CASCADE;""")

    engine = db.engine()
    EgonEhvSubstation.__table__.create(bind=engine, checkfirst=True)
    EgonHvmvSubstation.__table__.create(bind=engine, checkfirst=True)

def create_sql_functions():
    """Defines Postgresql functions needed to extract substation from osm

    Returns
    -------
    None.

    """

    # Create function: utmzone(geometry)
    # source: http://www.gistutor.com/postgresqlpostgis/6-advanced-postgresqlpostgis-tutorials/58-postgis-buffer-latlong-and-other-projections-using-meters-units-custom-stbuffermeters-function.html
    db.execute_sql(
        """
        DROP FUNCTION IF EXISTS utmzone(geometry) CASCADE;
        CREATE OR REPLACE FUNCTION utmzone(geometry)
        RETURNS integer AS
        $BODY$
        DECLARE
        geomgeog geometry;
        zone int;
        pref int;

        BEGIN
        geomgeog:= ST_Transform($1,4326);

        IF (ST_Y(geomgeog))>0 THEN
        pref:=32600;
        ELSE
        pref:=32700;
        END IF;

        zone:=floor((ST_X(geomgeog)+180)/6)+1;

        RETURN zone+pref;
        END;
        $BODY$ LANGUAGE 'plpgsql' IMMUTABLE
        COST 100;
        """
    )

    # Create function: relation_geometry
    # Function creates a geometry point from relation parts of type way

    db.execute_sql(
        """
        DROP FUNCTION IF EXISTS relation_geometry (members text[]) CASCADE;
        CREATE OR REPLACE FUNCTION relation_geometry (members text[])
        RETURNS geometry
        AS $$
        DECLARE
        way  geometry;
        BEGIN
            way = (SELECT ST_SetSRID
                   (ST_MakePoint((max(lon) + min(lon))/200.0,(max(lat) + min(lat))/200.0),900913)
                   FROM openstreetmap.osm_deu_nodes
                   WHERE id in (SELECT unnest(nodes)
                     FROM openstreetmap.osm_deu_ways
                     WHERE id in (SELECT trim(leading 'w' from member)::bigint
			                     FROM (SELECT unnest(members) as member) t
	                               WHERE member~E'[w,1,2,3,4,5,6,7,8,9,0]')));
        RETURN way;
        END;
        $$ LANGUAGE plpgsql;
        """
        )

    # Create function: ST_Buffer_Meters(geometry, double precision)

    db.execute_sql(
        """
        DROP FUNCTION IF EXISTS ST_Buffer_Meters(geometry, double precision) CASCADE;
        CREATE OR REPLACE FUNCTION ST_Buffer_Meters(geometry, double precision)
        RETURNS geometry AS
        $BODY$
        DECLARE
        orig_srid int;
        utm_srid int;

        BEGIN
        orig_srid:= ST_SRID($1);
        utm_srid:= utmzone(ST_Centroid($1));

        RETURN ST_transform(ST_Buffer(ST_transform($1, utm_srid), $2), orig_srid);
        END;
        $BODY$ LANGUAGE 'plpgsql' IMMUTABLE
        COST 100;
        """
        )


