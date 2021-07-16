"""The central module containing all code dealing with heat supply data

"""

from egon.data import db, config

from egon.data.datasets.heat_supply.district_heating import (
    cascade_heat_supply)
from egon.data.datasets.heat_supply.individual_heating import (
    cascade_heat_supply_indiv)
from egon.data.datasets.heat_supply.geothermal import (
    potential_germany)
from egon.data.processing.district_heating_areas import DistrictHeatingAreas
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry
from egon.data.datasets import Dataset
### will be later imported from another file ###
Base = declarative_base()

# TODO: set district_heating_id as ForeignKey
class EgonDistrictHeatingSupply(Base):
    __tablename__ = 'egon_district_heating'
    __table_args__ = {'schema': 'supply'}
    index = Column(Integer, primary_key=True)
    district_heating_id = Column(Integer)
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry('POINT', 3035))
    scenario = Column(String(50))

class EgonIndividualHeatingSupply(Base):
    __tablename__ = 'egon_individual_heating'
    __table_args__ = {'schema': 'supply'}
    index = Column(Integer, primary_key=True)
    mv_grid_id = Column(Integer)
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry('POINT', 3035))
    scenario = Column(String(50))

def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonDistrictHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonDistrictHeatingSupply.__table__.create(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.create(bind=engine, checkfirst=True)


def district_heating():
    """ Insert supply for district heating areas

    Returns
    -------
    None.

    """
    targets = config.datasets()['heat_supply']['targets']

    db.execute_sql(
        f"""
        DELETE FROM {targets['district_heating_supply']['schema']}.
        {targets['district_heating_supply']['table']}
        """)

    supply_2035 = cascade_heat_supply('eGon2035', plotting=False)

    supply_2035['scenario'] = 'eGon2035'

    supply_2035.to_postgis(
        targets['district_heating_supply']['table'],
        schema=targets['district_heating_supply']['schema'],
        con=db.engine(), if_exists='append')

def individual_heating():
    """ Insert supply for individual heating

    Returns
    -------
    None.

    """
    targets = config.datasets()['heat_supply']['targets']

    db.execute_sql(
        f"""
        DELETE FROM {targets['individual_heating_supply']['schema']}.
        {targets['individual_heating_supply']['table']}
        """)

    supply_2035 = cascade_heat_supply_indiv(
        'eGon2035', distribution_level='federal_states', plotting=False)

    supply_2035['scenario'] = 'eGon2035'

    supply_2035.to_postgis(
        targets['individual_heating_supply']['table'],
        schema=targets['individual_heating_supply']['schema'],
        con=db.engine(), if_exists='append')

class HeatSupply(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatSupply",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(create_tables, {
                district_heating, individual_heating, potential_germany}),
        )