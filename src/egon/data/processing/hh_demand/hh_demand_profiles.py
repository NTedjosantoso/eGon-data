"""
Household electricity demand time series for scenarios in 2035 and 2050

Electricity demand data for households in Germany in 1-hourly resolution for
an entire year. Spatially, the data is resolved to 100 x 100 m cells and
provides individual and distinct time series for each household in a cell.
The cells are defined by the dataset Zensus 2011.

The resulting data is stored in two separate tables

* `demand.household_electricity_profiles_in_census_cells`:
  Lists references and scaling parameters to time series data for each household in a cell by
  identifiers. This table is fundamental for creating subsequent data like
  demand profiles on MV grid level or for determining the peak load at load
  area level. The table is created by :func:`houseprofiles_in_census_cells`.
* `demand.household_electricity_profiles_hvmv_substation`:
  Household electricity demand profiles aggregated at MV grid district level.
  Primarily used to create the eTraGo data model.
  The table is created with :func:`mv_grid_district_HH_electricity_load`.

The following datasets are used for creating the data:

* Electricity demand time series for household categories
  produced by demand profile generator (DPG) from Fraunhofer IEE
  (see :func:`get_household_demand_profiles_raw`)
* Spatial information about people living in households by Zensus 2011 at
  federal state level
    * type of household (family status)
    * age
    * size
* Spatial information about number of households per ha, categorized by type
  of household (family status) with 5 categories (also from Zensus 2011)
* Demand-Regio annual household demand at NUTS3 level

**What is the goal?**

To use the electricity demand time series from the `demand profile generator`
to created spatially reference household demand time series for Germany at a
resolution of 100 x 100 m cells.

**What is the challenge?**

The electricity demand time series produced by demand profile generator offer
12 different household profile categories.
To use most of them, the spatial information about the number of households
per cell (5 categories) needs to be enriched by supplementary data to match
the household demand profile categories specifications. Hence, 10 out of 12
different household profile categories can be distinguished and by increasing
the number of categories of cell-level household data.

**How are these datasets combined?**

* Spatial information about people living in households by zensus (2011) at federal state NUTS1 level
 :var:`df_zensus` is aggregated to be compatible to IEE household profile specifications.
    * exclude kids and reduce to adults and seniors
    * group as defined in :var:`HH_TYPES`
    * convert data from people living in households to number of households by :var:`mapping_people_in_households`
    * calculate fraction of fine household types (10) within subgroup of rough household types (5) :var:`df_dist_households`
* Spatial information about number of households per ha :var:`df_households_typ` is mapped to NUTS1 and NUTS3 level.
  Data is enriched with refined household subgroups via :var:`df_dist_households` in :var:`df_zensus_cells`.
* Enriched 100 x 100 m household dataset is used to sample and aggregate household profiles. A table including
  individual profile id's for each cell and scaling factor to match Demand-Regio annual sum projections for 2035 and 2050
  at NUTS3 level is created in the database as `demand.household_electricity_profiles_in_census_cells`.

**What are central assumptions during the data processing?**

* mapping zensus data to IEE household categories is not trivial. In
  conversion from persons in household to number of
  households, number of inhabitants for multi-person households is estimated
  as weighted average in :var:`OO_factor`
* the distribution to refine household types at cell level are the same for each federal state
* refining of household types lead to float number of profiles drew at cell level and need to be rounded to nearest int.
* 100 x 100 m cells are matched to NUTS via centroid location
* cells with households in unpopulated areas are removed

**Drawbacks and limitations of the data**

* the distribution to refine household types at cell level are the same for
  each federal state
* Household profiles aggregated annual demand matches Demand Regio demand at
  NUTS-3 level, but it is not matching the demand regio time series profile


Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""

from itertools import cycle
from pathlib import Path
from urllib.request import urlretrieve
import random

from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.processing.zensus_grid_districts import MapZensusGridDistricts

Base = declarative_base()

import egon.data.config

# Define mapping of zensus household categories to eurostat categories
# - Adults living in househould type
# - number of kids not included even if in housholdtype name
# **! The Eurostat data only gives the amount of adults/seniors, excluding the amount of kids <15**
# eurostat is used for demand-profile-generator @fraunhofer
HH_TYPES = {
    "SR": [
        ("Einpersonenhaushalte (Singlehaushalte)", "Insgesamt", "Seniors"),
        ("Alleinerziehende Elternteile", "Insgesamt", "Seniors"),
    ],
    # Single Seniors Single Parents Seniors
    "SO": [
        ("Einpersonenhaushalte (Singlehaushalte)", "Insgesamt", "Adults")
    ],  # Single Adults
    "SK": [("Alleinerziehende Elternteile", "Insgesamt", "Adults")],
    # Single Parents Adult
    "PR": [
        ("Paare ohne Kind(er)", "2 Personen", "Seniors"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "2 Personen", "Seniors"),
    ],
    # Couples without Kids Senior & same sex couples & shared flat seniors
    "PO": [
        ("Paare ohne Kind(er)", "2 Personen", "Adults"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "2 Personen", "Adults"),
    ],
    # Couples without Kids adults & same sex couples & shared flat adults
    "P1": [("Paare mit Kind(ern)", "3 Personen", "Adults")],
    "P2": [("Paare mit Kind(ern)", "4 Personen", "Adults")],
    "P3": [
        ("Paare mit Kind(ern)", "5 Personen", "Adults"),
        ("Paare mit Kind(ern)", "6 und mehr Personen", "Adults"),
    ],
    "OR": [
        ("Mehrpersonenhaushalte ohne Kernfamilie", "3 Personen", "Seniors"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "4 Personen", "Seniors"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "5 Personen", "Seniors"),
        (
            "Mehrpersonenhaushalte ohne Kernfamilie",
            "6 und mehr Personen",
            "Seniors",
        ),
        ("Paare mit Kind(ern)", "3 Personen", "Seniors"),
        ("Paare ohne Kind(er)", "3 Personen", "Seniors"),
        ("Paare mit Kind(ern)", "4 Personen", "Seniors"),
        ("Paare ohne Kind(er)", "4 Personen", "Seniors"),
        ("Paare mit Kind(ern)", "5 Personen", "Seniors"),
        ("Paare ohne Kind(er)", "5 Personen", "Seniors"),
        ("Paare mit Kind(ern)", "6 und mehr Personen", "Seniors"),
        ("Paare ohne Kind(er)", "6 und mehr Personen", "Seniors"),
    ],
    # no info about share of kids
    # OO, O1, O2 have the same amount, as no information about the share of kids within zensus data set.
    # if needed the total amount can be corrected in the hh_tools.get_hh_dist function
    # using multi_adjust=True option
    "OO": [
        ("Mehrpersonenhaushalte ohne Kernfamilie", "3 Personen", "Adults"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "4 Personen", "Adults"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "5 Personen", "Adults"),
        (
            "Mehrpersonenhaushalte ohne Kernfamilie",
            "6 und mehr Personen",
            "Adults",
        ),
        ("Paare ohne Kind(er)", "3 Personen", "Adults"),
        ("Paare ohne Kind(er)", "4 Personen", "Adults"),
        ("Paare ohne Kind(er)", "5 Personen", "Adults"),
        ("Paare ohne Kind(er)", "6 und mehr Personen", "Adults"),
    ],
    # no info about share of kids
}

MAPPING_ZENSUS_HH_SUBGROUPS = {
    1: ["SR", "SO"],
    2: ["PR", "PO"],
    3: ["SK"],
    4: ["P1", "P2", "P3"],
    5: ["OR", "OO"],
}


class HouseholdElectricityProfilesInCensusCells(Base):
    __tablename__ = "household_electricity_profiles_in_census_cells"
    __table_args__ = {"schema": "demand"}

    cell_id = Column(Integer, primary_key=True)
    grid_id = Column(String)
    cell_profile_ids = Column(ARRAY(String, dimensions=2))
    nuts3 = Column(String)
    nuts1 = Column(String)
    factor_2035 = Column(Float)
    factor_2050 = Column(Float)


class EgonEtragoElectricityHouseholds(Base):
    __tablename__ = "egon_etrago_electricity_households"
    __table_args__ = {"schema": "demand"}

    version = Column(String, primary_key=True)
    subst_id = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))
    q_set = Column(ARRAY(Float))


def clean(x):
    """Clean zensus household data row-wise

    Clean dataset by

    * converting '.' and '-' to str(0)
    * removing brackets

    Table can be converted to int/floats afterwards

    Parameters
    ----------
    x: pd.Series
        It is meant to be used with :code:`df.applymap()`

    Returns
    -------
    pd.Series
        Re-formatted data row
    """
    x = str(x).replace("-", str(0))
    x = str(x).replace(".", str(0))
    x = x.strip("()")
    return x


def get_household_demand_profiles_raw():
    """
    Downloads and returns household electricity demand profiles

    Household electricity demand profiles generated by Fraunhofer IEE.
    Methodology is described in
    :ref:`Erzeugung zeitlich hochaufgelöster Stromlastprofile für verschiedene
    Haushaltstypen
    <https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen>`_.
    It is used and further described in the following theses by:

    * Jonas Haack:
      "Auswirkungen verschiedener Haushaltslastprofile auf PV-Batterie-Systeme" (confidential)
    * Simon Ruben Drauz
      "Synthesis of a heat and electrical load profile for single and multi-family houses used for subsequent
      performance tests of a multi-component energy system",
      http://dx.doi.org/10.13140/RG.2.2.13959.14248

    Download only happens, if file 'h0_profiles.h5' isn't already existing.

    Returns
    -------
    pd.DataFrame
        Table with profiles in columns and time as index. A pd.MultiIndex is
        used to distinguish load profiles from different EUROSTAT household
        types.
    """
    data_config = egon.data.config.datasets()["household_electricity_demand"]

    hh_profiles_url = data_config["sources"][
        "household_electricity_demand_profiles"
    ]["url"]
    hh_profiles_file = Path(".") / Path(hh_profiles_url).name

    if not hh_profiles_file.is_file():
        urlretrieve(hh_profiles_url, hh_profiles_file)

    hh_profiles = pd.read_hdf(hh_profiles_file)

    # set multiindex to HH_types
    hh_profiles.columns = pd.MultiIndex.from_arrays(
        [hh_profiles.columns.str[:2], hh_profiles.columns.str[3:]]
    )

    # Cast profile ids into int
    hh_profiles.columns = pd.MultiIndex.from_tuples(
        [(a, int(b)) for a, b in hh_profiles.columns]
    )

    return hh_profiles


def download_process_zensus_households():
    """
    Downloads and pre-processes zensus age x household type data

    Dataset about household size with information about the categories:

    * family type
    * age class
    * household size

    for Germany in spatial resolution of federal states.

    Data manually selected and retrieved from:
    https://ergebnisse2011.zensus2022.de/datenbank/online
    For reproducing data selection, please do:

    * Search for: "1000A-3016"
    * or choose topic: "Bevölkerung kompakt"
    * Choose table code: "1000A-3016" with title "Personen: Alter (11 Altersklassen) - Größe des
    privaten Haushalts - Typ des privaten Haushalts (nach Familien/Lebensform)"
    - Change setting "GEOLK1" to "Bundesländer (16)"

    Data would be available in higher resolution
    ("Landkreise und kreisfreie Städte (412)"), but only after registration.

    The downloaded file is called 'Zensus2011_Personen.csv'.

    Download only happens, if file isn't already existing.

    Returns
    -------
    pd.DataFrame
        Pre-processed zensus household data
    """
    data_config = egon.data.config.datasets()["household_electricity_demand"]

    households_url = data_config["sources"]["zensus_household_types"]["url"]
    households_file = Path(".") / Path(households_url).name

    # Download prepared data file from nextcloud
    if not households_file.is_file():
        urlretrieve(households_url, households_file)

    # Read downloaded file from disk
    households_raw = pd.read_csv(
        households_file,
        sep=";",
        decimal=".",
        skiprows=5,
        skipfooter=7,
        index_col=[0, 1],
        header=[0, 1],
        encoding="latin1",
        engine="python",
    )

    # Clean data
    households = households_raw.applymap(clean).applymap(int)

    # Make data compatible with household demand profile categories
    # Use less age interval and aggregate data to NUTS-1 level
    households_nuts1 = process_nuts1_zensus_data(households)

    return households_nuts1


def get_hh_dist(df_zensus, hh_types, multi_adjust=True, relative=True):
    """
    Group zensus data to fit Demand-Profile-Generator (DPG) format.

    Parameters
    ----------
    df_zensus: pd.DataFrame
        Zensus households data
    hh_types: dict
        Mapping of zensus groups to DPG groups
    multi-adjust: bool
        If True (default), splits DPG-group 'OO' into 3 subgroups and uses
        distribution factor derived by table II in
        https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen
    relative: bool
        if True produces relative values

    Returns
    ----------
    df_hh_types: pd.DataFrame
        distribution of people by household type and regional-resolution

        .. warning::

            Data still needs to be converted from amount of people to amount
            of households
    """
    # adjust multi with/without kids via eurostat share as not clearly derivable without infos about share of kids
    if multi_adjust:
        adjust = {
            "SR": 1,
            "SO": 1,
            "SK": 1,
            "PR": 1,
            "PO": 1,
            "P1": 1,
            "P2": 1,
            "P3": 1,
            "OR": 1,
            "OO": 0.703,
            "O1": 0.216,
            "O2": 0.081,
        }
    else:
        adjust = {
            "SR": 1,
            "SO": 1,
            "SK": 1,
            "PR": 1,
            "PO": 1,
            "P1": 1,
            "P2": 1,
            "P3": 1,
            "OR": 1,
            "OO": 1,
            "O1": 0,
            "O2": 0,
        }

    df_hh_types = pd.DataFrame(
        (
            {
                hhtype: adjust[hhtype] * df_zensus.loc[countries, codes].sum()
                for hhtype, codes in hh_types.items()
            }
            for countries in df_zensus.index
        ),
        index=df_zensus.index,
    )
    # drop zero columns
    df_hh_types = df_hh_types.loc[:, (df_hh_types != 0).any(axis=0)]
    if relative:
        # normalize
        df_hh_types = df_hh_types.div(df_hh_types.sum(axis=1), axis=0)
    return df_hh_types.T


def inhabitants_to_households(
    df_people_by_householdtypes_abs, mapping_people_in_households
):
    """
    Convert number of inhabitant to number of household types

    Takes the distribution of peoples living in types of households to
    calculate a distribution of household types by using a people-in-household
    mapping.

    Results are rounded to int (ceiled) to full households.

    Parameters
    ----------
    df_people_by_householdtypes_abs: pd.DataFrame
        Distribution of people living in households
    mapping_people_in_households: dict
        Mapping of people living in certain types of households

    Returns
    ----------
    df_households_by_type: pd.DataFrame
        Distribution of households type

    """
    # compare categories and remove form mapping if to many
    diff = set(df_people_by_householdtypes_abs.index) ^ set(
        mapping_people_in_households.keys()
    )

    if bool(diff):
        for key in diff:
            mapping_people_in_households = dict(mapping_people_in_households)
            del mapping_people_in_households[key]
        print(f"Removed {diff} from mapping!")

    # divide amount of people by people in household types
    df_households_by_type = df_people_by_householdtypes_abs.div(
        mapping_people_in_households, axis=0
    )
    # Number of people gets adjusted to integer values by ceiling
    # This introduces a small deviation
    df_households_by_type = df_households_by_type.apply(np.ceil)

    return df_households_by_type


def process_nuts1_zensus_data(df_zensus):
    """Make data compatible with household demand profile categories

    Groups, removes and reorders categories which are not needed to fit data to household types of
    IEE electricity demand time series generated by demand-profile-generator (DPG).

    * Kids (<15) are excluded as they are also excluded in DPG origin dataset
    * Adults (15<65)
    * Seniors (<65)

    Returns
    -------
    pd.DataFrame
        Aggregated zensus household data on NUTS-1 level
    """
    # Group data to fit Load Profile Generator categories
    # define kids/adults/seniors
    kids = ["Unter 3", "3 - 5", "6 - 14"]  # < 15
    adults = [
        "15 - 17",
        "18 - 24",
        "25 - 29",
        "30 - 39",
        "40 - 49",
        "50 - 64",
    ]  # 15 < x <65
    seniors = ["65 - 74", "75 und älter"]  # >65

    # sum groups of kids, adults and seniors and concat
    df_kids = (
        df_zensus.loc[:, (slice(None), kids)].groupby(level=0, axis=1).sum()
    )
    df_adults = (
        df_zensus.loc[:, (slice(None), adults)].groupby(level=0, axis=1).sum()
    )
    df_seniors = (
        df_zensus.loc[:, (slice(None), seniors)].groupby(level=0, axis=1).sum()
    )
    df_zensus = pd.concat(
        [df_kids, df_adults, df_seniors],
        axis=1,
        keys=["Kids", "Adults", "Seniors"],
        names=["age", "persons"],
    )

    # reduce column names to state only
    mapping_state = {
        i: i.split()[1] for i in df_zensus.index.get_level_values(level=0)
    }

    # rename index
    df_zensus = df_zensus.rename(index=mapping_state, level=0)
    # rename axis
    df_zensus = df_zensus.rename_axis(["state", "type"])
    # unstack
    df_zensus = df_zensus.unstack()
    # reorder levels
    df_zensus = df_zensus.reorder_levels(
        order=["type", "persons", "age"], axis=1
    )

    return df_zensus


def get_cell_demand_profile_ids(df_cell, pool_size):
    """
    Generates tuple of hh_type and zensus cell ids

    Takes a random sample (without replacement) of profile ids for given cell.
    The number of households are rounded to the nearest integer if float.

    Parameters
    ----------
    df_cell: pd.DataFrame
        Household type information for a single zensus cell
    pool_size: int
        Number of available profiles to select from

    Returns
    -------
    list of tuple
        List of (`hh_type`, `cell_id`)

    """
    # maybe use instead
    # np.random.default_rng().integers(low=0, high=pool_size[hh_type], size=sq) instead of random.sample
    # use random.choice() if with replacement
    # list of sample ids per hh_type in cell
    cell_profile_ids = [
        (hh_type, random.sample(range(pool_size[hh_type]), k=sq))
        for hh_type, sq in zip(
            df_cell["hh_type"],
            np.rint(df_cell["hh_10types"].values).astype(int),
        )
    ]

    # format to lists of tuples (hh_type, id)
    cell_profile_ids = [
        list(zip(cycle([hh_type]), ids)) for hh_type, ids in cell_profile_ids
    ]
    # reduce to list
    cell_profile_ids = [a for b in cell_profile_ids for a in b]

    return cell_profile_ids


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def get_cell_demand_metadata(df_zensus_cells, df_profiles):
    """
    Defines information about profiles for each zensus cell

    A table including the demand profile ids for each cell is created by using
    :func:`get_cell_demand_profile_ids`. Household profiles are randomly sampled for each cell. The profiles
    are not replaced to the pool within a cell but after. The number of households are rounded to the nearest integer
    if float. This results in a small deviation for the course of the aggregated profiles.

    Parameters
    ----------
    df_zensus_cells: pd.DataFrame
        Household type parameters. Each row representing one household. Hence,
        multiple rows per zensus cell.
    df_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

    Returns
    -------
    pd.DataFrame
        Tabular data with one row represents one zensus cell.
        The column `cell_profile_ids` contains
        a list of tuples (see :func:`get_cell_demand_profile_ids`) providing a
        reference to the actual load profiles that are associated with this
        cell.
    """

    df_cell_demand_metadata = pd.DataFrame(
        index=df_zensus_cells.grid_id.unique(),
        columns=[
            "cell_profile_ids",
            "cell_id",
            "nuts3",
            "nuts1",
            "factor_2035",
            "factor_2050",
        ],
    )
    # 'peak_loads_hh', 'peak_load_cell',
    df_cell_demand_metadata = df_cell_demand_metadata.rename_axis("grid_id")

    pool_size = df_profiles.groupby(level=0, axis=1).size()

    for grid_id, df_cell in df_zensus_cells.groupby(by="grid_id"):

        # random sampling of household profiles for each cell
        # without replacement within cell but after
        # number of households are rounded to the nearest integer if float
        # this results in a small deviation for the course of the aggregated profiles
        cell_profile_ids = get_cell_demand_profile_ids(df_cell, pool_size)

        df_cell_demand_metadata.at[grid_id, "cell_id"] = df_cell.loc[
            :, "cell_id"
        ].unique()[0]
        df_cell_demand_metadata.at[
            grid_id, "cell_profile_ids"
        ] = cell_profile_ids
        df_cell_demand_metadata.at[grid_id, "nuts3"] = df_cell.loc[
            :, "nuts3"
        ].unique()[0]
        df_cell_demand_metadata.at[grid_id, "nuts1"] = df_cell.loc[
            :, "nuts1"
        ].unique()[0]

    return df_cell_demand_metadata


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def adjust_to_demand_regio_nuts3_annual(
    df_cell_demand_metadata, df_profiles, df_demand_regio
):
    """
    Computes the profile scaling factor for alignment to demand regio data

    The scaling factor can be used to re-scale each load profile such that the
    sum of all load profiles within one NUTS-3 area equals the annual demand
    of demand regio data.

    Parameters
    ----------
    df_cell_demand_metadata: pd.DataFrame
        Result of :func:`get_cell_demand_metadata`.
    df_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

    df_demand_regio: pd.DataFrame
        Annual demand by demand regio for each NUTS-3 region and scenario year.
        Index is pd.MultiIndex with :code:`tuple(scenario_year, nuts3_code)`.

    Returns
    -------
    pd.DataFrame
        Returns the same data as :func:`get_cell_demand_metadata`, but with
        filled columns `factor_2035` and `factor_2050`.
    """
    for nuts3_id, df_nuts3 in df_cell_demand_metadata.groupby(by="nuts3"):
        nuts3_cell_ids = df_nuts3.index
        nuts3_profile_ids = df_nuts3.loc[:, "cell_profile_ids"].sum()

        # take all profiles of one nuts3, aggregate and sum
        # profiles in Wh
        nuts3_profiles_sum_annual = (
            df_profiles.loc[:, nuts3_profile_ids].sum().sum()
        )

        # Scaling Factor
        # ##############
        # demand regio in MWh
        # profiles in Wh
        df_cell_demand_metadata.loc[nuts3_cell_ids, "factor_2035"] = (
            df_demand_regio.loc[(2035, nuts3_id), "demand_mwha"]
            * 1e3
            / (nuts3_profiles_sum_annual / 1e3)
        )
        df_cell_demand_metadata.loc[nuts3_cell_ids, "factor_2050"] = (
            df_demand_regio.loc[(2050, nuts3_id), "demand_mwha"]
            * 1e3
            / (nuts3_profiles_sum_annual / 1e3)
        )

    return df_cell_demand_metadata


def get_load_timeseries(
    df_profiles, df_cell_demand_metadata, cell_ids, year, peak_load_only=False
):
    """
    Get peak load for one load area

    The peak load is calculated in aggregated manner for a group of zensus
    cells that belong to one load area (defined by `cell_ids`).

    Parameters
    ----------
    df_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

        Used to calculate the peak load from.
    df_cell_demand_metadata: pd.DataFrame
        Return value of :func:`adjust_to_demand_regio_nuts3_annual`.
    cell_ids: list
        Zensus cell ids that define one group of zensus cells that belong to
        the same load area.
    year: int
        Scenario year. Is used to consider the scaling factor for aligning
        annual demand to NUTS-3 data.
    peak_load_only: bool
        If true, only the peak load value is returned (the type of the return
        value is `float`). Defaults to False which returns the entire time
        series as pd.Series.

    Returns
    -------
    pd.Series or float
        Aggregated time series for given `cell_ids` or peak load of this time
        series.
    """
    timesteps = len(df_profiles)
    full_load = pd.Series(
        data=np.zeros(timesteps), dtype=np.float64, index=range(timesteps)
    )
    load_area_meta = df_cell_demand_metadata.loc[
        cell_ids, ["cell_profile_ids", "nuts3", f"factor_{year}"]
    ]
    for (nuts3, factor), df in load_area_meta.groupby(
        by=["nuts3", f"factor_{year}"]
    ):
        part_load = (
            df_profiles.loc[:, df["cell_profile_ids"].sum()].sum(axis=1)
            * factor
            / 1e3
        )  # profiles in Wh
        full_load = full_load.add(part_load)
    if peak_load_only:
        return full_load.max()
    else:
        return full_load


def houseprofiles_in_census_cells():
    """
    Identify household electricity profiles for each census cell

    Creates a table that maps household electricity demand profiles to zensus
    cells. Each row represents one cell and contains a list of profile IDs.

    Use :func:`get_houseprofiles_in_census_cells` to retrieve the data from
    the database as pandas

    """
    # Get demand profiles and zensus household type x age category data
    df_profiles = get_household_demand_profiles_raw()
    df_zensus = download_process_zensus_households()

    # hh_tools.get_hh_dist without eurostat adjustment for O1-03 Groups in absolute values
    df_hh_types_nad_abs = get_hh_dist(
        df_zensus, HH_TYPES, multi_adjust=False, relative=False
    )

    # Get household size for each census cell grouped by
    # As this is only used to estimate size of households for OR, OO, 1 P and 2 P households are dropped
    df_hh_size = db.select_dataframe(
        sql="""
                        SELECT characteristics_text, SUM(quantity) as summe
                        FROM society.egon_destatis_zensus_household_per_ha as egon_d
                        WHERE attribute = 'HHGROESS_KLASS'
                        GROUP BY characteristics_text """,
        index_col="characteristics_text",
    )
    df_hh_size = df_hh_size.drop(index=["1 Person", "2 Personen"])

    # Define/ estimate number of persons (w/o kids) for each household category
    # For categories S* and P* it's clear; for multi-person households (OO,OR)
    # the number is estimated as average by taking remaining persons
    OO_factor = (
        sum(df_hh_size["summe"] * [3, 4, 5, 6]) / df_hh_size["summe"].sum()
    )
    mapping_people_in_households = {
        "SR": 1,
        "SO": 1,
        "SK": 1,  # kids are excluded
        "PR": 2,
        "PO": 2,
        "P1": 2,  # kids are excluded
        "P2": 2,  # ""
        "P3": 2,  # ""
        "OR": OO_factor,
        "OO": OO_factor,
    }
    # Determine number of persons for each household category and per federal state
    df_dist_households = inhabitants_to_households(
        df_hh_types_nad_abs, mapping_people_in_households
    )

    # Calculate fraction of fine household types within subgroup of rough household types
    for value in MAPPING_ZENSUS_HH_SUBGROUPS.values():
        df_dist_households.loc[value] = df_dist_households.loc[value].div(
            df_dist_households.loc[value].sum()
        )

    # Retrieve information about households for each census cell
    df_households_typ = db.select_dataframe(
        sql="""
                    SELECT grid_id, attribute, characteristics_code, characteristics_text, quantity
                    FROM society.egon_destatis_zensus_household_per_ha
                    WHERE attribute = 'HHTYP_FAM' """
    )
    df_households_typ = df_households_typ.drop(
        columns=["attribute", "characteristics_text"]
    )
    df_households_typ = df_households_typ.rename(
        columns={"quantity": "hh_5types"}
    )

    # Census cells with nuts3 and nuts1 information
    df_grid_id = db.select_dataframe(
        sql="""
                            SELECT pop.grid_id, pop.gid as cell_id, vg250.vg250_nuts3 as nuts3, lan.nuts as nuts1, lan.gen
                            FROM society.destatis_zensus_population_per_ha_inside_germany as pop
                            LEFT JOIN boundaries.egon_map_zensus_vg250 as vg250
                            ON (pop.gid=vg250.zensus_population_id)
                            LEFT JOIN boundaries.vg250_lan as lan
                            ON (LEFT(vg250.vg250_nuts3, 3)=lan.nuts) """
    )
    df_grid_id = df_grid_id.drop_duplicates()
    df_grid_id = df_grid_id.reset_index(drop=True)

    # Merge household type and size data with considered (populated) census cells
    # how='inner' is used as ids of unpopulated areas are removed df_grid_id or earliers tables. see here:
    # https://github.com/openego/eGon-data/blob/59195926e41c8bd6d1ca8426957b97f33ef27bcc/src/egon/data/importing/zensus/__init__.py#L418-L449
    df_households_typ = pd.merge(
        df_households_typ,
        df_grid_id,
        left_on="grid_id",
        right_on="grid_id",
        how="inner",
    )

    # Merge Zensus nuts1 level household data with zensus cell level 100 x 100 m
    # by refining hh-groups with MAPPING_ZENSUS_HH_SUBGROUPS
    df_zensus_cells = pd.DataFrame()
    for (country, code), df_country_type in df_households_typ.groupby(
        ["gen", "characteristics_code"]
    ):

        # iterate over zenus_country subgroups
        for typ in MAPPING_ZENSUS_HH_SUBGROUPS[code]:
            df_country_type["hh_type"] = typ
            df_country_type["factor"] = df_dist_households.loc[typ, country]
            df_country_type["hh_10types"] = (
                df_country_type["hh_5types"]
                * df_dist_households.loc[typ, country]
            )
            df_zensus_cells = df_zensus_cells.append(
                df_country_type, ignore_index=True
            )

    df_zensus_cells = df_zensus_cells.sort_values(
        by=["grid_id", "characteristics_code"]
    ).reset_index(drop=True)

    # Annual household electricity demand on NUTS-3 level (demand regio)
    df_demand_regio = db.select_dataframe(
        sql="""
                                SELECT year, nuts3, SUM (demand) as demand_mWha
                                FROM demand.egon_demandregio_hh as egon_d
                                GROUP BY nuts3, year
                                ORDER BY year""",
        index_col=["year", "nuts3"],
    )

    # Finally create table that stores profile ids for each cell
    df_cell_demand_metadata = get_cell_demand_metadata(
        df_zensus_cells, df_profiles
    )
    df_cell_demand_metadata = adjust_to_demand_regio_nuts3_annual(
        df_cell_demand_metadata, df_profiles, df_demand_regio
    )
    df_cell_demand_metadata = df_cell_demand_metadata.reset_index(drop=False)

    # Insert data into respective database table
    engine = db.engine()
    HouseholdElectricityProfilesInCensusCells.__table__.drop(
        bind=engine, checkfirst=True
    )
    HouseholdElectricityProfilesInCensusCells.__table__.create(
        bind=engine, checkfirst=True
    )
    df_cell_demand_metadata["cell_id"] = df_cell_demand_metadata[
        "cell_id"
    ].astype(int)
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            HouseholdElectricityProfilesInCensusCells,
            df_cell_demand_metadata.to_dict(orient="records"),
        )


def get_houseprofiles_in_census_cells():
    """
    Retrieve household demand time profile mapping

    See Also
    --------
    :func:`houseprofiles_in_census_cells`

    Returns
    -------
    pd.DataFrame
        Mapping of household demand profiles to zensus cells
    """
    with db.session_scope() as session:
        q = session.query(HouseholdElectricityProfilesInCensusCells)

        census_profile_mapping = pd.read_sql(
            q.statement, q.session.bind, index_col="cell_id"
        )

    census_profile_mapping["cell_profile_ids"] = census_profile_mapping[
        "cell_profile_ids"
    ].apply(lambda x: [(cat, int(profile_id)) for cat, profile_id in x])

    return census_profile_mapping


def mv_grid_district_HH_electricity_load(
    scenario_name, scenario_year, version, drop_table=False
):
    """
    Aggregated household demand time series at HV/MV substation level

    Calculate the aggregated demand time series based on the demand profiles
    of each zensus cell inside each MV grid district.

    Parameters
    ----------
    scenario_name: str
        Scenario name identifier, i.e. "eGon2035"
    scenario_year: int
        Scenario year according to `scenario_name`
    version: str
        Version identifier
    drop_table: bool
        Toggle to True for dropping table at beginning of this function.
        Be careful, delete any data.

    Returns
    -------
    pd.DataFrame
        Multiindexed dataframe with `timestep` and `subst_id` as indexers.
        Demand is given in kWh.
    """
    engine = db.engine()
    if drop_table:
        EgonEtragoElectricityHouseholds.__table__.drop(
            bind=engine, checkfirst=True
        )
    EgonEtragoElectricityHouseholds.__table__.create(
        bind=engine, checkfirst=True
    )

    with db.session_scope() as session:
        cells_query = session.query(
            HouseholdElectricityProfilesInCensusCells,
            MapZensusGridDistricts.subst_id,
        ).join(
            MapZensusGridDistricts,
            HouseholdElectricityProfilesInCensusCells.cell_id
            == MapZensusGridDistricts.zensus_population_id,
        )

    cells = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="cell_id"
    )
    cells["cell_profile_ids"] = cells["cell_profile_ids"].apply(
        lambda x: [(cat, int(profile_id)) for cat, profile_id in x]
    )

    # Create aggregated load profile for each MV grid district
    df_profiles = get_household_demand_profiles_raw()

    mvgd_profiles_dict = {}
    for grid_district, data in cells.groupby("subst_id"):
        mvgd_profile = get_load_timeseries(
            df_profiles=df_profiles,
            df_cell_demand_metadata=data,
            cell_ids=data.index,
            year=scenario_year,
            peak_load_only=False,
        )
        mvgd_profiles_dict[grid_district] = [mvgd_profile.to_list()]
    mvgd_profiles = pd.DataFrame.from_dict(mvgd_profiles_dict, orient="index")

    # Reshape data: put MV grid ids in columns to a single index column
    mvgd_profiles = mvgd_profiles.reset_index()
    mvgd_profiles.columns = ["subst_id", "p_set"]

    # Add remaining columns
    mvgd_profiles["version"] = version
    mvgd_profiles["scn_name"] = scenario_name

    # Insert data into respective database table
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonEtragoElectricityHouseholds,
            mvgd_profiles.to_dict(orient="records"),
        )

    return mvgd_profiles
