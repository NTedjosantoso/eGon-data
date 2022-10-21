"""The central module containing all code dealing with heat sector in etrago
"""
from egon.data.datasets import Dataset
from egon.data.datasets.hydrogen_etrago.bus import (
    insert_hydrogen_buses,
    insert_hydrogen_buses_eGon100RE,
)
from egon.data.datasets.hydrogen_etrago.h2_grid import insert_h2_pipelines
from egon.data.datasets.hydrogen_etrago.h2_to_ch4 import (
    insert_h2_to_ch4_eGon100RE,
    insert_h2_to_ch4_to_h2,
)
from egon.data.datasets.hydrogen_etrago.power_to_h2 import (
    insert_power_to_h2_to_power,
    insert_power_to_h2_to_power_eGon100RE,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    calculate_and_map_saltcavern_storage_potential,
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
    insert_H2_storage_eGon100RE,
)


class HydrogenBusEtrago(Dataset):
    """Insert the H2 buses in the database for Germany

    Insert the H2 buses in Germany in the database for the scenarios
    eGon2035 and eGon100RE

    """

    #:
    name: str = "HydrogenBusEtrago"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                calculate_and_map_saltcavern_storage_potential,
                insert_hydrogen_buses,
                insert_hydrogen_buses_eGon100RE,
            ),
        )


class HydrogenStoreEtrago(Dataset):
    """Insert the H2 stores in the database for Germany

    Insert the H2 stores in Germany in the database for the scenarios
    eGon2035 and eGon100RE

    """

    #:
    name: str = "HydrogenStoreEtrago"
    #:
    version: str = "0.0.3"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                insert_H2_overground_storage,
                insert_H2_saltcavern_storage,
                insert_H2_storage_eGon100RE,
            ),
        )


class HydrogenPowerLinkEtrago(Dataset):
    """Insert the electrolysis and the fuel cells in the database

    Insert the the electrolysis and the fuel cell links in Germany in
    the database for the scenarios eGon2035 and eGon100RE

    """

    #:
    name: str = "HydrogenPowerLinkEtrago"
    #:
    version: str = "0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                insert_power_to_h2_to_power,
                insert_power_to_h2_to_power_eGon100RE,
            ),
        )


class HydrogenMethaneLinkEtrago(Dataset):
    """Insert the methanation, feed in and SMR in the database

    Insert the the methanation, feed in (only in eGon2035) and SMR
    links in Germany in the database for the scenarios eGon2035 and
    eGon100RE

    """

    #:
    name: str = "HydrogenMethaneLinkEtrago"
    #:
    version: str = "0.0.5"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_to_ch4_to_h2, insert_h2_to_ch4_eGon100RE),
        )


class HydrogenGridEtrago(Dataset):
    """Insert the H2 grid in Germany in the database for eGon100RE

    Insert the H2 links (pipelines) in Germany in the database for the
    scenario eGon100RE

    """

    #:
    name: str = "HydrogenGridEtrago"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_pipelines,),
        )
