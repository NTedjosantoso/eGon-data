# -*- coding: utf-8 -*-
"""
Module containing the conversion factors (gas sector)


"""

conversion_factor = {
    "MCMperDay_to_MWhperh": 437.5,  # MCM/day to MWh/h
    "m3perh_to_MWhperh": 0.01083,  # m^3/h to MWh/h
    "MWhperh_to_GWhperDay": (24 / 1000),  # (= 24 / 1000) MWh/h to GWh/d
    "GWhperDay_to_MWhperh": (1000 / 24),  # GWh/d to MWh/h
    "workingGas_M_m3_to_MWh": 10830,  # gross calorific value = 39 MJ/m3 (eurogas.org)
}
