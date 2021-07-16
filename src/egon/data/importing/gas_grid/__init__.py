# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing data from SciGRID_gas IGGIELGN data
"""
import os
import ast
import pandas as pd
import numpy as np
import geopandas
import json

from shapely import geometry
from egon.data import db
from egon.data.config import settings                     
from geoalchemy2.types import Geometry
from urllib.request import urlretrieve
from zipfile import ZipFile
from geoalchemy2.shape import from_shape

def next_id(component): 
    """ Select next id value for components in pf-tables
    Parameters
    ----------
    component : str
        Name of componenet
    Returns
    -------
    next_id : int
        Next index value
    """
    max_id = db.select_dataframe(
        f"""
        SELECT MAX({component}_id) FROM grid.egon_pf_hv_{component}
        """)['max'][0]

    if max_id:
        next_id = max_id + 1
    else:
        next_id = 1

    return next_id


def download_SciGRID_gas_data():
    """
    Download SciGRID_gas IGGIELGN data from Zenodo

    """
    path = os.path.dirname(__file__) + '/'
    
    zenodo_zip_file_url = ("https://zenodo.org/record/4767098/files/IGGIELGN.zip")
    if not os.path.isfile(zenodo_zip_file_url):
        urlretrieve(zenodo_zip_file_url, path + 'IGGIELGN.zip')
        
    components = ['Nodes', 'PipeSegments', 'Productions', 'Storages'] #'Compressors'
    files = []
    for i in components:
        files.append('data/IGGIELGN_' + i + '.csv')
    
    with ZipFile(path + 'IGGIELGN.zip', 'r') as zipObj:
        listOfFileNames = zipObj.namelist()
        for fileName in listOfFileNames:
            if fileName in files:
                zipObj.extract(fileName, path)


def define_gas_nodes_list():
    """Define list of gas nodes from SciGRID_gas IGGIELGN data
    
    Returns
    -------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
        
    """
    # Select next id value
    new_id = next_id('bus')
    
    # Read-in data from csv-file
    target_file = os.path.join(
        os.path.dirname(__file__), 'data/IGGIELGN_Nodes.csv')
    
    gas_nodes_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['lat', 'long', 'id', 'country_code','param'])
    
    # Ajouter tri pour ne conserver que les pays ayant des pipelines en commun.
    
    gas_nodes_list = gas_nodes_list.rename(columns={'lat': 'y','long': 'x'})

    # Remove buses disconnected of the rest of the grid, until the SciGRID_gas data has been corrected.
    gas_nodes_list = gas_nodes_list[ ~ gas_nodes_list['id'].str.match('SEQ_11790_p')]
    gas_nodes_list = gas_nodes_list[ ~ gas_nodes_list['id'].str.match('Stor_EU_107')]    
    
    gas_nodes_list['bus_id'] = range(new_id, new_id + len(gas_nodes_list))
    gas_nodes_list = gas_nodes_list.set_index('id')
   
    return gas_nodes_list


def insert_gas_nodes_list(gas_nodes_list):
    """Insert list of gas nodes from SciGRID_gas IGGIELGN data
        Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    Returns
    -------
    None.
    """
    # Connect to local database
    engine = db.engine()
    
    gas_nodes_list = gas_nodes_list[ gas_nodes_list['country_code'].str.match('DE')] # A remplacer evtmt par un test sur le NUTS0 ?
    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in gas_nodes_list.iterrows():
        param = ast.literal_eval(row['param'])
        NUTS1.append(param['nuts_id_1'])
    gas_nodes_list = gas_nodes_list.assign(NUTS1 = NUTS1)
    
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
               'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
               'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
               'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
               'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
               'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
               'Berlin': 'DE3', 'Bayern': 'DE2'}

        gas_nodes_list = gas_nodes_list[gas_nodes_list['NUTS1'].isin([map_states[boundary], np.nan])]
        
        # A completer avec nodes related to pipelines which have an end in the selected area et evt deplacer ds define_gas_nodes_list
    
    # Add missing columns
    c = {'version':'0.0.0', 'scn_name':'eGon2035', 'carrier':'gas'}
    gas_nodes_list = gas_nodes_list.assign(**c)
    
    gas_nodes_list = geopandas.GeoDataFrame(gas_nodes_list, geometry=geopandas.points_from_xy(gas_nodes_list['x'], gas_nodes_list['y']))
    gas_nodes_list = gas_nodes_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)
    
    gas_nodes_list = gas_nodes_list.reset_index(drop=True)
    gas_nodes_list = gas_nodes_list.drop(columns=['NUTS1', 'param', 'country_code' ])

    # Insert data to db    
    gas_nodes_list.to_postgis('egon_pf_hv_bus',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append',
                              dtype = {"geom": Geometry()})

    
def insert_gas_pipeline_list(gas_nodes_list):
    """Insert list of gas pipelines from SciGRID_gas IGGIELGN data
    Parameters
    ----------
    gas_nodes_list : dataframe
        Dataframe containing the gas nodes (Europe)
    Returns
    -------
    None.
    """
    # Connect to local database
    engine = db.engine()
    
    # Select next id value
    new_id = next_id('link')

    classifiaction_file = os.path.join(
        "data_bundle_egon_data/pipeline_classification_gas/",
        'pipeline_classification.csv')
        
    classification = pd.read_csv(classifiaction_file,
                               delimiter=',',
                               usecols = ['classification', 'max_transport_capacity_Gwh/d'])

    # Read-in data from csv-file
    target_file = os.path.join(
        os.path.dirname(__file__), 'data/IGGIELGN_PipeSegments.csv')
    
    gas_pipelines_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['id', 'node_id', 'lat', 'long', 'country_code', 'param'])

    # Select the links having at least one bus in Germany
    gas_pipelines_list = gas_pipelines_list[gas_pipelines_list['country_code'].str.contains('DE')] # A remplacer evtmt par un test sur le NUTS0 ?
    
    # Remove links disconnected of the rest of the grid, until the SciGRID_gas data has been corrected. 
    gas_pipelines_list = gas_pipelines_list[ ~ gas_pipelines_list['id'].str.match('EntsoG_Map__ST_195')]
    gas_pipelines_list = gas_pipelines_list[ ~ gas_pipelines_list['id'].str.match('EntsoG_Map__ST_5')]
    
    gas_pipelines_list['link_id'] = range(new_id, new_id + len(gas_pipelines_list))
    gas_pipelines_list['link_id'] = gas_pipelines_list['link_id'].astype(int)

    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in gas_pipelines_list.iterrows():
        param = ast.literal_eval(row['param'])
        NUTS1.append(param['nuts_id_1'])
    gas_pipelines_list['NUTS1'] = NUTS1
    
    boundary = settings()['egon-data']['--dataset-boundary']

    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
                'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
                'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
                'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
                'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
                'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
                'Berlin': 'DE3', 'Bayern': 'DE2'}
        gas_pipelines_list["NUTS1"] = [x[0] for x in gas_pipelines_list['NUTS1']]
        gas_pipelines_list = gas_pipelines_list[gas_pipelines_list["NUTS1"].str.contains(map_states[boundary])]
        
        # A completer avec nodes related to pipelines which have an end in the selected area
   
    # Add missing columns
    gas_pipelines_list['scn_name'] = 'eGon2035'
    gas_pipelines_list['carrier'] = 'gas'
    gas_pipelines_list['version'] = '0.0.0'
        
    diameter = []
    length = []
    geom = []
    topo = []
    
    for index, row in gas_pipelines_list.iterrows():
        
        param = ast.literal_eval(row['param'])
        diameter.append(param['diameter_mm'])
        length.append(param['length_km'])
        
        long_e = json.loads(row['long'])
        lat_e = json.loads(row['lat'])
        crd_e = list(zip(long_e, lat_e))
        topo.append(geometry.LineString(crd_e))
        
        long_path = param['path_long'] 
        lat_path = param['path_lat'] 
        crd = list(zip(long_path, lat_path))
        crd.insert(0, crd_e[0])
        crd.append(crd_e[1])
        lines = []
        for i in range(len(crd)-1):
            lines.append(geometry.LineString([crd[i], crd[i+1]]))      
        geom.append(geometry.MultiLineString(lines))
    
    print(topo)
    gas_pipelines_list['diameter'] = diameter
    gas_pipelines_list['length'] = length
    gas_pipelines_list['geom'] = geom
    gas_pipelines_list['topo'] = topo
    gas_pipelines_list = gas_pipelines_list.set_geometry('geom', crs=4326)
    
    # Adjust columns
    bus0 = []
    bus1 = []
    pipe_class = []
    
    for index, row in gas_pipelines_list.iterrows():
        
        buses = row['node_id'].strip('][').split(', ')
        bus0.append(gas_nodes_list.loc[buses[0][1:-1],'bus_id'])
        bus1.append(gas_nodes_list.loc[buses[1][1:-1],'bus_id'])
        
        if row['diameter'] >= 1000:
            pipe_class = 'A'
        elif 700 <= row['diameter'] <= 1000:
            pipe_class = 'B'
        elif 500 <= row['diameter'] <= 700:
            pipe_class = 'C'
        elif 350 <= row['diameter'] <= 500:
            pipe_class = 'D'
        elif 200 <= row['diameter'] <= 350:
            pipe_class = 'E'
        elif 100 <= row['diameter'] <= 200:
            pipe_class = 'F'
        elif row['diameter'] <= 100:
            pipe_class = 'G'
            
    gas_pipelines_list['bus0'] = bus0
    gas_pipelines_list['bus1'] = bus1    
    gas_pipelines_list['pipe_class'] = pipe_class

    gas_pipelines_list = gas_pipelines_list.merge(classification, 
                                                  how='left',
                                                  left_on='pipe_class', 
                                                  right_on='classification')
    gas_pipelines_list['p_nom'] = gas_pipelines_list['max_transport_capacity_Gwh/d'] * (1000/24)
    
    gas_pipelines_list = gas_pipelines_list.drop(columns=['id', 'node_id', 'param', 'NUTS1', 'country_code', 
                                                          'diameter', 'pipe_class', 'classification', 
                                                          'max_transport_capacity_Gwh/d', 'lat', 'long'])
    
    # Insert data to db
    gas_pipelines_list.to_postgis('egon_pf_hv_gas_link',
                          engine,
                          schema = 'grid',
                          index = False,
                          if_exists = 'replace',
                          dtype = { 'geom': Geometry(), 'topo': Geometry()})
    
    db.execute_sql(
        """
    select UpdateGeometrySRID('grid', 'egon_pf_hv_gas_link', 'topo', 4326) ;
    
    INSERT INTO grid.egon_pf_hv_link (version, scn_name, link_id, bus0,
                                              bus1, p_nom, length,
                                              geom, topo, carrier)
    SELECT
    version, scn_name, link_id, bus0, bus1, p_nom, length, geom, topo, carrier
    FROM grid.egon_pf_hv_gas_link;
        
    DROP TABLE grid.egon_pf_hv_gas_link;
        """)
        
    
def insert_gas_data():
    """Overall function for importing gas data from SciGRID_gas
    Returns
    -------
    None.
    """
    download_SciGRID_gas_data()
    
    gas_nodes_list = define_gas_nodes_list()
   
    insert_gas_nodes_list(gas_nodes_list)
    
    insert_gas_pipeline_list(gas_nodes_list)
    