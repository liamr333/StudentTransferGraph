import pandas as pd
from IPython.display import display, HTML
from matplotlib import pyplot as plt
from neo4j import GraphDatabase
from getpass import getpass
from py2neo import Graph
import re


# read files
transfer_report_20_21 = pd.read_csv('xfers_campus_2020_21.csv')
transfer_report_21_22 = pd.read_csv('xfers_campus_2021_22.csv')
schools_list = pd.read_csv('Schools_2021_to_2022.csv')

# remove -999 and '.' from TRANSFERS_IN_OR_OUT columns of both datasets
cleaned_transfer_report_20_21 = transfer_report_20_21[(transfer_report_20_21['TRANSFERS_IN_OR_OUT'] != '-999') & (transfer_report_20_21['TRANSFERS_IN_OR_OUT'] != '.')]
cleaned_transfer_report_21_22 = transfer_report_21_22[(transfer_report_21_22['TRANSFERS_IN_OR_OUT'] != '-999') & (transfer_report_21_22['TRANSFERS_IN_OR_OUT'] != '.')]
cleaned_schools_list = schools_list

# reduce columns in schools list to only the 5 below
remove_columns = ['School_Num', 'School_Nam', 'X', 'Y', 'LongLabel']
for column_title in schools_list.keys():
    if column_title not in remove_columns:
        cleaned_schools_list = cleaned_schools_list.drop(columns=column_title)
        
# remove rows in the schools list where school name or school number is NaN
cleaned_schools_list = cleaned_schools_list[cleaned_schools_list['School_Num'].notna()]
cleaned_schools_list = cleaned_schools_list[cleaned_schools_list['School_Nam'].notna()]

# convert school number in cleaned schools list to int type
cleaned_schools_list = cleaned_schools_list.astype({"School_Num":int})

# join school list to each dataset
cleaned_transfer_report_20_21.set_index('CAMPUS_RES_OR_ATTEND').join(cleaned_schools_list.set_index('School_Num'))
cleaned_transfer_report_21_22.set_index('CAMPUS_RES_OR_ATTEND').join(cleaned_schools_list.set_index('School_Num'))

password = getpass('Enter password: ')


def delete_all():
    database_connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth=('neo4j', password))
    session = database_connection.session()
    query = """
    MATCH (n)
    DELETE n
    """
    session.run(query)
    
    
# Detaches all schools from other schools
def detach_all():
    database_connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth=('neo4j', password))
    session = database_connection.session()
    query = "MATCH ()-[r]->() DELETE r;"
    session.run(query)
    

# (Just function name)
def remove_duplicate_relationships():
    ids_to_remove = []
    database_connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth=('neo4j', password))
    session = database_connection.session()
    query = """MATCH (s)-[r]->(e)
    WITH s,e,type(r) AS typ, tail(collect(r)) AS coll 
    FOREACH(x IN coll | DELETE x)"""
    session.run(query)


create_nodes_queries = []


# Creates school nodes
def give_school_node_query(school_name, school_number, school_x_coord, school_y_coord, school_address):
    query = "CREATE (n:School {Name: \'"
    query += school_name.replace(' ', '_').replace('\'', '_')
    query += "\', Number: "
    query += str(school_number)
    query += ", X: "
    query += str(school_x_coord)
    query += ", Y: "
    query += str(school_y_coord)
    query += ", Address: \'"
    query += school_address
    query += "\'})"
    return query



for index, row in cleaned_schools_list.iterrows():
    query = give_school_node_query(row['School_Nam'], row['School_Num'], row['X'], row['Y'], row['LongLabel'])
    create_nodes_queries.append(query)
    

database_connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth=('neo4j', password))
session = database_connection.session()
for query in create_nodes_queries:
    print(query)
    session.run(query)


create_relationships_queries = []

# create relationship query based on args
def create_relationship(school1_num, school2_num, num_transfers):
    
    insertion_query = """
    MATCH (a:School), (b:School)
    WHERE a.Number = {} AND b.Number = {}
    CREATE (a)-[r:TRANSFERRED_{}_TO]->(b) 
    RETURN type(r)
    """.format(int(school1_num), int(school2_num), num_transfers)
    return insertion_query


# create relationships between schools with a student transfer relationship
for index, row in cleaned_transfer_report_21_22.iterrows():
    if row['REPORT_TYPE'] == 'Transfers In From':
        query = create_relationship(row['CAMPUS_RES_OR_ATTEND'], row['REPORT_CAMPUS'], row['TRANSFERS_IN_OR_OUT'])
        create_relationships_queries.append(query)
    else:
        query = create_relationship(row['REPORT_CAMPUS'], row['CAMPUS_RES_OR_ATTEND'], row['TRANSFERS_IN_OR_OUT'])
        create_relationships_queries.append(query)
        

# connect to database and run queries
database_connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth=('neo4j', password))
session = database_connection.session()
for query in create_relationships_queries:
    session.run(query)
    
    

remove_duplicate_relationships()
        
