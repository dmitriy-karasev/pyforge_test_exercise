import sys
import os
import pandas as pd
import requests

from settings import base_url, access_token

#helper      
def get_the_csv_files_tuple() -> tuple:
    '''Returns as a tuple the complete path including file names of all .csv files in current folder'''
    files_list = list()
    path = sys.path[0]
    print(path)
    for file in os.listdir(path):
        print(file)
        if file.endswith(".csv"):
            files_list.append(os.path.join(path, file))       
    
    return tuple(files_list)

#helper
def get_the_number_of_rows_cols_for_all_csv_files(files: tuple) -> dict:
    '''Returns as a dictionary the names of csv files and the # of rows/columns for each of the files passed in'''
    dataset_count_dict = dict()
    for file in files: 
        df = pd.read_csv(file, sep='\t', engine='python')
        print(df.head())
        print(df.shape)
        rows = len(df)
        cols = len(df.columns)
        real_file_name = file.rsplit('\\',1)[1]
        dataset_count_dict[real_file_name] = {}
        dataset_count_dict[real_file_name]["rows"] = rows
        dataset_count_dict[real_file_name]["cols"] = cols

    return dataset_count_dict   

#helper 
def get_all_datasets_dict():
    '''Downloads the list of all available datasets and returns some data dictionary - see the fields below'''
    r = requests.get(base_url + "/api/datasets", headers={'Authorization': access_token})
    dataset_info_dict = dict()
    print("Get all datasets list\n")
    response_json = r.json()

    print(response_json)
    if len(response_json) != 0:
       for item in response_json:
           dataset_info_dict[item.get("name")] = {}
           dataset_info_dict[item.get("name")]["num_id"] = item.get("id")
           dataset_info_dict[item.get("name")]["dataset_id"] = item.get("dataset_id")
           dataset_info_dict[item.get("name")]["rows_num"] = item.get("rows_num") 
           dataset_info_dict[item.get("name")]["cols_num"] = item.get("cols_num")
        
    return dataset_info_dict

#helper 
def get_dataset_id(file_name):
    '''Returns the dataset_id by the provided fiel_name'''
    r = requests.get(base_url + "/api/datasets", headers={'Authorization': access_token})
    response_json = r.json()

    print(response_json)
    if len(response_json) != 0:
       for item in response_json:
           if item.get("name") == file_name:
               dataset_id = item.get("dataset_id")       
    
    return dataset_id
