import requests
import sys
import os
import pandas as pd
from settings import base_url, access_token

from time import sleep
from dictparse import DictionaryParser
from json_normalize import json_normalize

#base_url = 'https://qa.ddso-spot.quantori.com'
#access_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYWMxNjlkM2RiZDQ4ODlhYTgwNTUxM2I2NmRlYzFkOGM2MWQ2YWNjMTZmZWM3ODIwYzJiZDM3MjI5MWEyMGJmIiwiaWF0IjoxNjY0MDAxNDc2LCJuYmYiOjE2NjQwMDE0NzYsImp0aSI6IjViODc2MjgwLTRmNzMtNGQ2My1hNDgyLTgxOGZhZWI4MTAzNyIsImV4cCI6MTY2NDAzMDI3NiwidHlwZSI6ImFjY2VzcyIsImZyZXNoIjpmYWxzZSwiY3NyZiI6IjU4ODY4YTcxLWQ1Y2UtNDYyNy1iYTk0LTVmMjE1MDk2Zjk2NiJ9.mFZ93Lc79PGMouN7C3Zoar78yAx7tNxQu9nTaJJPcek'

access_token_expired = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYWMxNjlkM2RiZDQ4ODlhYTgwNTUxM2I2NmRlYzFkOGM2MWQ2YWNjMTZmZWM3ODIwYzJiZDM3MjI5MWEyMGJmIiwiaWF0IjoxNjYzNTc3Nzk2LCJuYmYiOjE2NjM1Nzc3OTYsImp0aSI6ImY2OGYyNjZkLWQ5YjctNDZiZi04M2I5LTUyNDQ0YzgxNDdjZCIsImV4cCI6MTY2MzYwNjU5NiwidHlwZSI6ImFjY2VzcyIsImZyZXNoIjpmYWxzZSwiY3NyZiI6ImRiODhmZjYyLWFkZWUtNDgyYS1hZTc1LTExZmJmNzVmN2NhZiJ9.9SaZYUhaZdUhsSI6iJFVkdxgsixNHrb333-mEdX-shh'
dataset_id = '6ed3cc84c0db4b3d87b7a355b6eecaf7'
dataset_info_list = list()
dataset_info_dict = dict()
csv_info_dict = dict()
feedback_id = ""


class aws_credentials():
    def __init__(self,private_url, key, x_amz_algorithm, x_amz_credentials, x_amz_date, policy, x_amz_signature):
        self.private_url = private_url
        self.key = key
        self.x_amz_algorithm = x_amz_algorithm
        self.x_amz_credentials = x_amz_credentials
        self.x_amz_date = x_amz_date
        self.policy = policy
        self.x_amz_signature = x_amz_signature 

class dataset_info():
    def __init__(self, name, num_id, dataset_id, rows_num, cols_num):
        self.name = name
        self.num_id = num_id
        self.dataset_id = dataset_id
        self.rows_num = rows_num
        self.cols_num = cols_num





def d_upload_dataset():
    '''Downloads the list of already uploaded datasets''' 
    files = {"file": open(os.path.join(sys.path[0],"1com.csv"),'rb')} 
    print(requests.Request('POST', base_url + "/api/datasets/upload", headers = {'Authorization': access_token}, files=files).prepare().body.decode('utf8'))
    r = requests.post(base_url + "/api/datasets/upload", headers = {'Authorization': access_token}, files=files)
    print(r.headers)
    print(r.content)

#upload_dataset()

def upload_dataset():
    '''Uploads file - consists of a sequence of API calls''' 
    payload={"filename":"5com.csv"}
    r = requests.get(base_url + "/api/upload/datasets/upload_params", headers = {'Authorization': access_token}, params=payload)
    #assert r.status_code == 200
    #print(r.headers)
    data = r.json()
    #print(data)
    normalized_json = json_normalize(data)
    #print(normalized_json)
    listed_response = list(normalized_json)
    #print(listed_response[0])
    #for items in listed_response[0]:
    #    print(items)

    #print(type(listed_response[0]))
    amazon_url = listed_response[0].get("data.url")
    amazon_key = listed_response[0].get("data.fields.key")
    x_amz_algorithm = listed_response[0].get("data.fields.x-amz-algorithm")
    x_amz_credential = listed_response[0].get("data.fields.x-amz-credential")
    x_amz_date = listed_response[0].get("data.fields.x-amz-date")
    policy = listed_response[0].get("data.fields.policy")
    x_amz_signature = listed_response[0].get("data.fields.x-amz-signature")
    payload_for_upload={"key":amazon_key, "x-amz-algorithm":x_amz_algorithm, "x-amz-credential":x_amz_credential,"x-amz-date":x_amz_date, "policy":policy, "X-amz-signature":x_amz_signature}
    files = {"file": open(os.path.join(sys.path[0],"5com.csv"),'rb')}
    print(payload_for_upload)
    print(amazon_url)
    #payload_for_upload["file"] = open(os.path.join(sys.path[0],"4com.csv"),'rb')
    upload = requests.post(amazon_url, data=payload_for_upload, files = files)  
    print("\n\n\n\n")
    print("From amazon: \n")
    print(upload.text)
    print(upload.status_code) #204
    if (upload.status_code) == 204:
        json_input = {"filepath":amazon_key}
        validation_response = requests.post(base_url + "/api/upload/datasets/validate", headers = {'Authorization': access_token}, json=json_input)
        print(validation_response.status_code) #200   
        print(validation_response.json()) 
        dataset_id = validation_response.json().get("id") 
    response = "STARTED"
    while response != "SUCCESS":    
        check_response = requests.get(base_url + "/api/task/" + dataset_id + "/status", headers = {'Authorization': access_token})
        response = check_response.json().get("status")
        sleep(1)
    result_upload_response = requests.get(base_url + "/api/task/" + dataset_id + "/result", headers = {'Authorization': access_token})
    data_result_upload_response = result_upload_response.json()
    print(data_result_upload_response)
    normalized_json_upload_response = json_normalize(data_result_upload_response)
    print("---------------------")
    dictionary_upload_response = list(normalized_json_upload_response)
    print(dictionary_upload_response[0])
#upload_dataset()

#get_datasets_list()
'''
response_content = {"private_url":"https://qtr-ddso-upload-qa.s3.amazonaws.com/datasets/dac169d3dbd4889aa805513b66dec1d8c61d6acc16fec7820c2bd372291a20bf/d5f89223/2com.csv","data":{"url":"https://qtr-ddso-upload-qa.s3.amazonaws.com/","fields":{"key":"datasets/dac169d3dbd4889aa805513b66dec1d8c61d6acc16fec7820c2bd372291a20bf/d5f89223/2com.csv","x-amz-algorithm":"AWS4-HMAC-SHA256","x-amz-credential":"AKIAQQR5GVBY7IHN442A/20220912/us-east-2/s3/aws4_request","x-amz-date":"20220912T110028Z","policy":"eyJleHBpcmF0aW9uIjogIjIwMjItMDktMTJUMTE6MTA6MjhaIiwgImNvbmRpdGlvbnMiOiBbWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsIDAsIDEwNDg1NzYwMF0sIHsiYnVja2V0IjogInF0ci1kZHNvLXVwbG9hZC1xYSJ9LCB7ImtleSI6ICJkYXRhc2V0cy9kYWMxNjlkM2RiZDQ4ODlhYTgwNTUxM2I2NmRlYzFkOGM2MWQ2YWNjMTZmZWM3ODIwYzJiZDM3MjI5MWEyMGJmL2Q1Zjg5MjIzLzJjb20uY3N2In0sIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwgeyJ4LWFtei1jcmVkZW50aWFsIjogIkFLSUFRUVI1R1ZCWTdJSE40NDJBLzIwMjIwOTEyL3VzLWVhc3QtMi9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1kYXRlIjogIjIwMjIwOTEyVDExMDAyOFoifV19","x-amz-signature":"ef6cd70b639ab749d7c92c8e4c4c59d99e26a0bf2127d0c1a95862f9eea38ade"}}}


print(type(response_content))
parser=DictionaryParser()
parser.add_param("private_url",str,required=True)
parser.add_param("data",str,required=True)
#parser.add_param("x-amz-algorithm",str,required=True)
#parser.add_param("x-amz-credential",str,required=True)
#parser.add_param("x-amz-date",str,required=True)
#parser.add_param("policy",str,required=True)
#parser.add_param("x-amz-signature",str,required=True)
params = parser.parse_dict(response_content)
print(params)

normalized_json = json_normalize(response_content)
listed_response = list(normalized_json)
for key in listed_response[0]:
    print(key)
    print('\n')
amazon_url = listed_response[0].get("data.url")
amazon_key = listed_response[0].get("data.fields.key")
x_amz_algorithm = listed_response[0].get("data.fields.x-amz-algorithm")
x_amz_credentials = listed_response[0].get("data.fields.x-amz-credentials")
x_amz_date = listed_response[0].get("data.fields.x-amz-date")
policy = listed_response[0].get("data.fields.policy")
x_amz_signature = listed_response[0].get("data.fields.signature")   
print(amazon_key)
#aws_credentials_data = aws_credentials(cfd, )
'''

def delete_all_datasets():
    '''Deletes all datasets'''
    get_datasets = requests.get(base_url + "/api/datasets", headers = {'Authorization': access_token})
    print(get_datasets.headers)
    print(get_datasets.json())
    response_json = get_datasets.json()
    dataset_id_list = []
    if len(response_json) != 0:
        for item in response_json:
            dataset_id_list.append(item.get("dataset_id"))
        print(dataset_id_list)
        for dataset_id in dataset_id_list:    
            delete_dataset = requests.delete(base_url + "/api/datasets/" + dataset_id, headers = {'Authorization': access_token})
            assert (delete_dataset.status_code) == 204
            print (delete_dataset.content)
            print (delete_dataset.text)
            assert (delete_dataset.text) == ''
    else: 
        print("Skipping the datasets deletion. No existing datasets found for the account")        
#delete_all_datasets()  


def get_all_datasets_list():
    '''Downloads the list of all available datasets'''
    r = requests.get(base_url + "/api/datasets", headers={'Authorization': access_token})
    print("foolow")
    #print(r.content)
    #print(r.headers)
    print("Get wall datasets list\n")
    response_json = r.json()

    print(response_json)
    print(type(response_json))
    if len(response_json) != 0:
       for item in response_json:
           #dataset_info_list.append(dataset_info(item.get("name"),item.get("num_id"),item.get("dataset_id"),item.get("rows_num"),item.get("cols_num")))
           dataset_info_dict[item.get("name")] = {}
           dataset_info_dict[item.get("name")]["num_id"] = item.get("id")
           dataset_info_dict[item.get("name")]["dataset_id"] = item.get("dataset_id")
           dataset_info_dict[item.get("name")]["rows_num"] = item.get("rows_num") 
           dataset_info_dict[item.get("name")]["cols_num"] = item.get("cols_num")
           print(dataset_info_dict)
    print(dataset_info_list)

def dataset_meta_info():
    '''Downloads the meta info for a particular dataset'''
    r = requests.get(base_url + "/api/datasets/meta/" +
    dataset_id, headers={'Authorization': access_token})

    print("Dataset meta info\n\n\n")
    print(r.json())
    #print(r.text)

#get_all_datasets_list()        
#dataset_meta_info()



#====================
def expired_auth_token():
    '''Checks that the response/status code is 422 if the authentication token is incorrect'''
    r = requests.get(base_url + "/api/datasets",
                    headers={'Authorization': access_token_expired})
    response_json = r.json()
    print(response_json)
    assert response_json.get("detail") == "Signature verification failed"
    assert r.status_code == 422

#expired_auth_token()    

def rename_dataset(dataset_id, new_name):
    '''Renames an existing dataset with passed in new name'''
    r = requests.patch(base_url + "/api/datasets/" + dataset_id + "?name=" + new_name, headers={'Authorization': access_token})
    #response_json = r.json()
    print(r.content)
    #assert response_json.get("detail") == "Signature verification failed"
    print(r.status_code)
    assert r.status_code == 204


#rename_dataset(dataset_id, "newname.csv")    

def rename_user():
    '''Gets user data and renames an existing/current user and then verifies the name change'''
    r = requests.get(base_url + "/api/authorization/me", headers={'Authorization': access_token})
    assert r.status_code == 200
    user_name = r.json().get("name")
    if len(user_name) <= 30:
        new_user_name = user_name + "1"
    else:
        new_user_name = "user"    
    rename = requests.patch(base_url + "/api/authorization/name", headers={'Authorization': access_token}, params = {"name":new_user_name})
    assert(rename.status_code) == 200
    check = requests.get(base_url + "/api/authorization/me", headers={'Authorization': access_token})
    assert check.json().get("name") == new_user_name

#rename_user()  

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


#helper - deprecated
def get_the_number_of_rows_cols():
    '''Returns as a tuple the names of all .csv files in current folder'''
    df = pd.read_csv(os.path.join(sys.path[0],"1com.csv"), sep='\t', engine='python')
    print(df.head())
    print(df.shape)
    rows = len(df)
    print(rows)
    cols = len(df.columns)
    print(cols)   



#get_the_number_of_rows_cols()     
#print(get_the_csv_files_tuple())

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

# files = get_the_csv_files_tuple()
# files_info_from_csv = get_the_number_of_rows_cols_for_all_csv_files(files)
# print(files_info_from_csv)
# get_all_datasets_list()


################to check the numbers of rows/cols in csv files and the one reported by app
# for key in files_info_from_csv:
    
#     print('Start') 
#     print(key, '->', files_info_from_csv[key]) 
#     print(key,'--->',dataset_info_dict[key])
#     assert files_info_from_csv[key]["rows"]  == dataset_info_dict[key]["rows_num"]
#     assert files_info_from_csv[key]["cols"]  == dataset_info_dict[key]["cols_num"]
#     print('\n\n') 


##create a feedback - get back the id, get the list of all of them - verify that created feedback is inside, then delete it and verify atht it was deleted

def post_feedback():
    '''Creates new feedback and saves its "created_id" in variable feedback_id'''
    global feedback_id
    data_input = {"type_of_request":"Report an issue", "user_issue":"New bug is found", "screenshots":"undefined", "email":"123@123.com", "form_name":"feedback-form"}
    feedback_response = requests.post(base_url + "/api/feedback/user-feedback", headers = {'Authorization': access_token}, data=data_input)
    print(feedback_response.json())
    feedback_id = feedback_response.json().get("created_id")
    print(f"Created_id returned --- {feedback_id} ---")
    assert feedback_response.status_code == 201

def get_feedback_exists():
    '''Verifies that for the crated feedback its id is returned back when requesting the list of all submitted feedbacks '''
    global feedback_id
    feedback_response = requests.get(base_url + "/api/feedback/user-feedback", headers = {'Authorization': access_token})
    feedback_response_json_list = feedback_response.json()
    print(type(feedback_response_json_list))
    ids_list = list()
    for item in feedback_response_json_list:
        ids_list.append(item.get("id"))
    print("Assert starts") 
    print(f"Saved created_id --- {feedback_id} ---")   
    print(ids_list)
    #feedback_id = 'dcb00f8b8bee4fe197064faba8fd353f'
    assert feedback_response.status_code == 200
    assert feedback_id in ids_list 
    print("success")     
    

def delete_feedback():
    '''Verifies that when deleting the feedback by its correct id the status_code in the response is 204 and when it is not correct id the the response is 404'''    
    feedback_response = requests.delete(base_url + "/api/feedback/user-feedback/" + feedback_id, headers = {'Authorization': access_token})
    assert feedback_response.status_code == 204 
    feedback_response = requests.delete(base_url + "/api/feedback/user-feedback/" + feedback_id + "1", headers = {'Authorization': access_token})
    assert feedback_response.status_code == 404 



post_feedback()
print(f"Saved created_id IN main FILE--- {feedback_id} ---") 
get_feedback_exists()
delete_feedback()

