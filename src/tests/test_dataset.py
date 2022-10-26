import requests
import sys
import os
import pytest

from time import sleep
from json_normalize import json_normalize
from settings import base_url, access_token
from helper.utilities import get_the_csv_files_tuple, get_the_number_of_rows_cols_for_all_csv_files, get_all_datasets_dict, get_dataset_id

class TestDataset(object):
    
    access_token_expired = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYWMxNjlkM2RiZDQ4ODlhYTgwNTUxM2I2NmRlYzFkOGM2MWQ2YWNjMTZmZWM3ODIwYzJiZDM3MjI5MWEyMGJmIiwiaWF0IjoxNjYzNTc3Nzk2LCJuYmYiOjE2NjM1Nzc3OTYsImp0aSI6ImY2OGYyNjZkLWQ5YjctNDZiZi04M2I5LTUyNDQ0YzgxNDdjZCIsImV4cCI6MTY2MzYwNjU5NiwidHlwZSI6ImFjY2VzcyIsImZyZXNoIjpmYWxzZSwiY3NyZiI6ImRiODhmZjYyLWFkZWUtNDgyYS1hZTc1LTExZmJmNzVmN2NhZiJ9.9SaZYUhaZdUhsSI6iJFVkdxgsixNHrb333-mEdX-shh'
    base_url = base_url
    access_token = access_token
    feedback_id = ""

    @staticmethod
    def setup_class(cls):
        '''Uploads file - consists of a sequence of API calls'''
        files_tuple_full_paths =  get_the_csv_files_tuple()
        for file in files_tuple_full_paths:
            real_file_name = file.rsplit('\\',1)[1]
            payload={"filename": real_file_name}
            this_url = TestDataset.base_url + "/api/upload/datasets/upload_params"
            print("Set up URL for the first file upload step is " + this_url)
            r = requests.get(this_url, headers = {'Authorization': TestDataset.access_token}, params=payload)
            data = r.json()
            normalized_json = json_normalize(data)
            listed_response = list(normalized_json)
            amazon_url = listed_response[0].get("data.url")
            amazon_key = listed_response[0].get("data.fields.key")
            x_amz_algorithm = listed_response[0].get("data.fields.x-amz-algorithm")
            x_amz_credential = listed_response[0].get("data.fields.x-amz-credential")
            x_amz_date = listed_response[0].get("data.fields.x-amz-date")
            policy = listed_response[0].get("data.fields.policy")
            x_amz_signature = listed_response[0].get("data.fields.x-amz-signature")
            payload_for_upload={"key":amazon_key, "x-amz-algorithm":x_amz_algorithm, "x-amz-credential":x_amz_credential,"x-amz-date":x_amz_date, "policy":policy, "X-amz-signature":x_amz_signature}
            files = {"file": open(os.path.join(sys.path[0],real_file_name),'rb')}
            upload = requests.post(amazon_url, data=payload_for_upload, files = files)  
            #Validate on successful file upload
            if (upload.status_code) == 204:
                json_input = {"filepath":amazon_key}
                validation_response = requests.post(TestDataset.base_url + "/api/upload/datasets/validate", headers = {'Authorization': TestDataset.access_token}, json=json_input)
                print(f"Upload to amazon was a success with status code {validation_response.status_code}") #200   
                print(f"Returned json is {validation_response.json()}") 
                dataset_id = validation_response.json().get("id")    
            response = "STARTED"
            while response != "SUCCESS": 
                print("Checking...") 
                print(f"Dataset upload status is {response}")  
                check_response = requests.get(TestDataset.base_url + "/api/task/" + dataset_id + "/status", headers = {'Authorization': TestDataset.access_token})
                response = check_response.json().get('status')
                sleep(1)
            result_upload_response = requests.get(TestDataset.base_url + "/api/task/" + dataset_id + "/result", headers = {'Authorization': TestDataset.access_token})
            data_result_upload_response = result_upload_response.json()

    @staticmethod
    def teardown_class(cls):
        '''Deletes all datasets'''
        get_datasets = requests.get(TestDataset.base_url + "/api/datasets", headers = {'Authorization': TestDataset.access_token})
        response_json = get_datasets.json()
        dataset_id_list = []
        if len(response_json) != 0:
            for item in response_json:
                dataset_id_list.append(item.get("dataset_id"))
            for dataset_id in dataset_id_list:    
                delete_dataset = requests.delete(TestDataset.base_url + "/api/datasets/" + dataset_id, headers = {'Authorization': TestDataset.access_token})
                assert (delete_dataset.status_code) == 204
        else: 
            print("Skipping the datasets deletion. No existing datasets found for the account") 

#####################  Tests begin #####################

    def test_incorrect_auth_token(self):
        '''Checks that the reponse is 422 if the authentication token is incorrect'''
        r = requests.get(self.base_url + "/api/datasets",
                        headers={'Authorization': self.access_token_expired})
        response_json = r.json()
        assert response_json.get("detail") == "Signature verification failed"
        assert r.status_code == 422
 
    def test_dataset_data(self):
        '''Downloads the data for a particular dataset with pagination/Result model'''
        dataset_id = get_dataset_id("20_20tab.csv")
        r = requests.get(self.base_url + "/api/datasets/" + dataset_id +
                        "/result_model?page=1&per_page=25&sort=&order=", headers={'Authorization': self.access_token})
        assert r.status_code == 200

########################   Datasets   ##########################

    def test_the_rows_cols_in_uploaded_datasets(self):
        '''Verifies that the number of rows/cols in downloaded datasets is returned correct 
           and it the same as in initial .csv files 
           which were uploaded during test setup'''
        files = get_the_csv_files_tuple()
        files_info_from_csv = get_the_number_of_rows_cols_for_all_csv_files(files)
        all_datasets_dict = get_all_datasets_dict()
        for key in files_info_from_csv:
            if key in all_datasets_dict:
                print(key, 'from csv file---->', files_info_from_csv[key]) 
                print(key, 'from dataset----->', all_datasets_dict[key])
                assert files_info_from_csv[key]["rows"]  == all_datasets_dict[key]["rows_num"]
                assert files_info_from_csv[key]["cols"]  == all_datasets_dict[key]["cols_num"]

    def test_rename_dataset(self):
        '''Renames an existing dataset with passed in new name'''
        
        #hardcode - to be refactored alter - dataset name "1com.csv" is renamed with "test_dataset.csv"
        dataset_id = get_dataset_id("1com.csv")
        print(dataset_id)
        r = requests.patch(self.base_url + "/api/datasets/" + dataset_id + "?name=" + "test_dataset.csv", headers={'Authorization': self.access_token})
        assert r.status_code == 204

#########################   Feedback   ##########################

    def test_post_feedback(self):
        '''Creates new feedback and saves its "created_id" in variable feedback_id'''
        global feedback_id
        data_input = {"type_of_request":"Report an issue", "user_issue":"New bug is found", "screenshots":"undefined", "email":"123@123.com", "form_name":"feedback-form"}
        feedback_response = requests.post(self.base_url + "/api/feedback/user-feedback", headers = {'Authorization': self.access_token}, data=data_input)
        print(feedback_response.json())
        feedback_id = feedback_response.json().get("created_id")
        print(f"Created_id returned --- {feedback_id} ---")
        assert feedback_response.status_code == 201

    def test_get_feedback_exists(self):
        '''Verifies that for the crated feedback its id is returned back when requesting the list of all submitted feedbacks '''
        global feedback_id
        feedback_response = requests.get(self.base_url + "/api/feedback/user-feedback", headers = {'Authorization': self.access_token})
        feedback_response_json_list = feedback_response.json()
        print(type(feedback_response_json_list))
        ids_list = list()
        for item in feedback_response_json_list:
            ids_list.append(item.get("id"))
        print("Assert starts") 
        print(f"Saved created_id --- {feedback_id} ---")   
        assert feedback_response.status_code == 200
        assert feedback_id in ids_list 

    def test_delete_feedback(self):
        '''Verifies that when deleting the feedback by its correct id the status_code in the response is 
           204 and when it is not correct id the the response is 404'''    
        feedback_response = requests.delete(self.base_url + "/api/feedback/user-feedback/" + feedback_id, headers = {'Authorization': self.access_token})
        assert feedback_response.status_code == 204 
        feedback_response = requests.delete(self.base_url + "/api/feedback/user-feedback/" + feedback_id + "1", headers = {'Authorization': self.access_token})
        assert feedback_response.status_code == 404 

############################   User   #############################  

    def test_rename_user(self):
        '''Gets user data and renames an existing/current user and then verifies the name change'''
        r = requests.get(self.base_url + "/api/authorization/me", headers={'Authorization': self.access_token})
        assert r.status_code == 200
        user_name = r.json().get("name")
        if len(user_name) <= 25:
            new_user_name = user_name + "1"
        else:
            new_user_name = "test_user"    
        rename = requests.patch(self.base_url + "/api/authorization/name", headers={'Authorization': self.access_token}, params = {"name":new_user_name})
        assert(rename.status_code) == 200
        check = requests.get(self.base_url + "/api/authorization/me", headers={'Authorization': self.access_token})
        assert check.json().get("name") == new_user_name

