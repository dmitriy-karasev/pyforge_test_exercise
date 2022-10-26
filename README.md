# pyforge_test_exercise

The dependencies/libs are in "requirements.txt" file

Dataset files to upload/test are in .\tests folder

They are to be discovered and uploaded one-by-one during test suite setup.

Suite teardown will delete all tests after tests are run.

.env file contains security token (to be updated manually before test execution) and the base URL for the environment under test.

The file utilities.py contains some helper functions for work with files.

Main test class/tests are inside test_dataset.py.

Tests done so far include checks for correct number of rows/columns in uploaded datasets,
tests for feedback posting/retrieving, dataset renaming and user renaming.

At the moment, though, QA environment is not available - 503 error is returned while accessing it and on DEV environment there is some problem with datasets upload, the datasets upload status remains "PENDING" - it should become "SUCCESS" in around 3-5 seconds.   
