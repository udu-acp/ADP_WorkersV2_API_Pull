# to call APIs
import requests
# to return response in json form
import json
# to grab environment variables
import os
# to convert lists into df
import pandas as pd
# to connect to blob store
from azure.storage.blob import BlobServiceClient, ContentSettings
# to pull in hidden variables in .env file
from dotenv import load_dotenv

# *keys: allows the function to accept any number of keys as separate args
# eg. statuses.append(safe_get(work_assignment, "assignmentStatus", "statusCode", "longName"))
def safe_get(data, *keys, default=""):
    # drill down through keys in data
    for key in keys:
        # if dictionary, go deeper
        if isinstance(data, dict):
            # if key exists, update data
            data = data.get(key, {})
        else:
            return default
    return data if data not in ({}, None) else default


# pull in environment variables
load_dotenv()

# Set token request body variables
grant_type = 'client_credentials'
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv("CLIENT_SECRET")


# Set SSL Client Certificates
cert_path = os.getenv('PEM_PATH')
key_path = os.getenv('KEY_PATH')

# Set POST Headers
content_type = 'application/x-www-form-urlencoded'

# The URL for all ADP token services
url = 'https://accounts.adp.com/auth/oauth/v2/token'

# Construct a Python dict containing all the headers, using the job variables
pl = {
    'grant_type': grant_type,
    'client_id': client_id,
    'client_secret': client_secret,
    'content-type': content_type
}

# store the bearer token from the response
resp = requests.post(url, data=pl, cert=(cert_path, key_path))
j = json.loads(resp.text)
bearer_token = j["access_token"]

### Matillion Logic
## Save the Bearer Token as a variable
# context.updateVariable('adp_access_token', bearer_token)

# Set GET Headers
headers = {"Authorization": f"Bearer {bearer_token}"}

# Set WorkersV2 URI Variables
endpoint = "https://api.adp.com/hr/v2/workers?$top=100&$skip="
skips = 0
URI = "{0}{1}".format(endpoint,skips)

# setup dictionary to append each call
data_dict = []

## loop each iteration for testing
# loop = 0

# Continue looping until status code changes (returns None)
while requests.get(URI, headers=headers, cert=(cert_path, key_path)).status_code == 200:
  data_dict += requests.get(URI, headers=headers, cert=(cert_path, key_path)).json()["workers"]
  skips += 100
  URI = "{0}{1}".format(endpoint,skips)
  # update loop variable for testing
  # loop += 1

# create lists to append to
associateIDs, workerIDs, originalhiredates = [], [], []
termdates, statuses, statuseffectivedates, departments = [], [], [], []
assignmenthiredates, assignmentstartdates, assignmenttermdates, assignmentstatusreasons = [], [], [], []
primaryindicators = []

# # test lists with sensative info
# last_names, first_names, full_names = [], [], []

# loop through all elements in the workers API response and add to individual lists
for worker in data_dict:
    # assignments
    work_assignment = safe_get(worker, "workAssignments", default=[{}])
    # loop through potential workAssignment list and create a complete record for each workAssignment
    for item in work_assignment:
        associateIDs.append(safe_get(worker, "associateOID"))
        workerIDs.append(safe_get(worker, "workerID", "idValue"))
        originalhiredates.append(safe_get(worker, "workerDates", "originalHireDate"))
        termdates.append(safe_get(worker, "workerDates", "terminationDate"))

        # grab assignment specific data
        # grab assignment hire date (it will be original hire date, if they haven't switched assignments)
        assignmenthiredates.append(safe_get(item, "hireDate"))
        assignmentstartdates.append(safe_get(item, "actualStartDate"))
        assignmenttermdates.append(safe_get(item, "terminationDate"))

        # grab shortname, longname, or set to ""
        if safe_get(item, "assignmentStatus", "reasonCode", "shortName"):
            assignmentstatusreasons.append(safe_get(item, "assignmentStatus", "reasonCode", "shortName"))
        elif safe_get(item, "assignmentStatus", "reasonCode", "longName"):
            assignmentstatusreasons.append(safe_get(item, "assignmentStatus", "reasonCode", "longName"))
        else:
            assignmentstatusreasons.append("")

        statuses.append(safe_get(item, "assignmentStatus", "statusCode", "longName"))
        statuseffectivedates.append(safe_get(item, "assignmentStatus", "effectiveDate"))
        primaryindicators.append(safe_get(item, "primaryIndicator"))

        # grab department
        # assignedOrganizationalUnits is a list of dicts
        org_units = safe_get(item, "assignedOrganizationalUnits", default=[])
        # iterate through org_units
        for unit in org_units:
            # if typeCode is Department, get shortName
            if safe_get(unit, "typeCode", "shortName") == 'Department':
                if safe_get(unit, "nameCode", "shortName"):
                    # get shortName from nameCode
                    department = safe_get(unit, "nameCode", "shortName")
                    # if department found
                    break
                elif safe_get(unit, "nameCode", "longName"):
                    # get longName from nameCode
                    department = safe_get(unit, "nameCode", "longName")
                    # if department found
                    break
        departments.append(department)
    
    # # info for QA
    # legal_name = safe_get(worker, "person", "legalName", default={})
    # first_names.append(safe_get(legal_name, "givenName"))
    # last_names.append(safe_get(legal_name, "familyName1"))
    # full_names.append(safe_get(legal_name, "formattedName"))

# convert lists into columns in dataframe
df = pd.DataFrame({
   'associate0ID': associateIDs,
   'workerID': workerIDs,
   'hire_date': originalhiredates,
   'term_date': termdates,
   'department': departments,
   'assignment_hire_date': assignmenthiredates,
   'assignment_start_date': assignmentstartdates,
   'assignment_term_date':assignmenttermdates,
   'assignment_status_reason': assignmentstatusreasons,
   'status': statuses,
   'status_effective_date': statuseffectivedates,
   'is_primary_record': primaryindicators,
#    'first_name': first_names,
#    'last_name': last_names,
#    'full_name': full_names
})


# convert df to string
output = df.to_csv(index=False, encoding="utf-8")
# print(output)

# connect to azure blob storage
account_name = os.getenv("AZURE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_ACCOUNT_KEY")
container_name = "adp"
blob_name = "APIs/WorkersV2.csv"
account_url = os.getenv('AZURE_ACCOUNT_URL')


# Create the BlobServiceClient objects, access contianer, blob, and then upload blob
blob_service = BlobServiceClient(account_url, credential=account_key)
container_client = blob_service.get_container_client(container_name)
blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
job_status = ""
try:
  blob_client.upload_blob(output, overwrite=True, content_settings=ContentSettings(content_type="text/csv"))
  job_status = "success"
except Exception as e:
  job_status = "failed"

print(job_status)

## Matillion Varible Logic
# context.updateVariable("blob_upload_status", job_status)