import requests
import json
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
import io

# Set token request body variables
grant_type = 'client_credentials'
# client_id = "b844b1f3-09d6-4b37-a9f2-e2395e562799"
# client_secret = "5c3e3573-1cd9-4a33-af72-387fa0102774"
client_id = os.getenv('EPOCHSL_CLIENT_ID')
client_secret = os.getenv("EPOCHSL_CLIENT_SECRET")


# Set SSL Client Certificates
cert_path = "C:/Users/JoshuaUdume/companyname_auth.pem"
key_path = "C:/Users/JoshuaUdume/companyname_auth.key"

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


# create empty df to hold data
df = pd.DataFrame()

# create lists to append to
associateIDs = []
workerID = []
hiredates = []
termdates = []
status = []
department = []
last_name = []
first_name = []
full_name = []

# loop through all elements in the workers API response and add to individual lists
for n in range(0, len(data_dict)):
  try:
    associateIDs.append(data_dict[n]["associateOID"])
  except Exception as e:
    associateIDs.append("") # Set to NULL in Matillion DPC
    
  try:
    workerID.append(data_dict[n]["workerID"]["idValue"])
  except Exception as e:
    workerID.append("")
   
  # grab the last "workAssignments" list entry to account for transfers
  try:
    hiredates.append(data_dict[n]["workAssignments"][-1]["hireDate"])
  except Exception as e:
    hiredates.append("")
    
  try:
    termdates.append(data_dict[n]["workAssignments"][-1]["terminationDate"])
  except Exception as e:
    termdates.append("")
    
  try:
    status.append(data_dict[n]["workAssignments"][-1]["assignmentStatus"]["statusCode"]["longName"])
  except Exception as e:
    status.append("")
    
  # the department value can be in two locations and some records don't have both options, so this trys for either if they are available
  try:
    if data_dict[n]["workAssignments"][-1]["assignedOrganizationalUnits"][0]["typeCode"]["shortName"] == 'Department':
      department.append(data_dict[n]["workAssignments"][-1]["assignedOrganizationalUnits"][0]["nameCode"]["shortName"])
    elif data_dict[n]["workAssignments"][-1]["assignedOrganizationalUnits"][1]["typeCode"]["shortName"] == 'Department':
      department.append(data_dict[n]["workAssignments"][-1]["assignedOrganizationalUnits"][1]["nameCode"]["shortName"])
  except Exception as e:
    department.append("")
  
  try:
    first_name.append(data_dict[n]["person"]["legalName"]["givenName"])
  except Exception as e:
    status.append("")
  
  try:
    last_name.append(data_dict[n]["person"]["legalName"]["familyName1"])
  except Exception as e:
    status.append("")
  
  try:
    full_name.append(data_dict[n]["person"]["legalName"]["formattedName"])
  except Exception as e:
    status.append("")
    

# turn lists into columns of dataframe
df["associate0ID"] = associateIDs
df["workerID"] = workerID
df['hire_date'] = hiredates
df['term_date'] = termdates
df['status'] = status
df['department'] = department
df['first_name'] = first_name
df['last_name'] = last_name
df['full_name'] = full_name


# convert df to string
output = df.to_csv(index=False, encoding="utf-8")

# connect to azure blob storage
account_name = "epochmatillion"
account_key = os.getenv("EPOCHSL_AZURE_ACCOUNT_KEY")
container_name = "adp"
blob_name = "APIs/WorkersV2.csv"
account_url = "https://epochmatillion.blob.core.windows.net"


# Create the BlobServiceClient objects, access contianer, blob, and then upload blob
blob_service = BlobServiceClient(account_url, credential=account_key)
container_client = blob_service.get_container_client(container_name)
blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
job_status = ""
try:
  blob_client.upload_blob(output, overwrite=True, content_settings=ContentSettings(content_type="text/csv"))
  job_status = "success"
except:
  job_status = "failed"

## Matillion Varible Logic
# context.updateVariable("blob_upload_status", job_status)