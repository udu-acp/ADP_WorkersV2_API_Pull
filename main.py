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

# Set GET Headers
headers = {"Authorization": f"Bearer {bearer_token}"}

# Set WorkersV2 URI Variables
endpoint = "https://api.adp.com/hr/v2/workers?$top=100&$skip="
skips = 0
URI = "{0}{1}".format(endpoint,skips)

print(URI)