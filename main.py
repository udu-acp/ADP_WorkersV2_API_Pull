import requests
import json
import os

# Set token request body variables
grant_type = 'client_credentials'
client_id = os.getenv("EpochSL_Client_ID")
client_secret = os.getenv("EpochSL_Client_Secret")
bearer_token = ""

# Set SSL Client Certificates
cert_path = "C:/Users/JoshuaUdume/companyname_auth.pem"
key_path = "C:/Users/JoshuaUdume/companyname_auth.key"

# Set Headers
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

# POST the request and parse the response
# resp = requests.post(url, data=pl, cert=(cert_path, key_path))
# j = json.loads(resp.text)

# Save the Bearer Token as a variable

# context.updateVariable('adp_access_token', j['access_token'])

print(client_id)