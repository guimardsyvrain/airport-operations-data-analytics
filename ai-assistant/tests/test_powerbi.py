import os
import msal
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
DATASET_ID = os.getenv("DATASET_ID")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

# Authenticate
app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY
)

result = app.acquire_token_interactive(scopes=SCOPES)

if "access_token" not in result:
    raise Exception(result.get("error_description"))

print("Authentication successful.")

# Test one existing measure from the semantic model
dax_query = """
EVALUATE
ROW(
    "On-Time Performance",
    [On-Time Performance %]
)
"""

# Power BI Execute Queries endpoint
url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/executeQueries"

headers = {
    "Authorization": f"Bearer {result['access_token']}",
    "Content-Type": "application/json"
}

body = {
    "queries": [
        {
            "query": dax_query
        }
    ],
    "serializerSettings": {
        "includeNulls": True
    }
}

response = requests.post(url, headers=headers, json=body)

print("Status:", response.status_code)
print(response.text)