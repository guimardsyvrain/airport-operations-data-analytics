import os
import msal
from dotenv import load_dotenv

# Load configuration from .env
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

# Create Microsoft authentication client
app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY
)

# Authenticate with your Microsoft account
result = app.acquire_token_interactive(
    scopes=SCOPES
)

if "access_token" in result:
    print("Authentication successful.")
else:
    print("Authentication failed.")
    print(result.get("error"))
    print(result.get("error_description"))