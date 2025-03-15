import requests
from requests.auth import HTTPDigestAuth

# Replace with your MarkLogic server details
host = "localhost"
port = 8010  # Port for the new REST API instance
username = "avperka"
password = "@Avyaya02"

# The endpoint for evaluating XQuery statements
eval_url = f"http://{host}:{port}/v1/eval"

# Define the XQuery statement
xquery_statement = """
xquery version "1.0-ml";
<response>
  <message>Hello, World!</message>
</response>
"""

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/xml"
}

# Digest Authentication
auth = HTTPDigestAuth(username, password)

# Define the payload for the request
data = {
    "xquery": xquery_statement
}

try:
    response = requests.post(eval_url, headers=headers, data=data, auth=auth)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

    if response.status_code == 200:
        print("XQuery executed successfully.")
        print("Response:")
        print(response.text)
    else:
        print(f"Failed to execute XQuery. Status code: {response.status_code}")
        print(response.text)
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    if e.response is not None:
        print(f"Response content: {e.response.content}")
