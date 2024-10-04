import requests
import json

def get_company_data(company_id):
    url = "https://targetare.ro/api/graphql"

    headers = {
        "Host": "targetare.ro",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "content-type": "application/json",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=4"
    }

    payload = {
        "operationName": "autocomplete",
        "variables": {"text": company_id},
        "query": """query autocomplete($text: String) {
          autocomplete(text: $text) {
            companyId
            companyName
            county
            taxId
            county
            status
            primaryEmail
            caen
            __typename
          }
        }"""
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    # Check the response
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Request failed with status code {response.status_code}")
        print("Response text:", response.text)
        return None


