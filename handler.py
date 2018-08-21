import json
import os
from base64 import b64decode
from base64 import b64encode

import boto3
import requests
from requests_toolbelt import MultipartEncoder


def user_events(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]

    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)

    print(json_payload)

    discourse_api_key = decrypt_value_str(os.environ['DISCOURSE_API_KEY'])
    discourse_api_user = decrypt_value_str(os.environ['DISCOURSE_API_USER'])

    user_id = json_payload["user"]["id"]

    uri = f"https://community.neo4j.com/admin/users/{user_id}/groups"

    payload = {
        "api_key": discourse_api_key,
        "api_user_name": discourse_api_user,
        "group_id": "43"
    }

    print(payload)

    m = MultipartEncoder(fields=payload)

    r = requests.post(uri, data=m, headers={'Content-Type': m.content_type})
    print(r)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


def decrypt_value(encrypted):
    decrypted_response = boto3.client('kms').decrypt(CiphertextBlob=b64decode(encrypted))
    return decrypted_response['Plaintext']


def decrypt_value_str(encrypted):
    decrypted_response = boto3.client('kms').decrypt(CiphertextBlob=b64decode(encrypted))
    return decrypted_response['Plaintext'].decode("utf-8")


def encrypt_value(value, kms_key):
    return b64encode(boto3.client('kms').encrypt(Plaintext=value, KeyId=kms_key)["CiphertextBlob"])
