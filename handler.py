import json
import os

import requests
from neo4j.v1 import GraphDatabase
from requests_toolbelt import MultipartEncoder

from util.encryption import decrypt_value_str

host_port = decrypt_value_str(os.environ['GRAPHACADEMY_DB_HOST_PORT'])
user = decrypt_value_str(os.environ['GRAPHACADEMY_DB_USER'])
password = decrypt_value_str(os.environ['GRAPHACADEMY_DB_PW'])

db_driver = GraphDatabase.driver(f"bolt://{host_port}", auth=(user, password), max_retry_time=15)

community_content_query = """\
MERGE (user:DiscourseUser {id: $params.topic.user_id })
SET user.name = $params.topic.created_by.username,
    user.avatarTemplate = $params.topic.created_by.avatar_template

MERGE (topic:DiscourseTopic {id: $params.topic.id })
SET topic.title = $params.topic.title,
    topic.createdAt = datetime($params.topic.created_at),
    topic.slug = $params.topic.slug,
    topic.approved = $params.approved,
    topic.rating = $params.rating

MERGE (user)-[:POSTED_CONTENT]->(topic)
"""


def community_content(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]

    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    tags = json_payload["topic"]["tags"]
    kudos_tags = [tag for tag in tags if tag.startswith("kudos")]

    if len(kudos_tags) > 0:
        json_payload["approved"] = True
        json_payload["rating"] = int(kudos_tags[0].split("-")[-1])

    with db_driver.session() as session:
        result = session.run(community_content_query, {"params": json_payload})
        print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


user_events_query = """\
MERGE (discourse:DiscourseUser {id: $params.user.id })
SET discourse.name = $params.user.username,
    discourse.location = $params.user.location,
    discourse.avatarTemplate = $params.user.avatar_template,
    discourse.screenName = $params.user.name
WITH discourse
OPTIONAL MATCH (user:User {auth0_key: $params.user.external_id})
MERGE (user)-[:DISCOURSE_ACCOUNT]->(discourse)

WITH user

OPTIONAL MATCH (twitter:Twitter {screen_name: $params.user.user_fields.`4`})
FOREACH (_ IN CASE twitter WHEN null THEN [] ELSE [1] END |
   MERGE (user)-[:TWITTER_ACCOUNT]->(twitter))

WITH user

OPTIONAL MATCH (github:GitHub {name: $params.user.user_fields.`6`})
FOREACH (_ IN CASE github WHEN null THEN [] ELSE [1] END |
   MERGE (user)-[:GITHUB_ACCOUNT]->(github))


WITH user
so
OPTIONAL MATCH (:StackOverflow {id: toInteger($params.user.user_fields.`5`)})
FOREACH (_ IN CASE so WHEN null THEN [] ELSE [1] END |
   MERGE (user)-[:STACKOVERFLOW_ACCOUNT]->(so))
"""


def user_events(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]

    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    with db_driver.session() as session:
        result = session.run(user_events_query, {"params": json_payload})
        print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}
