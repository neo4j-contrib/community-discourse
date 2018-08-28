import json
import os
import re
import requests

from bs4 import BeautifulSoup
from neo4j.v1 import GraphDatabase
from requests_toolbelt import MultipartEncoder
from util.encryption import decrypt_value_str, encrypt_value

host_port = decrypt_value_str(os.environ['GRAPHACADEMY_DB_HOST_PORT'])
user = decrypt_value_str(os.environ['GRAPHACADEMY_DB_USER'])
password = decrypt_value_str(os.environ['GRAPHACADEMY_DB_PW'])

db_driver = GraphDatabase.driver(f"bolt://{host_port}", auth=(user, password), max_retry_time=15)

discourse_api_key = decrypt_value_str(os.environ['DISCOURSE_API_KEY'])
discourse_api_user = decrypt_value_str(os.environ['DISCOURSE_API_USER'])

community_content_query = """\
MERGE (user:DiscourseUser {id: $params.topic.user_id })
SET user.name = $params.topic.created_by.username,
    user.avatarTemplate = $params.topic.created_by.avatar_template

MERGE (topic:DiscourseTopic {id: $params.topic.id })
SET topic.title = $params.topic.title,
    topic.createdAt = datetime($params.topic.created_at),
    topic.slug = $params.topic.slug,
    topic.approved = $params.approved,
    topic.rating = $params.rating,
    topic.likeCount = toInteger($params.topic.like_count),
    topic.views = toInteger($params.topic.views),
    topic.replyCount = toInteger($params.topic.replyCount)

MERGE (user)-[:POSTED_CONTENT]->(topic)
"""

community_content_active_query = """\
OPTIONAL MATCH (topic:DiscourseTopic {id: $params.topic.id })
RETURN topic
"""

kudos_message = """
Thanks for submitting!
 
I’ve added a tag that allows your blog to be displayed on the community home page!
"""


def community_content(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    with db_driver.session() as session:
        result = session.run(community_content_active_query, {"params": json_payload})
        row = result.peek()
        content_already_approved = row.get("topic").get("approved") if row.get("topic") else False

    tags = json_payload["topic"]["tags"]
    kudos_tags = [tag for tag in tags if tag.startswith("kudos")]

    if len(kudos_tags) > 0:
        json_payload["approved"] = True
        json_payload["rating"] = int(kudos_tags[0].split("-")[-1])

    with db_driver.session() as session:
        result = session.run(community_content_query, {"params": json_payload})
        print(result.summary().counters)

    if len(kudos_tags) > 0 and not content_already_approved:
        uri = f"https://community.neo4j.com/posts.json"

        payload = {
            "api_key": discourse_api_key,
            "api_user_name": discourse_api_user,
            "topic_id": str(json_payload["topic"]["id"]),
            "raw": kudos_message
        }

        m = MultipartEncoder(fields=payload)
        r = requests.post(uri, data=m, headers={'Content-Type': m.content_type})
        print(r)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


user_events_query = """\
MERGE (discourse:DiscourseUser {id: $params.user.id })
SET discourse.name = $params.user.username,
    discourse.location = $params.user.location,
    discourse.avatarTemplate = $params.user.avatar_template,
    discourse.screenName = $params.user.name
WITH discourse

MERGE (user:User {auth0_key: $params.user.external_id})
ON CREATE SET user.email = $params.user.email
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

OPTIONAL MATCH (so:StackOverflow {id: toInteger($params.user.user_fields.`5`)})
FOREACH (_ IN CASE so WHEN null THEN [] ELSE [1] END |
   MERGE (user)-[:STACKOVERFLOW_ACCOUNT]->(so))
"""


def user_events(request, context):
    headers = request["headers"]
    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    if event_type == "user" and event == "user_created":
        print("User created so we'll update everything when they login")
        return

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    with db_driver.session() as session:
        result = session.run(user_events_query, {"params": json_payload})
        print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


import_twin4j_query = """\
    MERGE (twin4j:TWIN4j {date: datetime($date) })
    SET twin4j.image = $image, twin4j.summaryText = $summaryText, twin4j.link = $link
    
    FOREACH(tag IN $allTheTags |
      MERGE (t:TWIN4jTag {tag: tag.tag, anchor: tag.anchor })
      MERGE (twin4j)-[:CONTAINS_TAG]->(t)
    )
    
    WITH twin4j
    UNWIND $people AS person
    OPTIONAL MATCH (twitter:User:Twitter) WHERE twitter.screen_name = person.screenName
    OPTIONAL MATCH (user:User) where user.id = toInteger(person.stackOverflowId)
    WITH coalesce(twitter, user) AS u, twin4j
    
    CALL apoc.do.when(u is NOT NULL, 'MERGE (twin4j)-[:FEATURED]->(u)', '', {twin4j: twin4j, u: u}) YIELD value
    RETURN value
    """


def import_twin4j(request, context):
    twin4j_posts = requests.get("https://neo4j.com/wp-json/wp/v2/posts?tags=3201").json()

    most_recent_post = twin4j_posts[0]

    date = most_recent_post["date"]
    link = most_recent_post["link"]

    html_content = most_recent_post["content"]["rendered"]

    soup = BeautifulSoup(html_content, "html.parser")

    featured_element = [tag for tag in soup.findAll("h3") if "Featured Community Member" in tag.text][0]
    match = re.match("Featured Community Members?: (.*)", featured_element.text)

    person = match.groups(1)[0].strip()
    people = [p.strip() for p in person.split(" and ")]

    if len(people) == 1:
        link_element = featured_element.find_all_next("a")[:1]
    else:
        link_element = featured_element.find_all_next("a")[:2]

    image = featured_element.parent.find_all("img")[0]["src"]

    print("Featured Community Member: ", [(link.text, link["href"]) for link in link_element])
    summary_text = soup.find_all("div")[2].text.strip()

    all_the_tags = [{"tag": tag.text, "anchor": tag["id"]}
                    for tag in soup.findAll("h3")
                    if "Featured Community Member" not in tag.text]

    params = {"people": [{"name": link.text,
                          "screenName": link["href"].split("/")[-1],
                          "stackOverflowId": link["href"].split("/")[-2] if "stackoverflow" in link["href"] else -1
                          }
                         for link in link_element],
              "date": date,
              "image": image,
              "summaryText": summary_text,
              "link": link,
              "allTheTags": all_the_tags}

    print(params)

    with db_driver.session() as session:
        result = session.run(import_twin4j_query, params)
        print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


def update_profile(request, context):
    headers = request["headers"]
    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    post_payload = json_payload.get("post")
    if event_type == "post" and event in ["post_edited", "post_created"] and post_payload and post_payload.get("post_number") == 1:
        post = json_payload["post"]
        username = post["username"]
        bio = post["cooked"]

        uri = f"https://community.neo4j.com/users/{username}.json"

        payload = {
            "api_key": discourse_api_key,
            "api_user_name": discourse_api_user,
            "bio_raw": bio,
        }

        m = MultipartEncoder(fields=payload)
        r = requests.put(uri, data=m, headers={'Content-Type': m.content_type})
        print(r)

        return {"statusCode": 200, "body": "Got the event", "headers": {}}


update_topics_query = """\
UNWIND $params AS t
MATCH (topic:DiscourseTopic {id: t.id })
SET topic.likeCount = toInteger(t.like_count),
    topic.views = toInteger(t.views),
    topic.replyCount = toInteger(t.replyCount)
"""


def update_topics(request, context):
    uri = "https://community.neo4j.com/c/68.json"

    r = requests.get(uri)
    json_payload = r.json()

    topics = [topic for topic in json_payload["topic_list"]["topics"]
              if not topic["pinned"]]
    print(topics)

    with db_driver.session() as session:
        result = session.run(update_topics_query, {"params": topics})
        print(result.summary().counters)
