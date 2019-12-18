import json
import os
import re
import requests
import feedparser
import boto3

from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from requests_toolbelt import MultipartEncoder
from util.encryption import decrypt_value_str, encrypt_value

from retrying import retry
import random
import datetime
from jinja2 import Template

ssmc = boto3.client('ssm')

def get_ssm_param(key):
  resp = ssmc.get_parameter(
    Name=key,
    WithDecryption=True
  )
  return resp['Parameter']['Value']

discourse_blog_category_id = 122

host_port = get_ssm_param('com.neo4j.graphacademy.dbhostport')
user = get_ssm_param('com.neo4j.graphacademy.dbuser')
password = get_ssm_param('com.neo4j.graphacademy.dbpassword')

db_driver = GraphDatabase.driver("bolt+routing://%s" % (host_port), auth=(user, password), max_retry_time=15)

discourse_api_key =  get_ssm_param('com.neo4j.devrel.discourse.apikey')
discourse_api_user =  get_ssm_param('com.neo4j.devrel.discourse.apiusername')

discourse_root_api_key =  get_ssm_param('com.neo4j.devrel.discourse.rootapikey')


# {'topic':
#      {'tags': ['kudos-4'],
#       'id': 1241, 'title': 'Boltalyzer', 'fancy_title': 'Boltalyzer', 'posts_count': 1, 'created_at': '2018-09-06T21:42:52.784Z', 'views': 224,
#       'reply_count': 0,
#       'like_count': 1,
#       'last_posted_at': '2018-09-06T21:42:52.853Z', 'visible': True, 'closed': False, 'archived': False,
#       'archetype': 'regular', 'slug': 'boltalyzer',
#       'category_id': 9, 'word_count': 56, 'deleted_at': None, 'pending_posts_count': 0, 'user_id': 10, 'featured_link': None, 'pinned_globally': False, 'pinned_at': None, 'pinned_until': None, 'unpinned': None, 'pinned': False,
#       'highest_post_number': 1, 'deleted_by': None, 'has_deleted': False, 'bookmarked': None, 'participant_count': 1,
#       'created_by': {'id': 10, 'username': 'david.allen', 'name': 'M. David Allen', 'avatar_template': '/user_avatar/community.neo4j.com/david.allen/{size}/11_2.png'},
#       'last_poster': {'id': 10, 'username': 'david.allen', 'name': 'M. David Allen', 'avatar_template': '/user_avatar/community.neo4j.com/david.allen/{size}/11_2.png'}}}

import_post_query = """\
MERGE (user:DiscourseUser {id: $params.post.user_id })
ON CREATE SET user.name = $params.post.username,
    user.avatarTemplate = $params.post.avatar_template

MERGE (topic:DiscourseTopic {id: $params.post.topic_id })
SET topic.title = $params.post.topic_title, topic.slug = $params.post.topic_slug

MERGE (user)-[:POSTED_CONTENT]->(topic)

MERGE (post:DiscoursePost {id: $params.post.id})
SET post.text = $params.post.cooked, post.createdAt = datetime($params.post.created_at),
    post.number = $params.post.post_number

MERGE (user)-[:POSTED_CONTENT]->(post)
MERGE (post)-[:PART_OF]->(topic)
"""

import_topic_query = """\
MATCH (category:DiscourseCategory {id: $params.topic.category_id})

MERGE (user:DiscourseUser {id: $params.topic.user_id })
ON CREATE SET user.name = $params.topic.created_by.username,
              user.avatarTemplate = $params.topic.created_by.avatar_template
MERGE (topic:DiscourseTopic {id: $params.topic.id })
SET topic.title = $params.topic.title,
    topic.createdAt = datetime($params.topic.created_at),
    topic.slug = $params.topic.slug,
    topic.approved = $params.approved,
    topic.rating = $params.rating,
    topic.likeCount = toInteger($params.topic.like_count),
    topic.views = toInteger($params.topic.views),
    topic.replyCount = toInteger($params.topic.reply_count),
    topic.categoryId = $params.topic.category_id


MERGE (topic)-[:IN_CATEGORY]->(category)
MERGE (user)-[:POSTED_CONTENT]->(topic)
"""

def import_posts_topics(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)

    if event_type == "topic" and json_payload["topic"]["archetype"] == "regular":
        with db_driver.session() as session:
            result = session.run(import_topic_query, {"params": json_payload})
            print(result.summary().counters)

    if event_type == "post" and json_payload["post"]["topic_archetype"] == "regular":
        with db_driver.session() as session:
            result = session.run(import_post_query, {"params": json_payload})
            print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


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
    topic.replyCount = toInteger($params.topic.reply_count),
    topic.categoryId = $params.topic.category_id

MERGE (user)-[:POSTED_CONTENT]->(topic)
"""

community_content_active_query = """\
OPTIONAL MATCH (topic:DiscourseTopic {id: $params.topic.id })
RETURN topic
"""

kudos_message = """
Thanks for submitting!

I've added a tag that allows your blog to be displayed on the community home page!
"""

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def get_community_content_active(params):
    with db_driver.session() as session:
        result = session.run(community_content_active_query, params)
        print(result.summary().counters)
        row = result.peek()
        content_already_approved = row.get("topic").get("approved") if row.get("topic") else False
        return content_already_approved

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_community_content(params):
    with db_driver.session() as session:
        result = session.run(community_content_query, params)
        print(result.summary().counters)
        return True

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_user_events(params):
    with db_driver.session() as session:
        result = session.run(user_events_query, params)
        print(result.summary().counters)
        return True

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_import_twin4j(params):
    with db_driver.session() as session:
        result = session.run(import_twin4j_query, params)
        print(result.summary().counters)
        return True

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_update_topics(params):
    with db_driver.session() as session:
        result = session.run(update_topics_query, params)
        print(result.summary().counters)
        return True


def community_content(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    content_already_approved = get_community_content_active({"params": json_payload})

    tags = json_payload["topic"]["tags"]
    kudos_tags = [tag for tag in tags if tag.startswith("kudos")]

    if len(kudos_tags) > 0:
        json_payload["approved"] = True
        json_payload["rating"] = int(kudos_tags[0].split("-")[-1])

    set_community_content({"params": json_payload})

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
        return {"statusCode": 200, "body": "It was just a user creation event", "headers": {}}
    elif event_type == "user" and event == "user_destroyed":
        print("User destroyed so no longer need to care about user")
        return {"statusCode": 200, "body": "It was just a user destruction event -- no action as dont know reason", "headers": {}}


    body = request["body"]
    json_payload = json.loads(body)
    print(json_payload)

    set_user_events({"params": json_payload})

    return {"statusCode": 200, "body": "Updated user", "headers": {}}


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

edu_discourse_users_query = """
MATCH (edu:EduApplication)-[r:SUBMITTED_APPLICATION]-(user:User)-[r2:DISCOURSE_ACCOUNT]-(discourse:DiscourseUser)
WHERE edu.status = 'APPROVED'
RETURN discourse.name as discourse_users
"""

def assign_edu_group(params):
    counter = 0
    uri = f"https://community.neo4j.com/groups/49/members.json"

    with db_driver.session() as session:
      result = session.run(edu_discourse_users_query, {})

      for record in result:
        group = group + ',' + record
        counter = counter + 1

        payload = {
            "api_key": discourse_api_key,
            "api_user_name": discourse_api_user,
            "usernames": group
        }

        print(payload)

        m = MultipartEncoder(fields=payload)
        r = requests.put(uri, data=m, headers={'Content-Type': m.content_type})
        print(r)
        print("Added %d users to Edu group" % (counter))

edu_discourse_invite_query = """
MATCH (edu:EduApplication)-[r:SUBMITTED_APPLICATION]-(user:User)
WHERE edu.status = 'APPROVED'
AND NOT exists(user.discourseInviteSent)
AND NOT exists((user)-[:DISCOURSE_ACCOUNT]-(:DiscourseUser))
RETURN DISTINCT(user.email) as edu_email
"""

edu_discourse_invited_update = """
WITH $params.result as usersInvited
MATCH (user:User)-[:SUBMITTED_APPLICATION]->(:EduApplication)
WHERE user.email IN usersInvited
 SET user.discourseInviteSent = datetime()
RETURN count(user)
"""

def send_edu_discourse_invites(params):
    uri = f"https://community.neo4j.com/invites"

    with db_driver.session() as session:
      result = session.run(edu_discourse_invite_query, {})

      for record in result:
        payload = {
            "api_key": discourse_api_key,
            "api_user_name": discourse_api_user,
            "email": record,
            "group_names": "Neo4j-Educators",
            "custom_message": "The Neo4j Educator Program includes access to a private channel on our Community Site where you can ask questions, share resources, and learn from others. Join us!"
        }

        print(payload)

        m = MultipartEncoder(fields=payload)
        r = requests.post(uri, data=m, headers={'Content-Type': m.content_type})
        print(r)

        updatedUsers = session.run(edu_discourse_invited_update, {params: result})

        return "Updated %d users invited" % (updatedUsers)

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

    set_import_twin4j(params)

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

        return {"statusCode": 200, "body": "Updated user bio", "headers": {}}
    else:
        return {"statusCode": 200, "body": "No action necessary", "headers": {}}


update_topics_query = """\
UNWIND $params AS t
MATCH (topic:DiscourseTopic {id: t.id })
SET topic.likeCount = toInteger(t.like_count),
    topic.views = toInteger(t.views),
    topic.replyCount = toInteger(t.replyCount)
"""

store_medium_post_query = """\
MATCH (ma:MediumAuthor {name: $author})
MERGE (mp:MediumPost {guid: $guid})
ON CREATE SET
  mp.title = $title,
  mp.author = $author,
  mp.url = $url,
  mp.content = $content,
  mp.date = $date
MERGE (mp)-[:AUTHORED_BY]->(ma)
"""

get_medium_posts_query = """\
MATCH (mp:MediumPost)-[:AUTHORED_BY]->(ma:MediumAuthor)-[:HAS_DISCOURSE]->(du:DiscourseUser)
WHERE
  mp.discourse_id IS NULL
RETURN
  mp.title AS title,
  mp.url AS url,
  mp.content AS content,
  mp.date AS date,
  mp.guid AS guid,
  du.name AS discourse_user
"""

set_medium_post_posted_query = """\
MATCH (mp:MediumPost)
WHERE
  mp.guid = $mediumId
SET
  mp.discourse_id = $discourseId
"""

def fetch_medium_posts(request, context):
    d = feedparser.parse('https://medium.com/feed/neo4j')
    post_count = 0
    for entry in d.entries:
      post_count = post_count + 1
      guid = entry.guid
      blog_author = entry.author
      blog_url = entry.link
      blog_title = entry.title
      blog_content = entry.content[0].value
      date = entry.published_parsed
      blog_date = "%d-%02d-%02d" % (date.tm_year, date.tm_mon, date.tm_mday)
      with db_driver.session() as session:
        params = {"title": blog_title, "author": blog_author, "url": blog_url, "date": blog_date, "guid": guid, "content": blog_content}
        result = session.run(store_medium_post_query, params)
        result.consume()

def post_medium_to_discourse(request, context):
    counter = 0
    with db_driver.session() as session:
      result = session.run(get_medium_posts_query, {})
      for record in result:
        dt = post_topic_to_discourse(discourse_blog_category_id, record['discourse_user'], record['title'], record['content'], record['date'])
        counter = counter + 1
        if 'id' in dt:
          params = {"discourseId": dt['id'], "mediumId": record['guid']}
          update_res = session.run(set_medium_post_posted_query, params)
          update_res.consume()
    return "Posted %d posts to discourse" % (counter)

def post_topic_to_discourse(category, username, title, body, published_date):
    post_data = {}
    post_data['title'] = title
    post_data['category'] = category
    post_data['raw'] = body
    post_data['created_at'] = published_date
    params = "api_key=%s&api_username=%s" % (discourse_root_api_key, username)
    r = requests.post("https://community.neo4j.com/posts.json?%s" % (params), json=post_data)
    print(r.content)
    return json.loads(r.content)

def update_topics(request, context):
    uri = "https://community.neo4j.com/c/68.json"

    r = requests.get(uri)
    json_payload = r.json()

    topics = [topic for topic in json_payload["topic_list"]["topics"]
              if not topic["pinned"]]
    print(topics)

    set_update_topics({"params": topics})

update_categories_subcategories_query = """\
UNWIND $params AS event
MERGE (category:DiscourseCategory {id: event.id})
SET category.name = event.name, category.description = event.description
WITH category, event
UNWIND event.subcategory_ids AS subCategoryId
MERGE (subCategory:DiscourseCategory {id: subCategoryId})
MERGE (subCategory)-[:CHILD]->(category)
"""

update_categories_query = """\
UNWIND $params AS event
MERGE (category:DiscourseCategory {id: event.id})
SET category.name = event.name, category.description = event.description
"""

def update_categories_tx_fn(tx, params):
    tx.run(tx, params=params)

def update_categories(request, context):
    uri = f"https://community.neo4j.com/categories.json"

    payload = {
        "api_key": discourse_api_key,
        "api_user_name": discourse_api_user
    }

    r = requests.get(uri, data=payload)
    response = r.json()
    print(len(response["category_list"]["categories"]))

    with db_driver.session() as session:
        categories = response["category_list"]["categories"]
        result = session.run(update_categories_subcategories_query, params=categories)
        print(result.summary().counters)

    r = requests.get("https://community.neo4j.com/site.json", data=payload)
    response = r.json()
    categories = response["categories"]

    with db_driver.session() as session:
        result = session.run(update_categories_query, params=categories)
        print(result.summary().counters)

ninjas_so_query = """\
WITH $now as currentMonth
Match (u:User:StackOverflow)
match (u)-[:POSTED]->(a:Answer)-[:ANSWERED]->(q:Question)
WHERE apoc.date.format(coalesce(a.created,q.created),'s','yyyy-MM') = currentMonth
with *, apoc.date.format(coalesce(a.created,q.created),'s','yyyy-MM-W') as week
with currentMonth, week, u.name as user, count(*) as total, sum(case when a.is_accepted then 1 else 0 end) as accepted
ORDER BY total DESC
return currentMonth, user, collect([week,total,accepted]) as weekly
"""


ninajs_discourse_query = """\
MATCH path = (u)-[:POSTED_CONTENT]->(post:DiscoursePost)-[:PART_OF]->(topic)-[:IN_CATEGORY]->(category)
WHERE datetime({year:$year, month:$month+1 }) > post.createdAt >= datetime({year:$year, month:$month })
with *, post.createdAt.week as week
with week, u, count(*) as total, collect(DISTINCT category.name) AS categories
ORDER BY total DESC
WITH u, collect(["Week " + week, total]) as weekly, categories
RETURN DISTINCT u.name AS user, [(u)<-[:DISCOURSE_ACCOUNT]-(user) WHERE exists(user.auth0_key) | user.email][0] AS email, weekly, categories
"""

def ninja_activity(request, context):
    now = datetime.datetime.now()
    with db_driver.session() as session:
        params = {"year": now.year, "month": now.month }
        result = session.run(ninajs_discourse_query, params)

        discourse_header = result.keys()
        discourse_rows = [row.values() for row in result]

        params = {"now": now.strftime("%Y-%m")}
        result = session.run(ninjas_so_query, params)

        so_header = result.keys()
        so_rows = [row.values() for row in result]

        t = Template("""\
            <html>
            <head>
                <style>
                #customers {
                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                width: 100%;
                }

                #customers td, #customers th {
                border: 1px solid #ddd;
                padding: 8px;
                }

                #customers tr:nth-child(even){background-color: #f2f2f2;}

                #customers tr:hover {background-color: #ddd;}

                #customers th {
                padding-top: 12px;
                padding-bottom: 12px;
                text-align: left;
                background-color: #4291d6;
                color: white;
                }
                </style>
            </head>
            <body>
            <p>
                Hi Karin,
            </p>
            <p>
                The Neo4j Ninjas have been busy. See below for the biggest contributors this month.
            </p>

            <p>
                Cheers, Greta the Graph Giraffe
            </p>

            <h2>Discourse Activity</h2>
            <table id="customers">
                <tr>
                {% for column in discourse_header %}
                    <th>{{column}}</th>
                {% endfor %}
                </tr>
                {% for row in discourse_rows %}
                    <tr>
                        {% for column in row %}
                            <td>{{ column }}</td>
                        {% endfor %}
                    </tr>
                {% endfor %}
            </table>

            <h2>StackOverflow Activity</h2>
            <table id="customers">
                <tr>
                {% for column in so_header %}
                    <th>{{column}}</th>
                {% endfor %}
                </tr>
                {% for row in so_rows %}
                    <tr>
                        {% for column in row %}
                            <td>{{ column }}</td>
                        {% endfor %}
                    </tr>
                {% endfor %}
            </table>
            </body>
            </html>""")
        message = t.render(
            discourse_rows = discourse_rows,
            discourse_header=discourse_header,
            so_header = so_header,
            so_rows = so_rows
            )

        client = boto3.client('ses')
        response = client.send_email(
            Source="mark.needham@neo4j.com",
            Destination={"ToAddresses": ["karinwolok1@gmail.com", "m.h.needham@gmail.com"]},
            # Destination={"ToAddresses": ["m.h.needham@gmail.com"]},
            Message={
                "Body": {
                    "Html": {
                        "Data": message
                    }
                },
                "Subject": {
                    "Data": f"Ninja Activity on {now.strftime('%d %b %Y at %H:%M')}"
                }
            })
        print(response)
