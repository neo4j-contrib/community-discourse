import datetime
import json
import logging
import re

import boto3
import feedparser
import flask
import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from requests_toolbelt import MultipartEncoder
from retrying import retry

import util.ninja as n
import util.queries as q

ASSIGN_BADGES_TOPIC = "Discourse-Badges"
STORE_BADGES_TOPIC = "Store-Discourse-Badges"

ASSIGN_GROUPS_TOPIC = "Discourse-Groups"
STORE_GROUPS_TOPIC = "Store-Discourse-Groups"

CERTIFICATION_BADGE_ID = "103"
CERTIFICATION_GROUP_ID = "41"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def construct_topic_arn(context, topic):
    context_parts = context.invoked_function_arn.split(':')
    region = context_parts[3]
    account_id = context_parts[4]
    return f"arn:aws:sns:{region}:{account_id}:{topic}"


def get_ssm_param(key):
    resp = ssmc.get_parameter(
        Name=key,
        WithDecryption=True
    )
    return resp['Parameter']['Value']


ssmc = boto3.client('ssm', region_name="us-east-1")
app = flask.Flask('feedback form')
discourse_blog_category_id = 122

host_port = get_ssm_param('com.neo4j.graphacademy.dbhostport')
user = get_ssm_param('com.neo4j.graphacademy.dbuser')
password = get_ssm_param('com.neo4j.graphacademy.dbpassword')

db_driver = GraphDatabase.driver(f"neo4j://{host_port}", auth=(user, password), max_retry_time=15)

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


def import_posts_topics(request, context):
    headers = request["headers"]

    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    print(f"Received {event_type}: {event}")

    body = request["body"]
    json_payload = json.loads(body)

    if event_type == "topic" and json_payload["topic"]["archetype"] == "regular":
        with db_driver.session() as session:
            result = session.run(q.import_topic_query, {"params": json_payload})
            print(result.summary().counters)

    if event_type == "post" and json_payload["post"]["topic_archetype"] == "regular":
        with db_driver.session() as session:
            result = session.run(q.import_post_query, {"params": json_payload})
            print(result.summary().counters)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


kudos_message = """
Thanks for submitting!

I've added a tag that allows your blog to be displayed on the community home page!
"""


@retry(stop_max_attempt_number=5, wait_random_max=1000)
def get_community_content_active(params):
    with db_driver.session() as session:
        result = session.run(q.community_content_active_query, params)
        print(result.summary().counters)
        row = result.peek()
        content_already_approved = row.get("topic").get("approved") if row.get(
            "topic") else False
        return content_already_approved


@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_community_content(params):
    with db_driver.session() as session:
        result = session.run(q.community_content_query, params)
        print(result.summary().counters)
        return True


@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_user_events(params):
    with db_driver.session() as session:
        result = session.run(q.user_events_query, params)
        print(result.summary().counters)
        return True


@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_import_twin4j(params):
    with db_driver.session() as session:
        result = session.run(q.import_twin4j_query, params)
        print(result.summary().counters)
        return True


@retry(stop_max_attempt_number=5, wait_random_max=1000)
def set_update_topics(params):
    with db_driver.session() as session:
        result = session.run(q.update_topics_query, params)
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
            "topic_id": str(json_payload["topic"]["id"]),
            "raw": kudos_message
        }

        m = MultipartEncoder(fields=payload)
        r = requests.post(uri, data=m,
                          headers={'Content-Type': m.content_type,
                                   'Api-Key': discourse_api_key,
                                   'Api-Username': discourse_api_user})
        print(r)

    return {"statusCode": 200, "body": "Got the event", "headers": {}}


def user_events(request, context):
    headers = request["headers"]
    event_type = headers["X-Discourse-Event-Type"]
    event = headers["X-Discourse-Event"]
    logger.info(f"Received {event_type}: {event}")

    if event_type == "user" and event == "user_created":
        logger.info("User created so we'll update everything when they login")
        return {"statusCode": 200, "body": "It was just a user creation event", "headers": {}}
    elif event_type == "user" and event == "user_destroyed":
        logger.info("User destroyed so no longer need to care about user")
        return {"statusCode": 200, "body": "It was just a user destruction event -- no action as dont know reason", "headers": {}}

    body = request["body"]
    json_payload = json.loads(body)
    logger.info(f"json: {json_payload}")

    set_user_events({"params": json_payload})

    sns = boto3.client('sns')
    sns.publish(TopicArn=(construct_topic_arn(context, ASSIGN_BADGES_TOPIC)),
                Message=json.dumps({
                    "externalId": json_payload["user"]["external_id"],
                    "userName": json_payload["user"]["username"],
                    "discourseId": json_payload["user"]["id"],
                    "badgeId": CERTIFICATION_BADGE_ID
                }))

    sns.publish(TopicArn=(construct_topic_arn(context, ASSIGN_GROUPS_TOPIC)),
                Message=json.dumps({
                    "externalId": json_payload["user"]["external_id"],
                    "userName": json_payload["user"]["username"],
                    "discourseId": json_payload["user"]["id"],
                    "badgeId": CERTIFICATION_GROUP_ID
                }))

    return {"statusCode": 200, "body": "Updated user", "headers": {}}


def edu_discourse_users_query(tx):
    query = """
    MATCH (edu:EduApplication)-[r:SUBMITTED_APPLICATION]-(user:User)-[r2:DISCOURSE_ACCOUNT]-(discourse:DiscourseUser)
    WHERE edu.status = 'APPROVED'
    RETURN DISTINCT(discourse.name) as discourse_users
    """
    return tx.run(query)


def assign_edu_group(request, context):
    counter = 0
    group = ''
    uri = f"https://community.neo4j.com/groups/49/members.json"

    with db_driver.session() as session:
        result = session.read_transaction(edu_discourse_users_query)
        for record in result:
            if counter != 0:
                group += ','
            group += record['discourse_users']
            counter = counter + 1

    payload = {
        "usernames": group
    }
    logger.info(f"Adding {payload} users to Edu group")

    m = MultipartEncoder(fields=payload)
    r = requests.put(uri, data=m,
                     headers={'Content-Type': m.content_type, 'Api-Key': discourse_api_key,
                              'Api-Username': discourse_api_user})
    logger.info(f"Added {counter} users to Edu group")


def edu_discourse_invite_query(tx):
    query = """
    MATCH (edu:EduApplication)<-[r:SUBMITTED_APPLICATION]-(user:User)
    WHERE edu.status = 'APPROVED'
    AND NOT exists(user.discourseInviteSent)
    AND NOT exists((user)-[:DISCOURSE_ACCOUNT]->(:DiscourseUser))
    RETURN DISTINCT(user.email) as edu_email
    """
    return tx.run(query)


def edu_discourse_invited_update(tx, usersInvited):
    query = """
    UNWIND {usersInvited} as invitedUser
    MATCH (user:User {email: invitedUser})-[:SUBMITTED_APPLICATION]->(edu:EduApplication)
    WHERE edu.status = 'APPROVED'
    AND NOT exists(user.discourseInviteSent)
    AND NOT exists((user)-[:DISCOURSE_ACCOUNT]->(:DiscourseUser))
    WITH distinct(user)
     SET user.discourseInviteSent = datetime()
    RETURN count(user) as userCount
    """
    return tx.run(query, usersInvited=usersInvited)


def send_edu_discourse_invites(request, context):
    counter = 0
    usersInvited = []
    uri = f"https://community.neo4j.com/invites"

    with db_driver.session() as session:
        results = session.read_transaction(edu_discourse_invite_query)
        
        for record in results:
            payload = {
                "email": record['edu_email'],
                "group_names": "Neo4j-Educators",
                "custom_message": "The Neo4j Educator Program includes access to a private channel on our Community Site where you can ask questions, share resources, and learn from others. Join us!"
            }
            print(payload)
            usersInvited.append(record['edu_email'])
            counter += 1

        m = MultipartEncoder(fields=payload)
        r = requests.post(uri, data=m, headers={'Content-Type': m.content_type, 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
        print("Invited %d Edu users to Discourse" % (counter))

        updatedUsers = session.write_transaction(edu_discourse_invited_update, usersInvited)
        for record in updatedUsers:
            print("Updated %d users invited" % (record['userCount']))


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
            "bio_raw": bio,
        }

        m = MultipartEncoder(fields=payload)
        r = requests.put(uri, data=m, headers={'Content-Type': m.content_type, 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
        print(r)

        return {"statusCode": 200, "body": "Updated user bio", "headers": {}}
    else:
        return {"statusCode": 200, "body": "No action necessary", "headers": {}}


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
        result = session.run(q.store_medium_post_query, params)
        result.consume()


def post_medium_to_discourse(request, context):
    counter = 0
    with db_driver.session() as session:
      result = session.run(q.get_medium_posts_query, {})
      for record in result:
        dt = post_topic_to_discourse(discourse_blog_category_id, record['discourse_user'], record['title'], record['content'], record['date'])
        counter = counter + 1
        if 'id' in dt:
          params = {"discourseId": dt['id'], "mediumId": record['guid']}
          update_res = session.run(q.set_medium_post_posted_query, params)
          update_res.consume()
    return "Posted %d posts to discourse" % (counter)


def post_topic_to_discourse(category, username, title, body, published_date):
    post_data = {}
    post_data['title'] = title
    post_data['category'] = category
    post_data['raw'] = body
    post_data['created_at'] = published_date
    params = "api_key=%s&api_username=%s" % (discourse_root_api_key, username)
    r = requests.post("https://community.neo4j.com/posts.json?%s" % (params), json=post_data, headers={'Content-Type': 'application/json', 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
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


def update_categories_tx_fn(tx, params):
    tx.run(tx, params=params)


def update_categories(request, context):
    uri = f"https://community.neo4j.com/categories.json"

    r = requests.get(uri, headers={'Content-Type': 'application/json', 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
    response = r.json()
    print(len(response["category_list"]["categories"]))

    with db_driver.session() as session:
        categories = response["category_list"]["categories"]
        result = session.run(q.update_categories_subcategories_query, params=categories)
        print(result.summary().counters)

    r = requests.get("https://community.neo4j.com/site.json", headers={'Content-Type': 'application/json', 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
    response = r.json()
    categories = response["categories"]

    with db_driver.session() as session:
        result = session.run(q.update_categories_query, params=categories)
        print(result.summary().counters)


def assign_badges(event, context):
    sns = boto3.client('sns')
    topic_arn = construct_topic_arn(context, STORE_BADGES_TOPIC)

    for record in event["Records"]:
        message = json.loads(record["Sns"]["Message"])
        external_id = message["externalId"]
        badge_id = message["badgeId"]
        user_name = message["userName"]
        discourse_id = message["discourseId"]

        # Check if the user earnt the badge
        with db_driver.session() as session:
            is_certified = session.run(q.did_user_pass_query, {"externalId": external_id}).single()["certified"]

        logger.info(f"externalId: {external_id}, userName: {user_name}, discourseId: {discourse_id}, isCertified: {is_certified}")

        if is_certified:
            uri = f"https://community.neo4j.com/user_badges.json"

            payload = {
                "username": user_name,
                "badge_id": badge_id
            }

            m = MultipartEncoder(fields=payload)
            r = requests.post(uri, data=m, headers={'Content-Type': m.content_type, 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user})
            logger.info(f"user: {user_name}, discourseId: {discourse_id}, response:  {r} ->  {r.json()}")

            message = {"discourseId": discourse_id, "userName": user_name}
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def find_users_badges(event, context):
    topic_arn = construct_topic_arn(context, STORE_BADGES_TOPIC)

    sns = boto3.client('sns')
    with db_driver.session() as session:
        rows = session.run(q.users_badge_refresh_query)
        for row in rows:
            logger.info(f"row: {row}")
            username = row["userName"]
            discourse_id = row["discourseId"]

            message = {"discourseId": discourse_id, "userName": username}
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def store_badges_tx(tx, params):
    return tx.run(q.store_badges_query, params)


def store_badges(event, context):
    for record in event["Records"]:
        logger.info(f"Record: {record}")

        message = json.loads(record["Sns"]["Message"])
        discourse_id = message["discourseId"]
        username = message["userName"]

        r = requests.get(f"https://community.neo4j.com/user-badges/{username}.json")
        badges = r.json().get("badges")
        badges = badges if badges else []
        logger.info(f"user: {username}, discourseId: {discourse_id}, badges: {badges}")
        with db_driver.session() as session:
            params = {"id": discourse_id, "badges": badges}
            result = session.write_transaction(store_badges_tx, params)
            logger.info(f"params: {params}, result: {result.summary().counters}")


def missing_badges(event, context):
    sns = boto3.client('sns')
    topic_arn = construct_topic_arn(context, ASSIGN_BADGES_TOPIC)

    with db_driver.session() as session:
        rows = session.run(q.users_who_passed_query_but_dont_have_badge)
        for row in rows:
            logger.info(f"row: {row}")
            message = {
                "externalId": row["externalId"],
                "userName": row["userName"],
                "discourseId": row["discourseId"],
                "badgeId": CERTIFICATION_BADGE_ID
            }
            logger.info(f"message: {message}")
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def find_users_groups(event, context):
    topic_arn = construct_topic_arn(context, STORE_GROUPS_TOPIC)

    sns = boto3.client('sns')
    with db_driver.session() as session:
        rows = session.run(q.users_groups_refresh_query)
        for row in rows:
            logger.info(f"row: {row}")
            username = row["userName"]
            discourse_id = row["discourseId"]

            message = {"discourseId": discourse_id, "userName": username}
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def store_groups_tx(tx, params):
    return tx.run(q.store_groups_query, params)


def store_groups(event, context):
    for record in event["Records"]:
        message = json.loads(record["Sns"]["Message"])
        discourse_id = message["discourseId"]
        username = message["userName"]
        logger.info(f"username: {username}, discourseId: {discourse_id}")

        headers = {'Content-Type': 'application/json',
                   'Api-Key': discourse_api_key,
                   'Api-Username': discourse_api_user}
        r = requests.get(f"https://community.neo4j.com/users/{username}.json", headers=headers)
        groups = r.json().get("user", {}).get("groups", [])
        groups = groups if groups else []
        logger.info(f"user: {username}, discourseId: {discourse_id}, groups: {groups}")
        with db_driver.session() as session:
            params = {"id": discourse_id, "groups": groups}
            result = session.write_transaction(store_groups_tx, params)
            logger.info(f"params: {params}, result: {result.summary().counters}")


def missing_groups(event, context):
    sns = boto3.client('sns')
    topic_arn = construct_topic_arn(context, ASSIGN_GROUPS_TOPIC)

    with db_driver.session() as session:
        rows = session.run(q.users_who_passed_query_but_dont_have_group)
        for row in rows:
            logger.info(f"row: {row}")
            message = {
                "externalId": row["externalId"],
                "userName": row["userName"],
                "discourseId": row["discourseId"],
                "groupId": CERTIFICATION_GROUP_ID
            }
            logger.info(f"message: {message}")
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def assign_groups(event, context):
    sns = boto3.client('sns')
    topic_arn = construct_topic_arn(context, STORE_GROUPS_TOPIC)

    for record in event["Records"]:
        message = json.loads(record["Sns"]["Message"])
        external_id = message["externalId"]
        group_id = message["groupId"]
        user_name = message["userName"]
        discourse_id = message["discourseId"]

        with db_driver.session() as session:
            is_certified = session.run(q.did_user_pass_query, {"externalId": external_id}).single()["certified"]

        logger.info(f"externalId: {external_id}, userName: {user_name}, discourseId: {discourse_id}, isCertified: {is_certified}")

        if is_certified:
            uri = f"https://community.neo4j.com/groups/{group_id}/members.json"
            payload = { "usernames": user_name}
            logger.info(f"Adding {payload} users to group {group_id}")

            m = MultipartEncoder(fields=payload)
            headers = {'Content-Type': m.content_type, 'Api-Key': discourse_api_key,'Api-Username': discourse_api_user}
            r = requests.put(uri, data=m, headers=headers)
            logger.info(f"user: {user_name}, discourseId: {discourse_id}, groups: {r} -> {r.json()}")

            message = {"discourseId": discourse_id, "userName": user_name}
            sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def poll_ninja_requests(event, context):
    headers = {'Content-Type': 'application/json', 'Api-Key': discourse_api_key,'Api-Username': discourse_api_user}
    group_name = "ninja"
    group_id = 50
    mother_of_ninjas = "mark.needham,karin.wolok"

    logger.info("Polling for new requests to join the Ninja Group")
    requesters_uri = f"https://community.neo4j.com/g/{group_name}/members.json?requesters=true"
    response = requests.get(requesters_uri, headers=headers).json()
    members = [{k: v
                for k, v in m.items() if k in ["id", "name", "username"]}
               for m in response["members"]]
    logger.info(f"Found members: {members}")

    handle_request_uri = f"https://community.neo4j.com/groups/{group_id}/handle_membership_request.json"
    for member in members:
        name = member.get('name')
        username = member.get('username')
        with db_driver.session() as session:
            is_certified = session.run(q.did_discourse_user_pass_query, {"discourseUserId": member["id"]}).single()["certified"]
            if is_certified:
                logger.info(f"User is certified: {member}")
                payload = {"accept": True, "user_id": member["id"]}
                add_to_group_response = requests.put(handle_request_uri,
                                                     headers=headers,
                                                     data=json.dumps(payload))
                logger.info(f"Request processed: {add_to_group_response.json()}")

                send_private_message(headers, {
                    "raw": n.ninja_acceptance_message(name, username),
                    "target_recipients": username,
                    "title": f"Neo4j Ninja Group Request Accepted"
                })
                send_private_message(headers, {
                    "raw": n.ninja_approval_owner_message(name, username),
                    "target_recipients": mother_of_ninjas,
                    "title": f"Neo4j Ninja Approved: {name or username}"
                })
            else:
                logger.info(f"User is not certified: {member}")
                payload = {"user_id": member["id"]}
                add_to_group_response = requests.put(handle_request_uri,
                                                     headers=headers,
                                                     data=json.dumps(payload))
                logger.info(f"Request processed: {add_to_group_response.json()}")

                send_private_message(headers, {
                    "raw": n.ninja_rejection_owner_message(name, username),
                    "target_recipients": mother_of_ninjas,
                    "title": f"Neo4j Ninja Rejected: {name or username}"
                })


def send_private_message(headers, payload):
    payload["archetype"] = "private_message"
    response = requests.post(f"https://community.neo4j.com/posts.json",
                             headers=headers,
                             data=json.dumps(payload))
    logger.info(f"response: {response.json()}")


def poll_ninja_recommended_questions(event, context):
    headers = {'Content-Type': 'application/json', 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user}

    now = datetime.datetime.now()
    week_starting = (now - datetime.timedelta(days=(now.weekday() + 1) % 7)).date()

    with db_driver.session() as session:
        params = {"weekStarting": week_starting, "limit": 1}
        logger.info(f"Params: {params}")
        result = session.read_transaction(lambda tx: tx.run(q.find_ninja_to_process, params).data())
        logger.info(f"User to process: {result}")

        for row in result:
            name = row["u"].get('screenName')
            username = row["u"].get('name')
            user_id = row["u"].get('id')

            params = {"userId": user_id}
            recommendations = session.read_transaction(
                lambda tx: tx.run(q.find_topics_to_recommend, params).data())[:3]

            params = {
                "userId": user_id,
                "weekStarting": week_starting,
                "topics": [r["topicId"] for r in recommendations]
            }
            response = session.write_transaction(lambda tx: tx.run(q.save_recommendations_query, params).summary().counters)
            logger.info(f"Stored recommendations for {params}: {response}")

            has_recommendations = len(recommendations) > 0
            logger.info(f"User: {username}, Has recommendations? {has_recommendations}")
            if has_recommendations:
                send_private_message(headers, {
                    "raw": n.ninja_questions(name, username, recommendations),
                    "target_recipients": username,
                    "title": f"Neo4j Ninja questions to answer: {datetime.datetime.now().strftime('%d %B %Y')}"
                })


def duplicate_users_query(tx):
    query = """
    MATCH (me:DiscourseUser)
    WITH me.name AS name, count(*) AS count, collect(me.id) AS ids
    WHERE count > 1
    UNWIND ids AS id
    RETURN id
    LIMIT 5
    """
    return tx.run(query)


def deprecate_missing_user(tx, user_id):
    query = """
    MATCH (u:DiscourseUser {id: $userId})
    REMOVE u:DiscourseUser
    SET u:MissingDiscourseUser
    """
    return tx.run(query, userId=user_id)


def update_user_screenname(tx, user_id, json):
    query = """
    MATCH (u:DiscourseUser {id: $userId})
    SET u.name = $json.username
    """
    return tx.run(query, userId=user_id, json=json)


def clean_up_discourse_users(event, context):
    headers = {'Content-Type': 'application/json', 'Api-Key': discourse_api_key, 'Api-Username': discourse_api_user}

    with db_driver.session() as session:
        result = session.read_transaction(duplicate_users_query)
        for row in result:
            id = row["id"]
            r = requests.get(f"https://community.neo4j.com/admin/users/{id}.json", headers=headers)
            status_code = r.status_code
            json = r.json()
            print(row, status_code, json)
            if status_code == 404:
                result = session.write_transaction(deprecate_missing_user, id)
                logger.info(f"result: {result.summary().counters}")
            if status_code == 200:
                result = session.write_transaction(update_user_screenname, id, json)
                logger.info(f"result: {result.summary().counters}")