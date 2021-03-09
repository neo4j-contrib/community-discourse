import calendar
import datetime
import json
import logging

import boto3
from dateutil import parser
from neo4j import GraphDatabase

import util.queries as q

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ssmc = boto3.client('ssm')


def get_ssm_param(key):
    resp = ssmc.get_parameter(
        Name=key,
        WithDecryption=True
    )
    return resp['Parameter']['Value']


host_port = get_ssm_param('com.neo4j.graphacademy.dbhostport')
user = get_ssm_param('com.neo4j.graphacademy.dbuser')
password = get_ssm_param('com.neo4j.graphacademy.dbpassword')

db_driver = GraphDatabase.driver(f"neo4j://{host_port}", auth=(user, password))


def workdays(d, end, excluded=(6, 7)):
    days = []
    while d.date() <= end.date():
        if d.isoweekday() not in excluded:
            days.append(d)
        d += datetime.timedelta(days=1)
    return days


def all_ninjas(event, context):
    print(event)
    qs = event.get("multiValueQueryStringParameters")
    if qs and qs.get("date"):
        now = parser.parse(qs["date"][0]).replace(day=1)
    else:
        now = datetime.datetime.now().replace(day=1)

    start = now - datetime.timedelta(days=(now.weekday() + 1) % 7)
    end = now.replace(day=calendar.monthrange(now.year, now.month)[1])
    end = end - datetime.timedelta(days=(end.weekday() + 1) % 7)
    logger.info(f"Retrieving Ninja activities for {now}. Start: {start}, End: {end}")

    weeks = [{"start": day, "end": day + datetime.timedelta(days=6)} for day in
             workdays(start, end, [1, 2, 3, 4, 5, 6])]

    with db_driver.session() as session:
        params = {"year": now.year, "month": now.month}
        print("params", params)
        result = session.run(q.ninjas_api_discourse_query, params)

        discourse_rows = [row.data() for row in result]

        params = {"now": now.strftime("%Y-%m")}
        result = session.run(q.ninjas_api_so_query, params)

        so_rows = [row.data() for row in result]

    return {"statusCode": 200, "body": json.dumps({
        "discourse": discourse_rows,
        "so": so_rows,
        "weeks": [{"start": week["start"].strftime("%Y-%m-%d"),
                   "end": week["end"].strftime("%Y-%m-%d")}
                  for week in weeks]
    }), "headers": {
        "Content-Type": "application/json",
        'Access-Control-Allow-Origin': '*'
    }}
