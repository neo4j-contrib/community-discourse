import timeago
import datetime

def ninja_acceptance_message(name, username):
    return f"""
Dear {name or username}, 

Congratulations on being added to the [Neo4j Ninja Group](https://community.neo4j.com/g/ninja)!

Every month, we will be hosting 1 type of session. This could be an advanced-level webinar, a training, product demo, exclusive AMA (ask-me-anything), or feedback session with senior members of the Neo4j Product and Engineering teams. 

**Only participating Ninjas for the prior month will be granted access to that session**

## How to qualify:

Answer 1 question per week for 4 weeks within a given month. 
Technical help questions is where our community members need the most help, but we will also calculate your contributions through your replies, comments, and feedback in projects, content, and introduce-yourself categories. We also calculate Stack Overflow contributions. 

## Tips to get you started: 

* View unanswered questions using these two links: [Neo4j community site](http://community.neo4j.com/search?q=status%3Anoreplies%20after%3A14%20-tags%3Aknowledge-base%20in%3Aunseen%20order%3Alatest) or [Stack Overflow](https://stackoverflow.com/questions/tagged/neo4j?sort=Newest&filters=NoAnswers). You can also browse and track categories if you prefer. 
* Track your Ninja progress: In the [Neo4j Ninjas Leaderboard](http://ninjas.neo4j.com.s3-website-us-east-1.amazonaws.com/)
* If you haven't yet, [introduce-yourself](https://community.neo4j.com/c/general/introduce-yourself) to the community. This into ends up populating into your profile bio. 
* Update your community site profile with your Stack Overflow userID to ensure those contributions are calculated.
* Join us on Slack. We've created a private #Neo4jNinjas [slack channel](http://neo4j.com/slack) if you want to connect with other Ninjas or need guidance answering specific challenging questions. It's a private channel, so if you'd like access, ping Karin on the [Neo4j users slack](http://neo4j.com/slack) (@karinwolok) and she can add you. :)

And if you have any questions feel free to message [Karin Wolok](https://community.neo4j.com/u/karin.wolok/summary).

Happy ninja'ing,
Greta the Graph Giraffe"""


def ninja_rejection_owner_message(name, username):
    return f"""
Dear Mother of Ninjas, 

[{name or username}](https://community.neo4j.com/u/{username}/summary) requested to join the [Neo4j Ninja Group](https://community.neo4j.com/g/ninja) but hasn't  yet passed the Neo4j Certification exam. 

I'll let you handle it from here.

Happy ninja'ing,
Greta the Graph Giraffe"""


def ninja_approval_owner_message(name, username):
    return f"""
Dear Mother of Ninjas, 

[{name or username}](https://community.neo4j.com/u/{username}/summary) has been added to the [Neo4j Ninja Group](https://community.neo4j.com/g/ninja) as they have passed the Neo4j Certification exam. 

Happy ninja'ing,
Greta the Graph Giraffe"""


def ninja_questions(name, username, recommendations):
    reco = "\n".join([f"* [{r['title']}]({r['link']}) (created {timeago.format(r['createdAt'].to_native(), datetime.datetime.now())})" for r in recommendations])
    return f"""
Dear {name or username}, 

It's time to answer some community site questions to make sure you keep getting your Ninja rewards.
Below are some questions that I think you can help out with:

{reco}

Happy ninja'ing,
Greta the Graph Giraffe"""
