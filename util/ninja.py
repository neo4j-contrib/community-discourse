def ninja_acceptance_message(name, username):
    return f"""
Dear {name or username}, 

Congratulations on being added to the [Neo4j Ninja Group](https://community.neo4j.com/g/ninja)!

You can track your progress on the [Neo4j Ninja Dashboard](http://ninjas.neo4j.com.s3-website-us-east-1.amazonaws.com/) and if you have any questions feel free to message [Karin Wolok](https://community.neo4j.com/u/karin.wolok/summary).

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