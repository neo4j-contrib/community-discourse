# Community-Discorse Integrations

## Requirements:
* Python 3.8
* virtualenv (pip3 install virtualenv)
* node > 12.x & npm
* aws cli
* op (Optional: 1Password-cli)


## Setup:
    virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt
    npm install -g serverless 
    npm install

## Get .env.yml file (optionally with op)
    eval $(op signin neo_technology)
    # To get the document uuid op list documents | jq
    op get document iafvjavwmpmqygnbssthoqzi7q > env.yml
## Deploy
    sls deploy --aws-profile <aws-profile>



