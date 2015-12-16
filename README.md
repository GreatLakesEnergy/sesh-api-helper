# sesh-api-helper

sesh-api-helper is a simple flask application that exposes endpoints to accept data from the emonhub.  accepts the same data format and thus does only require minimal change on the hub side of things.

## Installation
  
### Dependencies

python dependencies are defined in the requirements.txt file.

1) install the dependencies

    pip install -r reqirements.txt

2) run the server

    python api.py

## Configuration

You can optionally set the path to a config file in a FLASK_SETTINGS environment variable. 
Have a look at the config.cfg.example file

    export FLASK_SETTINGS=/path/to/config.cfg
    export PORT=5000
    python api.py
