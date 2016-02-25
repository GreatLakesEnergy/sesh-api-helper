# sesh-api-helper

sesh-api-helper is a simple flask application that exposes endpoints to accept data from the emonhub. It accepts the same data format and thus does only require minimal change on the hub side of things.
sesh-api-helper writes data to a MySQL database and to InfluxDB. 
The MySQL database schema is defined by the SESH Dashboard. InfluxDB database and schema must be defined seperately.

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
    export FLASK_PORT=5000
    export FLASK_HOST=0.0.0.0
    python api.py


## Known issues:

* InfluxDB values must be type casted before inserting. (MySQL does that for us depending on the schema) Currently we are not felxible enough and simply try to convert anything to integers
* The HTTP interface should be seperated from type casting and writing the data. For example make databases plugable 
* If inserting to one of the databases fails the whole request fails
* Bulk inserts could be made more efficient with less writes to the databases
* The same mapping of parameters is applied to MySQL and InfluxDB. column == measurement
* We can make better use of InfluxDB tags. currently only the site_id is a tag
