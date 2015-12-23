import sqlalchemy

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime

# TODO: use config from the config file
engine = create_engine('sqlite:///test.db')

metadata = MetaData()

user = Table('test_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('battery_voltage', Integer),
    Column('power', String(16)),
    Column('time', String())
    #Column('time', DateTime())
)

metadata.create_all(engine)
