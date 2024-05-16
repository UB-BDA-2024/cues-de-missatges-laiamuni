from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import json

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.cassandra_client import CassandraClient
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.sensors import models, schemas
from shared.timescale import Timescale


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(mongodb: MongoDBClient, sensor_id: int) -> Optional[models.Sensor]:
    sensor = mongodb.get_sensor({"id": sensor_id})
    return sensor

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    date = datetime.now()

    db_sensor = models.Sensor(name=sensor.name, joined_at=date)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    return db_sensor

def add_sensor_to_mongodb(mongodb_client: MongoDBClient, db_sensor: schemas.SensorCreate, id):
    mongo_projection = schemas.SensorMongoProjection(id=id, name=db_sensor.name, location={'type': 'Point',
                                                                                           'coordinates': [
                                                                                               db_sensor.longitude,
                                                                                               db_sensor.latitude]},
                                                     type=db_sensor.type, mac_address=db_sensor.mac_address,
                                                     description=db_sensor.description,
                                                     serie_number=db_sensor.serie_number,
                                                     firmware_version=db_sensor.firmware_version, model=db_sensor.model,
                                                     manufacturer=db_sensor.manufacturer)
    mongodb_client.getDatabase()
    mongoInsert = mongo_projection.dict()
    mongodb_client.getCollection().insert_one(mongoInsert)
    return mongo_projection.dict()

def getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient, elastic: ElasticsearchClient, cassandra: CassandraClient) -> Optional[models.Sensor]:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    sensor2 = {
        "id": db_sensor.id,
        "name": sensor.name,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "type":sensor.type,
        "mac_address":sensor.mac_address,
        "manufacturer":sensor.manufacturer,
        "serie_number":sensor.serie_number,
        "model":sensor.model,
        "firmware_version":sensor.firmware_version,
        "description":sensor.description
    }

    return_value = mongodb.set_sensor(sensor2)

    document_to_index = {
         "name": sensor2["name"],
         "type": sensor2["type"],
         "description": sensor2["description"]
     }
    
    elastic.index_document(index_name="sensors", document=document_to_index)
    
    query = f"INSERT INTO sensor.quantity(sensor_id, sensor_type) VALUES ({db_sensor.id}, '{sensor.type}');"
    cassandra.execute(query)

    sensor_info = mongodb.get_sensor({"id":db_sensor.id})
    return sensor_info

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData, ts: Timescale, cassandra: CassandraClient) -> schemas.Sensor:
    db_sensordata = {
        "velocity": data.velocity,
        "temperature": data.temperature,
        "humidity": data.humidity,
        "battery_level": data.battery_level,
        "last_seen": data.last_seen
    }
    
    ts_data = {key: value if value is not None else 'NULL' for key, value in db_sensordata.items()}
    ts_data['last_seen'] = f"'{ts_data['last_seen']}'"



    query = f"""INSERT INTO sensor_data (sensor_id, temperature, humidity, velocity, battery_level, last_seen) 
             VALUES ({sensor_id}, {ts_data['temperature']}, {ts_data['humidity']}, {ts_data['velocity']}, {ts_data['battery_level']}, {ts_data['last_seen']})
             ON CONFLICT (sensor_id, last_seen) DO UPDATE SET temperature = EXCLUDED.temperature, humidity=EXCLUDED.humidity, velocity=EXCLUDED.velocity, battery_level=EXCLUDED.battery_level;"""

    ts.execute(query)
    ts.execute("commit")

    if data.temperature is not None:
        cassandra.execute("INSERT INTO sensor.temp_values (sensor_id, temp) VALUES (" + str(sensor_id) + ", " + str(data.temperature) + ");")
    
    cassandra.execute("INSERT INTO sensor.low_bat (sensor_id, battery) VALUES (" + str(sensor_id) + ", " + str(data.battery_level) + ");")

    redis.set(sensor_id, db_sensordata)


    return data

def get_data(redis: RedisClient, sensor_id: int,  db: Session, ts: Timescale, from_: str, to: str, bucket: str) -> schemas.Sensor:
    if from_ is not None and to is not None and bucket is not None:
        query = f"""
            SELECT 
                sensor_id,
                time_bucket('1 {bucket}', last_seen) AS {bucket},
                AVG(velocity) AS velocity,
                AVG(temperature) AS temperature,
                AVG(humidity) AS humidity
            FROM sensor_data
            WHERE sensor_id = {sensor_id} AND last_seen >= '{from_}' AND last_seen <= '{to}'
            GROUP BY sensor_id, {bucket};
        """
        
        ts.execute(query)
        result = ts.cursor.fetchall()

        return result

    else:
        db_sensordata = redis.get(sensor_id)

        if db_sensordata is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        
        db_sensordata["id"] = sensor_id
        db_sensordata["name"] = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first().name
    
        return db_sensordata

def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: RedisClient, ts: Timescale):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    query_delete = {"id": sensor_id}
    mongodb.delete_sensor(query_delete)

    redis.delete(sensor_id)

    query = "DELETE FROM sensor_data WHERE sensor_id == " + str(sensor_id)
    ts.execute(query)
    return db_sensor

def get_temperature_values(mongodb: MongoDBClient, cassandra: CassandraClient):
    db_sensor = cassandra.execute("SELECT sensor_id, MAX(temp) as max_temp, MIN(temp) as min_temp, AVG(temp) as avg_temp FROM sensor.temp_values GROUP BY sensor_id;")
    
    sensors = []
    for row in db_sensor:
        sensor_id = row.sensor_id
        data_sensor = get_sensor(mongodb, sensor_id)
        data_sensor["values"] = {"max_temperature": row.max_temp, "min_temperature": row.min_temp, "average_temperature": row.avg_temp}
        sensors.append(data_sensor)
    
    return {"sensors": sensors}

def get_sensors_quantity(db: Session, cassandra_client: CassandraClient):
    db_sensor = cassandra_client.execute("SELECT sensor_type, COUNT(*) FROM sensor.quantity GROUP BY sensor_type;")
    
    sensors = list()
    for row in db_sensor:
        type_dict = {"type":row.sensor_type, "quantity": row.count}
        sensors.append(type_dict)
    
    return {"sensors": sensors}

def get_low_battery_sensors(mongodb: MongoDBClient, cassandra: CassandraClient):
    db_sensor = cassandra.execute("SELECT * FROM sensor.low_bat WHERE battery <= 0.2 ALLOW FILTERING;")

    sensors = list()
    for row in db_sensor:
        sensor_id = row.sensor_id
        data_sensor = get_sensor(mongodb, sensor_id)
        data_sensor.update({"battery_level": round(row.battery, 2)})
        sensors.append(data_sensor)

    return {"sensors": sensors}

def search_sensors(db: Session,mongodb: MongoDBClient, elastic: ElasticsearchClient, query: str, size: int, search_type: str):
    search = list()
    query2 = json.loads(query)

    if search_type == "similar":
        search_type = "fuzzy"

    query_search = {
        "query":{
            search_type: query2
        }
    }

    results = elastic.search(index_name="sensors", query=query_search)

    for hit in results['hits']['hits']:
        name = hit["_source"]["name"]
        sensor = mongodb.get_sensor({"name": name})

        search.append(sensor)

        if len(search) == size:
            break
    return search

def get_sensor2(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_data2(redis: RedisClient, sensor_id: int, data: Session) -> dict:
    db_sensor = redis.get(sensor_id)

    if db_sensor:
        sensor_data = db_sensor
        sensor_data["id"] = sensor_id
        sensor_data["name"] = data.query(models.Sensor).filter(models.Sensor.id == sensor_id).first().name

        return sensor_data

def get_sensor_near(mongodb: MongoDBClient, redis: RedisClient, latitude: float, longitude: float, radius: float, db:Session)->List:
    query = {"latitude":{"$gte":latitude - radius,"$lte":latitude + radius}, "longitude":{"$gte": longitude-radius, "$lte":longitude+radius}}
    documents = list(mongodb.getDocuments(query))

    for i in documents:
        db_sensor = get_sensor2(db=db, sensor_id=i['id'])

        db_sensor=get_data(redis=redis,sensor_id=db_sensor.id,data=db)

        i['velocity']=db_sensor['velocity']
        i['temperature']=db_sensor['temperature']
        i['humidity']=db_sensor['humidity']
        i['battery_level']=db_sensor['battery_level']
        i['last_seen']=db_sensor['last_seen']
    
    return documents