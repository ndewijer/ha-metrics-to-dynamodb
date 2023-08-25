from decimal import Decimal, DecimalException
import mariadb
import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

metadata_ids = []

if __name__ == '__main__':

    # DEFINE Variables
    awsregion = "eu-west-1"
    ssm_parameter_timestamp = "electricity-usage-timestamp"
    dynamodb_database = "ddbtable-electricity"

    mariadb_server = ""
    mariadb_user = ""
    mariadb_passwd = ""
    mariadb_db = "homeassistant"

    # DEFINE Sensors to poll
    sensors = [
        "sensor.electricity_meter_energy_consumption_tarif_1",
        "sensor.electricity_meter_energy_consumption_tarif_2",
        "sensor.electricity_meter_energy_production_tarif_1",
        "sensor.electricity_meter_energy_production_tarif_2",
        "sensor.solaredge_ac_power",
        "sensor.solaredge_ac_energy_kwh"
    ]

    try:
        # Connect AWS DynamoDB and SSM
        session = boto3.Session(profile_name='default')
        ddb = session.resource('dynamodb', region_name=awsregion)
        table = ddb.Table(dynamodb_database)
        ssm = session.client('ssm', region_name=awsregion)
    except ClientError as err:
        logger.error(
            "Couldn't connect to DynamoDB or SSM. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    # get ssm parameter for dynamodb timestamp
    try:
        res = ssm.get_parameter(
            Name=ssm_parameter_timestamp, WithDecryption=False)
        timestamp = res['Parameter']['Value']
    except ClientError as err:
        logger.error(
            "Couldn't get timestamp from SSM. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    # Connect to mariadb
    try:
        con = mariadb.connect(
            user=mariadb_user,
            password=mariadb_passwd,
            host=mariadb_server,
            port=3306,
            database=mariadb_db
        )
        cur = con.cursor()
    except mariadb.Error as err:
        logger.error(
            "Couldn't connect to mariadb. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    # Get metadata_ids
    for sensor in sensors:
        try:
            cur.execute(
                "SELECT metadata_id FROM states_meta WHERE entity_id = ?", (sensor,))
            metadata_id = cur.fetchone()[0]
            metadata_ids.append(metadata_id)
        except mariadb.Error as err:
            logger.error(
                "Couldn't get metadata_id from mariadb. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Using metadata ids, get all the sensor data.
    try:
        cur.execute("SELECT state, last_updated_ts, metadata_id FROM states where metadata_id in {} and "
                    "last_updated_ts > {}".format(str(tuple(metadata_ids)), timestamp))
        rows = cur.fetchall()
        logger.info("total records found: %s", len(rows))
    except mariadb.Error as err:
        logger.error(
            "Couldn't get data from mariadb. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    # Batch writer allows for fast entries into DynamoDB
    with table.batch_writer() as batch:
        for idx, row in enumerate(rows):
            try:
                # In certain situations, the value of state will be unknown which decimal cannot handle.
                try:
                    state = Decimal(row[0])
                except (ValueError, DecimalException):
                    state = Decimal(0)
                    pass

                content = {
                    'metadata_id': row[2],
                    'state': state,
                    'last_updated_ts': str(row[1])
                }
                batch.put_item(Item=content)
            except ClientError as err:
                logger.error(
                    "Couldn't add entry with timestamp %s to table %s. Here's why: %s: %s",
                    str(row[1]), table.name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                try:
                    # write timestamp to ssm parameter to try again next time
                    ssm.put_parameter(Name=ssm_parameter_timestamp,
                                      Value=timestamp, Overwrite=True)
                except ClientError as err:
                    logger.error(
                        "Double error. Could not save up to date timestamp to SSM Parameter. Please be wary of "
                        "duplicates. Timestamp: %s. Error: %s: %s",
                        str(row[1]), err.response['Error']['Code'], err.response['Error']['Message'])
                raise

            # Each 25 records, write status to logger.
            if ((idx + 1) % 25 == 0) or ((idx + 1) == len(rows)):
                logger.info("Added %s entries to DynamoDB table. Latest timestamp: %s", str(
                    idx + 1), str(row[1]))
            timestamp = str(row[1])
    try:
        ssm.put_parameter(Name=ssm_parameter_timestamp,
                          Value=timestamp, Overwrite=True)
        logger.info(
            "Finished. Updated SSM Parameter with latest timestamp: %s", timestamp)
    except ClientError as err:
        logger.error(
            "Could not save up to date timestamp to SSM Parameter. Please be wary of "
            "duplicates. Timestamp: %s. Error: %s: %s",
            str(row[1]), err.response['Error']['Code'], err.response['Error']['Message'])
        raise
