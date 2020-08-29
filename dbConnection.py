"""
Created by Akshat Ahuja [akshat.ahuja@sap.com(i516378)]
Used for connecting to HanaDB
"""

# Library for connecting to hana (https://pypi.org/project/hdbcli/)
from hdbcli import dbapi
# Access and use and use CF env in a better way (https://pypi.org/project/cfenv/)
from cfenv import AppEnv
# Import OS libraries to get enviournment(sued to get VCAP from Enviournment variables created by CF and binding applications)
import os

# this function is used to connect to Hana DB
def establish_connection():
# Checking if VCAP as env variable exists
    if os.getenv('VCAP_APPLICATION'):
        # Check https://pypi.org/project/cfenv/ fro more info
        env = AppEnv()
        # getting env with label hanatrial (check Enviornment varibales inside app)
        env_hana = env.get_service(label='hanatrial')
        # If Service is not binded
        if env_hana is None:
            res = {
                'Success':False,
                'Error':"Hana Service not binded, please check the manifest and make sure you have a running Hana service binded to the application"
            }
            return res
        # Getting credentails from VCAP json from Enviournment varibale of container (ENV is written and as service is binded, it gets the required creds)
        hana_cred = env_hana.credentials
        # Connecting to hanaDB, using env variables
        conn = dbapi.connect(
            address=hana_cred['host'],
            port=hana_cred['port'],
            encrypt="true",
            sslValidateCertificate= 'false',
            user=hana_cred['user'],
            password=hana_cred['password'],
            schema = hana_cred['schema']
        )
        
        ''' 
        Alteratively, if required, you can pass the values directly like:
            conn = dbapi.connect(
                address="HOST",
                port="23803",
                encrypt="true",
                sslValidateCertificate= 'false',
                user="TEST_USER",
                password="TEST_PASSWORD",
                schema= "TEST_SCHEMA"
            )
        '''
        cursor = conn.cursor()
        return cursor
    else:
        return False


# This method is used to execute queries directly on the db
def execute_sql(query):
    sql = query
    cursor = establish_connection()
    cursor.execute(sql)
    return cursor