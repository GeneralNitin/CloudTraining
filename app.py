"""
Created by Akshat Ahuja [akshat.ahuja@sap.com(i516378)]
This is a hello world applcation which connects to hanaDB on CF
This was used for Cloud Training by Batch 5 of Scholar Batch 2020 
Trainer: Shivam Pandey 
Co-trainer: Akshat Ahuja
"""

# Flask for serving APIs
from flask import Flask, request
# Import OS libraries to get enviournment(sued to get VCAP from Enviournment variables created by CF and binding applications)
import os
# Library for connecting to hana (https://pypi.org/project/hdbcli/)
from hdbcli import dbapi
# Importing SQL execute from dbConnection File
from dbConnection import execute_sql

# Global variables, you can change them
Table = "SCHOLARS"
Values = [
    "Akshat",
    "Shivam",
    "Nitin"
]

app = Flask(__name__)
# On Localhost,it uses port 9000 as stated while declaring the variable port to use as set by enviournmrnt variable in CF
port = int(os.getenv("PORT", 9000))

# Hello World end point, returns hello world as JSON
@app.route('/', methods=['GET'])
def hello_world():
    res = {
        'Success': True,
        'Msg': "Welcome to the program, and well, Hello World"
    }
    return res

# Get request for getting values inserted in the table
@app.route('/get', methods=['GET'])
def get_table():
    try:
        # initialisng the array to store names
        names = []
        # Executing the sql using dbConnection's execute SQL function
        cursor = execute_sql('SELECT * FROM '+Table)
        # Iterating over the rows
        if cursor:
            for row in cursor:
                names.append(str(row[0]))
            res = {
                'Success': True,
                'Inserted Names': names
            }
        else:
            res = {
                'Success': False,
                'Error': "Issue with setting Cursor, please check logs"
            }
    except dbapi.ProgrammingError as error:
        # Error Code 259 specifies that the table is deosn't exist
        if str(error)[1:4] == "259":
            res = {
                "Error": str(error),
                "Msg":"The table was never created please hit /create API endpoint tp create and populate the table"
            }
        else:
            res = {
                "Error": str(error),
            }
    return res

# Get Request to create table if it does not exist and then insert some values
@app.route("/create")
def create_table():
    try:
        cursor = execute_sql('CREATE TABLE '+Table+' (Name varchar(100))')
        if cursor:
            names = []
            for name in Values:
                cursor = execute_sql("INSERT INTO "+Table+" VALUES ('"+name+"')")
                names.append(name)
            res = {
                "Msg":"Created the table " + Table +  " and inserted values",
                "Values Insrted": names
            }
        else:
            res = {
                'Success': False,
                'Error': "Issue with setting Cursor, please check logs"
            }
    except dbapi.ProgrammingError as error:
        # Error Code 288 specifies that the table is duplicate, this condition drops it
        if str(error)[1:4] == "288":
            cursor = execute_sql('DROP Table '+Table)
            res = {
                "Error": str(error),
                "Msg":"As table was duplicate, dropping table, please hit the same API to create the table again and insert values"
            }
        else:
            res = {
                "Error": str(error),
            }
    return res


# Main Function - runs the flask app (Host should be 0.0.0.0 to run on CF, and PORT is provided by CF using enviournment variables)
# On Localhost,it uses port 9000 as stated while declaring the variable port
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)