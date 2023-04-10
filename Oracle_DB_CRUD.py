# -*- coding: utf-8 -*-

import oracledb
from datetime import datetime

class OracleSQL:
    """
    OracleSQL is a class for interacting with Oracle databases using Python.
    """
    def __init__(self, username, password, host, port, service):
        """
        Initializes OracleSQL class instance with the required connection details
        
        Args:
        username: str, username to login to Oracle Database
        password: str, password to login to Oracle Database
        host: str, host name of the Oracle Database server
        port: str, port number of the Oracle Database server
        service: str, name of the service to connect to
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service = service
        self.connection = None
    
    def connect(self):
        """
        Connects to Oracle Database with the details provided in the constructor using the oracledb module.
        """
        try:
            dsn_string = f"{self.host}:{self.port}/{self.service}"
            oracledb.init_oracle_client(lib_dir=r"C:\Users\susanth\Downloads\instantclient_19_17")
            self.connection = oracledb.connect(user=self.username, password=self.password, dsn=dsn_string)
            print("Connected to Oracle Database")
        except oracledb.DatabaseError as e:
            error, = e.args
            print("Error code:", error.code)
            print("Error message:", error.message)
    
    def close(self):
        """
        Closes the connection to Oracle Database.
        """
        try:
            self.connection.close()
            print("Connection closed")
        except oracledb.DatabaseError as e:
            error, = e.args
            print("Error code:", error.code)
            print("Error message:", error.message)
    
    def check_table_exists(self, table_name):
        """
        Checks if a table exists in the database.
        
        Args:
        table_name: str, name of the table to check
        
        Returns:
        bool, True if the table exists, False otherwise.
        """
        cur = self.connection.cursor() 
        cur.execute(f"SELECT COUNT(*) FROM all_tables WHERE table_name = '{table_name.upper()}'")
        if cur.fetchone()[0] != 0:
            return True
        cur.close()
            
    def create_table(self, table_name, columns_dict):
        """
        Creates a table in the database with the given name and columns.
        
        Args:
        table_name: str, name of the table to create
        columns_dict: dict, a dictionary containing the names and datatypes of the columns.
        """
        if not self.check_table_exists(table_name):
            try:
                    cur = self.connection.cursor()
                    sql_query = f"CREATE TABLE {table_name.upper()} ("
                    for column_name, column_type in columns_dict.items():
                        sql_query += f"{column_name} {column_type},"
                    sql_query = sql_query[:-1] + ")"
                    cur.execute(sql_query)
                    self.connection.commit()
                    print(f"Table {table_name.upper()} created successfully")
            except oracledb.DatabaseError as e:
                error, = e.args
                print("Error code:", error.code)
                print("Error message:", error.message)
            finally:
                cur.close()
        else:
            print(f"Table {table_name.upper()} already exists")
    
    def delete_table(self, table_name):
        """
        Delete a table from the database.

        Args:
        - table_name: str, name of the table to be deleted

        Returns:
        - None
        """
        if self.check_table_exists(table_name):
            try:
                cur = self.connection.cursor()
                query = f"DROP TABLE {table_name.upper()}"
                cur.execute(query)
                self.connection.commit()
                cur.close()
                print(f"Table {table_name.upper()} deleted successfully")
            except oracledb.Error as e:
                self.connection.rollback()
                print("Error occured: ", e)
        else:
            print(f"Table {table_name.upper()} does not exist")
        
    def insert_into_table(self, table_name, values_dic):
        """
        Insert a record into the database.

        Args:
        - table_name: str, name of the table to insert the record into
        - values_dic: dict, dictionary containing column names and their corresponding values

        Returns:
        - None
        """
        if self.check_table_exists(table_name):
            try:
                cur = self.connection.cursor()
                col_str = ', '.join(values_dic.keys())
                value_str = ', '.join([f':{i+1}' for i in range(len(values_dic))])
                query = f"INSERT INTO {table_name.upper()}({col_str}) VALUES({value_str})"
                cur.execute(query, list(values_dic.values()))
                self.connection.commit()
                cur.close()
                print("Record inserted successfully")
            except oracledb.Error as e:
                self.connection.rollback()
                print("Error occured: ", e)
        else:
            print(f"Table {table_name.upper()} does not exist")
    
    def call_procedure(self, procedure_name, procedure_inputs):
        """
        Call a stored procedure in the database.

        Args:
        - procedure_name: str, name of the stored procedure to be called
        - procedure_inputs: list, list of inputs to be passed to the stored procedure

        Returns:
        - procedure_return: obj, the return value of the stored procedure
        """
        cur = self.connection.cursor()
        try:
            cur.callproc(procedure_name, procedure_inputs)
            self.connection.commit()
            procedure_return = cur.fetchall()
        except oracledb.DatabaseError as e:
            error, = e.args
            print("Error code:", error.code)
            print("Error message:", error.message)
        finally:
            cur.close()
            return procedure_return
        
    def upload_log(self, procedure_name, procedure_inputs):
        """
        Calls the given stored procedure in the database with the given inputs.

        Args:
        - procedure_name (str): the name of the stored procedure to call
        - procedure_inputs (list): a list of inputs to pass to the stored procedure

        Returns:
        - str: a string describing the result of the stored procedure call
        """
        valid_values = ["drive_1", "drive_2", "drive_3", "drive_4", "drive_5", "drive_6"]
        if not isinstance(procedure_inputs[0], str):
            return "Invalid input: First input must be string"
        elif not isinstance(procedure_inputs[1], datetime.datetime):
            return "Invalid input: Second input must be datetime"
        elif not isinstance(procedure_inputs[2], str):
            return "Invalid input: Third input must be string"
        elif not procedure_inputs[2] in valid_values:
            return "Invalid input: Third input must be a string of value drive_1 / 2 / 3 / 4 / 5 / 6"
        elif not isinstance(procedure_inputs[3], str):
            return "Invalid input: Fourth input must be string"
        else:
            return self.call_procedure(procedure_name, procedure_inputs)        

    def check_duplicates(self, table_name, column_name, value):
        """
        Checks if there are any duplicate entries in the given table for the given column and value.

        Args:
        - table_name (str): the name of the table to check
        - column_name (str): the name of the column to check for duplicates
        - value (any): the value to check for duplicates

        Returns:
        - bool: True if there are duplicates, False otherwise
        """
        cur = self.connection.cursor()
        try:
            query = f"SELECT {column_name}, COUNT(*) FROM {table_name.upper()} WHERE {column_name} = :value GROUP BY {column_name} HAVING COUNT(*) > 1"
            cur.execute(query, value=value)
            cur.fetchall()
            if cur.rowcount > 0:
                return True
            else:
                return False
        except oracledb.Error as e:
            print("There was an error:", e)
            return False
        finally:
            cur.close()

username=
userpwd = 
host = 
port = 
service_name = 

oracle = OracleSQL(username, userpwd, host, port,service_name)
columns_dic = {"id": "INT", "date_time": "DATE"}
oracle.connect()
oracle.create_table("example_table", columns_dic)
now = datetime.now()
values_dic = {"id": 1, "date_time": now}
oracle.insert_into_table("example_table", values_dic)
oracle.check_duplicates("example_table", "id", 1)
oracle.delete_table("example_table")
oracle.close()

    
