#################################################################
# Script Name: 	Reporting
# Use:			Set of Functions that Create and Insert data to database for Sisense Purposes
# Assumptions:	User has access to database
# Author:		Joao Ferreira
# Version:		0.1 (Non Production)
#################################################################

import pyodbc
import uuid
import os


class Reporting_Class:
    def __init__(self, Server, Database,User,Password):
        self.__Database = Database
        self.__User = User
        self.__PWD = Password
        self.__Server = Server
        self.__uuid = str(uuid.uuid1())

        create_table = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'""" + Database + """.dbo.Custom_Execution') AND type in (N'U'))
        BEGIN
        CREATE TABLE """ + Database + """.dbo.Custom_Execution (
            ReservationID  varchar(200) NOT NULL,
            UID uniqueidentifier  NOT NULL,
            SCRIPT_NAME varchar(200) NOT NULL,
            TYPE varchar(200) NOT NULL,
            RESOURCE varchar(200),
            TOPOLOGY varchar(200)
        );
        END
        """

        create_table_ID = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'""" + Database + """.dbo.Custom_Execution_Key') AND type in (N'U'))
        BEGIN
        CREATE TABLE """ + Database + """.dbo.Custom_Execution_Key (
            UID  uniqueidentifier NOT NULL,
            KEY_NAME  varchar(200) NOT NULL,
            KEY_VALUE  varchar(200) NOT NULL
        );
        END
        """

        self.cnxn = pyodbc.connect("DRIVER={SQL Server};SERVER=" + self.__Server + ";DATABASE=" + Database + ";UID=" + \
                    User + ";PWD=" + Password)
        self.cursor = self.cnxn.cursor()

        self.__connected = True
        self.cursor.execute(create_table_ID)
        self.cursor.execute(create_table)


    def insert_entry(self,reservation_id,type="Resource",resource=None,topology=None):
        Script_Name = os.path.basename(__file__)
        self.cursor.execute ("INSERT INTO Custom_Execution (ReservationID,UID,SCRIPT_NAME,TYPE,RESOURCE,TOPOLOGY) VALUES (?,?,?,?,?,?);" \
                             ,(reservation_id,self.__uuid,Script_Name,type,resource,topology))

    def ___print_table(self,table):
        self.cursor.execute('SELECT * FROM QualiInsight.dbo.ResourceExecution')

        for row in self.cursor.fetchall():
             print row

    def commit(self):
        self.cursor.commit()

    def insert_key(self,key_name,key_value):
        self.cursor.execute ("INSERT INTO Custom_Execution_Key (UID,KEY_NAME,KEY_VALUE) VALUES (?,?,?);",(self.__uuid,key_name,key_value))

    def __del__(self):
        if (self.__connected ==True):
            print("Closing Connection")
            self.cursor.close()
            self.cnxn.close()



ReservationID = "test"
Connection = Reporting_Class(Server="localhost\sqlexpress",Database="QualiInsight",User="quali",Password="quali")
Connection.insert_entry(reservation_id=ReservationID,type="Resource",resource=None,topology=None)
Connection.insert_key(key_name="RESULT",key_value="PASS")
Connection.commit()







