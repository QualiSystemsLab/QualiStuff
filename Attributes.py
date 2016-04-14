#################################################################
# Script Name: 	Reporting
# Use:			Set of Functions that Create and Insert data to database for Sisense Purposes
# Assumptions:	User has access to database
# Author:		Joao Ferreira
# Version:		0.2 (Non Production)
#################################################################

import pyodbc
import uuid
import os
import qualipy.api.cloudshell_api as API


class Reporting_Attributes_Class:
    def __init__(self, Server, Database,User,Password,QualiAPI):
        self.__Database = Database
        self.__User = User
        self.__PWD = Password
        self.__Server = Server
        self.QualiAPI = QualiAPI

        create_table = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'""" + Database + """.dbo.Custom_Attributes') AND type in (N'U'))
        BEGIN
        CREATE TABLE """ + Database + """.dbo.Custom_Attributes (
            ResourceFullName  varchar(200) NOT NULL,
            ResourceName  varchar(200) NOT NULL
        );
        END
        """

        self.cnxn = pyodbc.connect("DRIVER={SQL Server};SERVER=" + self.__Server + ";DATABASE=" + Database + ";UID=" + \
                    User + ";PWD=" + Password)
        self.cursor = self.cnxn.cursor()

        self.__connected = True
        self.cursor.execute(create_table)


    def insert_entry(self,resource_list,attribute_list):
        """
        :param  list[API.FindResourceInfo] attribute_list
        """

        # Create collum for attribute if doesn't exist
        for attribute in attribute_list:

            sql_statement = """ IF COL_LENGTH('Custom_Attributes', '""" + attribute +  """') IS NULL

                BEGIN
                        ALTER TABLE Custom_Attributes
                        ADD [""" + attribute +  """] varchar(200)
                END """

            self.cursor.execute(sql_statement)

        for resource in resource_list:
            #check if resource exists
            assert isinstance(resource, API.FindResourceInfo)
            sql_statement = """ IF NOT EXISTS(SELECT 1 FROM Custom_Attributes
              WHERE ResourceFullName = '""" + resource.FullName + """' AND ResourceName = '""" + resource.Name + """')
              BEGIN
                INSERT INTO Custom_Attributes (ResourceFullName,ResourceName) VALUES ( '""" + resource.FullName + """' , '""" + resource.Name + """');
              END """
            self.cursor.execute(sql_statement)

            assert isinstance(self.QualiAPI, API.CloudShellAPISession)
            resource_attributes = self.QualiAPI.GetResourceDetails(resource.FullName).ResourceAttributes
            for attribute in attribute_list:
                for resource_attribute in resource_attributes:
                    if (resource_attribute.Name == attribute):
                        sql_statement = """ UPDATE Custom_Attributes
                        SET [""" + attribute + """ = '"""] + str(resource_attribute.Value) + """'
                        WHERE ResourceFullName = '""" + resource.FullName + """' AND ResourceName = '""" + resource.Name + """'; """
                        self.cursor.execute(sql_statement)



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


attributes_to_record = ["asd","asddd","sdf"]

# Set Quali Session and list of resources
QualiAPISession = API.CloudShellAPISession(user="admin",password="admin",host="localhost",domain="Global")
#QualiAPISession
list_of_resources = QualiAPISession.FindResources(resourceFamily="test").Resources


# Set Connetion
Connection = Reporting_Attributes_Class(Server="localhost\sqlexpress",Database="QualiInsight",User="quali",Password="quali",QualiAPI=QualiAPISession)
Connection.insert_entry(resource_list=list_of_resources,attribute_list=attributes_to_record)
#Connection.insert_key(key_name="RESULT",key_value="PASS")
Connection.commit()







