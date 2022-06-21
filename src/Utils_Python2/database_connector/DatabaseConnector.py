import os
import configparser
import urllib.parse

import cx_Oracle
import psycopg2

from sqlalchemy import create_engine
# from sqlalchemy import text


# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta
sys.path.append(f"{Path(__file__).parents[1]}/data_manipulator/")
from StringManipulator import StringManipulator



class DatabaseConnector(metaclass=SingletonMeta):
    '''
    Class of DatabaseConnector

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of DatabaseConnector object.
    '''


    __config = None
    __connection = {}
    __stringManipulator = None
    __RANDOM_STRING_LENGTH = 8


    def __init__(self):
        config = configparser.ConfigParser()
        config.read(f'{os.path.dirname(os.path.abspath(__file__))}/database_connector_config.ini')
        self.__config = config
        self.__stringManipulator = StringManipulator()


    def getConnectionDetailByTag(self, connTagName):
        '''
        Get the details needed to open a database connection,
        based on the tag name recorded in the 'database_connector_config.ini'.

        Parameters:
        connTagName (str): Connection tag name. Either <connection_tag_name> OR <connection_tag_name>_<session_id>

        Returns:
        Object: Returns information details in the object or JSON format string.
        '''
        try:
            if self.__config[connTagName]:
                return self.__config[connTagName]

            if len(connTagName.rsplit('_', 1)[1]) == self.__RANDOM_STRING_LENGTH:
                tmpConnTagName = connTagName.rsplit('_', 1)[0]
                return self.__config[tmpConnTagName]
            else:
                print(  f'''Cannot find the information of the given Connection Name '{connTagName}'. Was that a Connection "Tag" Name? \n'''
                        f'''Please ensure that the format is <Tag Name> OR <Tag Name>_<Session ID>''')
        except Exception as e:
            raise Exception(f"getConnectionDetailByTag() - {e}") from e


    def openConnection(self, connTagName, isSqlAlchemy=False, verbose=True):
        '''
        Open a database connection.

        Parameters:
        connTagName (str): Connection tag name. Format: <connection_tag_name>
        isSqlAlchemy (bool): If the connection is using SQLAlchemy.
        verbose (bool): If the operation is verbose.

        Returns:
        Object: Returns the Connection object, which comprises of {connID, engine, connection, databaseName}.
        '''
        connID = ''

        dbUri = None
        engine = None
        toStoreConn = True

        try:
            config = self.__config[connTagName]

            match config['type']:
                case 'postgresql':
                    if isSqlAlchemy:
                        dbUri = (   f"""postgresql+psycopg2://"""
                                    f"""{config['username']}:{urllib.parse.quote_plus(config['password'])}"""
                                    f"""@{config['host']}:{config['port']}"""
                                    f"""/{config['database']}""")
                    else:
                        conn = psycopg2.connect(host=config['host'],
                                                port=config['port'],
                                                dbname=config['database'],
                                                user=config['username'],
                                                password=urllib.parse.quote_plus(config['password']))
                        conn.autocommit = False
                case 'mssql':
                    if isSqlAlchemy:
                        dbUri = (   f"""mssql+pyodbc://"""
                                    f"""{config['username']}:{urllib.parse.quote_plus(config['password'])}"""
                                    f"""@{config['host']}:{config['port']}"""
                                    f"""/{config['database']}?driver=ODBC+Driver+17+for+SQL+Server""")
                    else:
                        pass
                case 'oracle':
                    if isSqlAlchemy:
                        dbUri = (   f"""oracle+cx_oracle://"""
                                    f"""{config['username']}:{urllib.parse.quote_plus(config['password'])}"""
                                    f"""@{config['host']}:{config['port']}"""
                                    f"""/?service_name={config['database']}"""
                                    f"""&encoding=UTF-8&nencoding=UTF-8""")
                    else:
                        dsn_tns = cx_Oracle.makedsn(config['host'],
                                                    config['port'],
                                                    service_name=config['database'])

                        conn = cx_Oracle.connect(user=config['username'],
                                                    password=urllib.parse.quote_plus(config['password']),
                                                    dsn=dsn_tns,
                                                    encoding="UTF-8")
                        conn.autocommit = False
                case None:
                    toStoreConn = False
                case _:
                    toStoreConn = False

            if toStoreConn:
                if dbUri is not None:
                    engine = create_engine(dbUri)
                    conn = engine.connect()

                connID = f"{connTagName}_{self.__stringManipulator.getAlphaNumericRandomString(self.__RANDOM_STRING_LENGTH)}"
                while any(connID in stringName for stringName in self.__connection.keys()):
                    connID = f"{connTagName}_{self.__stringManipulator.getAlphaNumericRandomString(self.__RANDOM_STRING_LENGTH)}"

                self.__connection[connID] = {   
                                                'connID': connID,
                                                'engine': engine,
                                                'connection': conn,
                                                'databaseName': config['database']
                                            }
                if verbose:
                    print(f'Successfully connect to \'{connTagName}\' database, with connection ID \'{connID}\'!')

                return self.__connection[connID]
            else:
                if verbose:
                    print(f'Did not open connection: \'{connID}\'')

        except Exception as e:
            raise Exception(f"openConnection() - {connTagName}\n{e}") from e
        return None


    def closeConnection(self, connObject, verbose=True):
        '''
        Close a database connection based on given Database Connection Object.

        Parameters:
        connObject (Object): Database connection object.
        verbose (bool): If the operation is verbose.

        Returns:
        None: No return value.
        '''
        try:
            connDict = self.__connection[connObject['connID']]
            connDict['connection'].close()
            if connDict['engine'] is not None:
                connDict['engine'].dispose()
            del self.__connection[connObject['connID']]
            if verbose:
                print(f"""Successfully close the \'{connObject['connID']}\' connection!""")
        except Exception as e:
            raise Exception(f"closeConnection() - {connObject['connID']}\n{e}") from e


    # def getConnection(self, connName):
    #     '''
    #     Get the database connection.

    #     Parameters:
    #     connName (str): Connection name.

    #     Returns:
    #     Object: Returns the connection engine.
    #     '''
    #     try:
    #         return self.__connection[connName]['connection']
    #     except Exception as e:
    #         raise Exception(f"getConnection() - {e}") from e


    # def setConnectionTimeout(self, connName, ms):
    #     '''
    #     Sets the database connection timeout.

    #     Parameters:
    #     connName (str): Connection name.
    #     ms (int): Milliseconds to wait.

    #     Returns:
    #     None: No return value.
    #     '''
    #     try:
    #         if len(connName.rsplit('_', 1)[1]) == self.__RANDOM_STRING_LENGTH:
    #             connTagName = connName.rsplit('_', 1)[0]
    #         else:
    #             print(  f'Cannot find the information of the given Connection Name. Was that a Connection "Tag" Name? \n'
    #                     f'Please ensure that the format is <Tag Name>_<Session ID>')
    #             return

    #         config = self.__config[connTagName]

    #         match config['type']:
    #             case 'postgresql':
    #                 pass
    #             case 'mssql':
    #                 pass
    #             case 'oracle':
    #                 self.__connection[connName]['connection'].callTimeout = ms
    #             case None:
    #                 pass
    #             case _:
    #                 pass
    #     except Exception as e:
    #         raise Exception(f"setConnectionTimeout() - {e}") from e


    # def createDbTableFromDataFrame(self, connName, df, tempTableName, schema=None):
    #     '''
    #     Create a table in the connected database based on the given dataframe.
    #     Table Name will based on the given 'tempTableName'.
    #     Column structure will be automatically generated.
    #     It will fail if the table is already exists.

    #     Parameters:
    #     connName (str): Connection name.
    #     df (DataFrame): A DataFrame to be use to create Table in the database.
    #     tempTableName (str): A name for the created Table in the database.
    #     ifExists (str): String that determines what is going to happen if the Table already existed.
    #         {'fail', 'replace', 'append'}, default 'fail'.

    #     Returns:
    #     None: No return value.
    #     '''
    #     try:
    #         df.to_sql(tempTableName, self.__connection[connName]['connection'], schema=schema, if_exists='fail', index=False)
    #         print(f"""Successfully create \'{tempTableName}\' table in the \'{self.__connection[connName]['databaseName']}\' database!\n""")
    #     except Exception as e:
    #         raise Exception(f"createDbTableFromDataFrame() - {e}") from e


    # def pushDataFrameToDb(self, connName, df, tableName, targetSchema=None):
    #     '''
    #     Push the dataframe into an exising Database.
    #     It will append to the given table name in the Database.

    #     Parameters:
    #     connName (str): Connection name.
    #     df (DataFrame): A DataFrame to be pushed into the given Table in the database.
    #     tableName (str): A name of the given Table in the database to be appended.

    #     Returns:
    #     None: No return value.
    #     '''
    #     try:
    #         # df=df.astype(str)
    #         df = df.head(10)
    #         df.to_sql(name=tableName, 
    #                     con=self.__connection[connName]['connection'], 
    #                     schema=targetSchema, 
    #                     if_exists='append', 
    #                     index=False,
    #                     chunksize=10000)
    #         print(f"""{connName} - Successfully append the data into \'{targetSchema}.{tableName}\' table in the \'{self.__connection[connName]['databaseName']}\'!\n""")
    #     except Exception as e:
    #         raise Exception(f"pushDataFrameToDb() - {e}") from e


    # def beginTransaction(self, connName):
    #     '''
    #     Begin a transaction for a specific connection.

    #     Parameters:
    #     connName (str): Connection name.

    #     Returns:
    #     Object: Return a transaction object.
    #     '''
    #     try:
    #         return self.__connection[connName]['connection'].begin()
    #     except Exception as e:
    #         raise Exception(f"beginTransaction() - {e}") from e


    # def executeQueryWithoutResult(self, connName, query):
    #     '''
    #     Execute query only without returning any results from the database.

    #     Parameters:
    #     connName (str): Connection name.
    #     query (Text): Text object from sqlalchemy.

    #     Returns:
    #     None: No return value.
    #     '''
    #     try:
    #         self.__connection[connName]['connection'].execute(query)
    #     except Exception as e:
    #         raise Exception(f"executeQueryWithoutResult() - {e}") from e


    # def executeQueryWithResult(self, connName, query):
    #     '''
    #     Execute query which returns results from the database.

    #     Parameters:
    #     connName (str): Connection name.
    #     query (Text): Text object from sqlalchemy.

    #     Returns:
    #     List: Results from the database.
    #     '''
    #     try:
    #         return self.__connection[connName]['connection'].execute(query)
    #     except Exception as e:
    #         raise Exception(f"executeQueryWithResult() - {e}") from e


    # def createTableStructureBasedOn(self, connName, tableName, refTableName):
    #     '''
    #     Create an Empty New Table based on a Reference Table.

    #     Parameters:
    #     connName (str): Connection name.
    #     tableName (str): New Table name to be created.
    #     refTableName (str): Reference Table name to be referred to.

    #     Returns:
    #     None: No return value.
    #     '''
    #     try:
    #         config = self.__config[connName]

    #         match config['type']:
    #             case 'postgresql':
    #                 query = text(   f"""CREATE TABLE IF NOT EXISTS {tableName} """
    #                                 f"""(LIKE {refTableName} INCLUDING ALL); \n""")
    #             case 'mssql':
    #                 query = text(   f"""IF NOT EXISTS (SELECT * FROM sys.objects """
    #                                 f"""               WHERE object_id = OBJECT_ID('{tableName}')) """
    #                                 f"""BEGIN """
    #                                 f"""    SELECT * INTO {tableName} """
    #                                 f"""    FROM {refTableName} WHERE 1 = 0; """
    #                                 f"""END \n""")
    #             case '':
    #                 query = None
    #             case None:
    #                 query = None
    #             case _:
    #                 query = None

    #         self.__connection[connName]['connection'].execute(query)

    #     except Exception as e:
    #         raise Exception(f"createTableStructureBasedOn() - {e}") from e


    # def getStringAllColumnNames(self, connName, tableName):
    #     '''
    #     Return all column names of the table in one single string.

    #     Parameters:
    #     connName (str): Connection name.
    #     tableName (str): Name of the table.

    #     Returns:
    #     str: List of column names with comma-separated.
    #     '''
    #     try:
    #         config = self.__config[connName]
    #         match config['type']:
    #             case 'postgresql':
    #                 query = text(   f"""SELECT COLUMN_NAME """
    #                                 f"""FROM INFORMATION_SCHEMA.COLUMNS """
    #                                 f"""WHERE TABLE_NAME = '{tableName}' """
    #                                 f"""ORDER BY ORDINAL_POSITION; """)
    #             case 'mssql':
    #                 query = text(   f"""SELECT COLUMN_NAME """
    #                                 f"""FROM INFORMATION_SCHEMA.COLUMNS """
    #                                 f"""WHERE TABLE_NAME = '{tableName}' """
    #                                 f"""ORDER BY ORDINAL_POSITION; """)
    #             case 'oracle':
    #                 query = text(   f"""SELECT column_name FROM all_tab_cols WHERE table_name = '{tableName}' \n""")
    #             case None:
    #                 query = None
    #             case _:
    #                 query = None
                    
    #         res = self.__connection[connName]['connection'].execute(query)
            
    #         colNames = ''
    #         for row in res:
    #             colNames += f"""{row[0]}, """
    #         colNames = colNames[:-len(', ')]
    #         return colNames
    #     except Exception as e:
    #         raise Exception(f"getStringAllColumnNames() - {e}") from e
