import pandas as pd
import pymysql, sys


class MySQLConnect:

    def __init__(self, host='', database='', user='', password='', port=3306):
        self.dbUser = user 
        self.dbPass = password
        self.dbHost = host
        self.dbPort = port
        self.dbName = database

    def SetConnection(self, host='tpsan1srv01', database='telematics', user='', password='', port=3306 ):
        self.dbUser = user
        self.dbPass = password
        self.dbHost = host
        self.dbPort = port
        self.dbName = database

    def GetConnection(self):
        return pymysql.connect(host=self.dbHost, user=self.dbUser, password=self.dbPass, database=self.dbName, port=self.dbPort)

    def SelectTuple(self, sql):
        connection = self.GetConnection()
        results = ""
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
        except KeyError as ke:
            connection.rollback()
            print(ke)
        connection.close()
        # results is at tuple of tuples
        return tuple([tple[0] for tple in results])

    
    def SelectDict(self, sql):
        connection = self.GetConnection()
        results = ""
        try:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
        except KeyError as ke:
            connection.rollback()
            print(ke)
        connection.close()
        # results is at tuple of tuples
        return results
    
    # def upload_raw(self):
    #     store_raw_to =  """SELECT * FROM `vehicles_to_process_json`;"""
    #     # 'false' to False and 'true' to True
    #     vehiclesList = self.SelectDict(sql=sql_VehiclesToProcess)
    #     for i in range( len(vehiclesList) ):
    #         for key, value in vehiclesList[i].items():
    #             try:
    #                 if value.lower() == 'true':
    #                     vehiclesList[i][key] = True
    #                 elif value.lower() == 'false':
    #                     vehiclesList[i][key] = False
    #                 else:
    #                     pass
    #             except AttributeError:
    #                 print("NoneType Skipped")
    #     return vehiclesList
   
    # def Insert(self, sql=None):
    #     connection = self.GetConnection()
    #     try:
    #         with connection.cursor() as cursor:
    #             try:
    #                 cursor.execute(sql)
    #                 connection.commit()
    #             except pymysql.err.ProgrammingError:
    #                 print("\n\t> Skipped over empty file")
    #                 pass
    #     except pymysql.err.IntegrityError as ke:
    #         connection.rollback()
    #         print("\n\t> Data for this Date Already Exists!")
    #         print("\t> Execption Format: project - project_number - YYYY - MM - DD")
    #         print("\t> {}".format(ke))
    #     connection.close()
    #     # results is at tuple of tuples
    
    def upload_raw_data(self,msg,dbName, dbTable):
        col_name=['vehicle_name','time','label','value']
        ColumnNames = "`" + '`,`'.join(col_name) + "`"  # place an accent character around each `column header`
        # Loading_Bar = LoadingBar(totalIterations=len(data_frame.index), barName="Uploading Rows ") # create loading bar 
        # for index, row in data_frame.iterrows(): # data_frame index is ignored
        #     Loading_Bar.ProgressBar() # Build Loading Bar for each iteration
        raw_in_tuple=(msg["vid"],msg["time"],msg["label"],msg["value"])
        values = "{},{},{},{}".format(raw_in_tuple) # convert row to: -> list -> tuple -> string
        try:
            self.Insert(sql=f"INSERT INTO `{dbName}`.`{dbTable}` ({ColumnNames}) VALUES {values};")
        except pymysql.IntegrityError:
            print(exception)