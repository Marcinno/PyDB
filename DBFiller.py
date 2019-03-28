import pyodbc # a open source data base driver ? provider?
import datetime
import random
import string
import time
import json

class Settinger():
    def __init__(self):
        map_method_list = []
        self.map_method_list = map_method_list
        with open("ad.json", "r") as read_file:
            self.data = json.load(read_file)
        print(self.data)
        for i in self.data:
            map_method_list.append(i)
        #reading-out json data from file with path

    def generate_result(self, name):
        x = random.choice(self.data[name])
        return x
        
class DBdriver():
    def __init__(self):
        #Settinger object with path
        self.settings = Settinger()
        connection = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-137JTOH\SQLEXPRESS;DATABASE=Test4;UID=user;PWD=admin') #hard coded connection string
        cursor = connection.cursor()
        method_returning_value_list = ["","","","get_decimal","get_int","","","","","get_date","","","get_varchar"]

        self.connection = connection
        self.cursor = cursor
        self.table_names = []     
        self.method_returning_value_list = method_returning_value_list
        
    def get_table_list(self):
        self.settings_dict = dict()
        self.cursor.execute("SELECT * FROM Sys.Tables")
        row = self.cursor.fetchall()
        print("Printing list of database tables")
        for i in row:
            print(" ", i[0])
            self.table_names.append(i[0])

        for i in self.table_names:
            print("Settings : %r" %i)
            value = input("Podaj wartość : ")
            self.settings_dict[i] = value

        self.get_extra_info()

    def get_extra_info(self):
        for i in self.table_names:

            self.cursor.execute("SELECT object_name(parent_object_id), object_name(referenced_object_id), name FROM sys.foreign_keys WHERE parent_object_id = object_id('{0}')".format(i))
            fk_tables = self.cursor.fetchall()

            self.cursor.execute("sp_Columns '"+i+"' ")
            colums = self.cursor.fetchall()
            
            self.insert_data_with_fk(i, colums, fk_tables)

    def insert_data_with_fk(self, table_name, colums, fk_tables):
        related_table_with_keys = dict()
        fk_names_list = []

        if len(fk_tables) != 0 :              
            for i in fk_tables:
                cut =  i[2][:3] + i[2][(len(table_name) + 4):]
                #print(cut)
                if len(cut) < 4:
                    related_table_with_keys[i[2].lower()] = i[1].lower()
                    fk_names_list.append(i[2].lower())
                else:
                    related_table_with_keys[cut.lower()] = i[1].lower()
                    fk_names_list.append(cut.lower())
        #l2 = [x.lower() for x in l1]
        print("Fk names")
        print(fk_names_list)
        for a in range(int(self.settings_dict[table_name])):
            parameters_value = []          
            for i in colums:
                if i[3].lower() in fk_names_list:
                    x = self.get_count(related_table_with_keys[i[3].lower()])
                    print("Wartość %x" %x)
                    parameters_value.append(random.randrange(1, x-1, 1))
                else:
                    if i[3].lower() in self.settings.map_method_list:
                        x = self.settings.generate_result(i[3].lower())
                        parameters_value.append(x)
                    else:
                        result = getattr(self,self.method_returning_value_list[abs(i[4])])(i[3].lower())    
                        parameters_value.append(result)             

            cuted_parameter_value = parameters_value[1:len(parameters_value)] # cuted because public key isn't required to insert
            print(cuted_parameter_value)
            if (colums[0][3].lower() == 'pk_id'):
                query = "INSERT INTO {0} values ({1})".format(table_name, ','.join('?' * (len(colums) -1) ))
                print(query)
                self.cursor.execute(query,cuted_parameter_value)
            else:
                query = "INSERT INTO {0} values ({1})".format(table_name, ','.join('?' * len(colums) ))
                self.cursor.execute(query,parameters_value)
            self.connection.commit()       

    def get_count(self, name):
        self.cursor.execute("SELECT COUNT(*) FROM dbo."+name+" ")
        row = self.cursor.fetchone()
        print(row[0])
        return row[0]

    def get_int(self, column_name):
        print("dupaint")
        return 50
    def get_date(self, column_name):
        print("dupadate")
        return "2016-02-02"
    def get_decimal(self, column_name):
        print("dupadecimal")
        return 2.0
    def get_varchar(self, column_name):
        print("dupavarchar")
        return "eloelo"

if __name__ == "__main__":
    print("Siema byku")
    db = DBdriver()
    db.get_table_list()