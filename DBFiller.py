import pyodbc # a open source data base driver ? provider?
import datetime
import random
import string
import time
sql = """SELECT
object_name(parent_object_id),
object_name(referenced_object_id),
name 
FROM sys.foreign_keys
WHERE parent_object_id = object_id('Kraj')

"""
class DBdriver():
    def __init__(self):
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-137JTOH\SQLEXPRESS;DATABASE=Test;UID=user;PWD=admin') #hard coded connection string
        cursor = conn.cursor()

        self.conn = conn
        self.cursor = cursor
        self.table_names = []     

    def get_table_list(self):
        self.d = dict()
        self.cursor.execute("SELECT * FROM Sys.Tables")
        row = self.cursor.fetchall()
        print("Printing list of database tables")
        for i in row:
            print(" ", i[0])
            self.table_names.append(i[0])

        for i in self.table_names:
            print("Settings : %r" %i)
            value = input("Podaj wartość : ")
            self.d[i] = value

        self.filling_itxd()

    def filling_itxd(self):
        global sql
        for i in self.table_names:

            self.cursor.execute("SELECT object_name(parent_object_id), object_name(referenced_object_id), name FROM sys.foreign_keys WHERE parent_object_id = object_id('{0}')".format(i))
            fk_tables = self.cursor.fetchall()

            self.cursor.execute("sp_Columns '"+i+"' ")
            colums = self.cursor.fetchall()
            
            if not fk_tables:
                self.insert_data(i, colums)
            else:
                self.insert_data_with_fk(i, colums, fk_tables)

    def insert_data(self, table_name, columns):
        for a in range(int(self.d[table_name])):              
            params = []
            print(len(columns))

            for i in columns:
                if i[4] == 4:
                    ina = random.randrange(0, 101, 2)
                    params.append(ina)
                elif i[4] == 12:
                    sta = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(6))
                    params.append(sta)
                elif i[4] == 3:
                    params.append(random.randrange(0, 101, 2))           
                elif i[4] == -9:
                    params.append(datetime.datetime.now)
            param = params[1:len(params)]
            #print(param)
            query = "INSERT INTO {0} values ({1})".format(table_name, ','.join('?' * (len(columns) -1) ))
            self.cursor.execute(query,param)
            self.conn.commit()

    def insert_data_with_fk(self, table_name, colums, fk_tables):
        d1 = dict()
        l1 = []
        for i in fk_tables:
            temp = i[2]
            cut =  i[2][:3] + i[2][(len(table_name) + 4):]
            #print(cut)
            if len(cut) < 4:
                d1[i[2].lower()] = i[1].lower()
                l1.append(i[2])
            else:
                d1[cut.lower()] = i[1].lower()
                l1.append(cut)

        l2 = [x.lower() for x in l1]
        for a in range(int(self.d[table_name])):              
            params = []

            print(l1)
            for i in colums:
                #time.sleep(0.1)
                print("KII " + i[3].lower())
                if i[3].lower() in l2:
                    print("????")
                    x = self.get_count(d1[i[3].lower()])
                    print("Wartość %x" %x)
                    params.append(x)
                else:
                    if i[4] == 4:
                        ina = random.randrange(1, 80, 1)
                        params.append(ina)
                    elif i[4] == 12:
                        sta = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(6))
                        params.append(str(sta))
                    elif i[4] == 3:
                        params.append(25.5)           
                    elif i[4] == -9:
                        params.append("2005-02-02")
            param = params[1:len(params)]
            print(param)
            print(colums[0][3].lower())
            if (colums[0][3].lower() == 'pk_id'):
                query = "INSERT INTO {0} values ({1})".format(table_name, ','.join('?' * (len(colums) -1) ))
                self.cursor.execute(query,param)
            else:
                query = "INSERT INTO {0} values ({1})".format(table_name, ','.join('?' * len(colums) ))
                self.cursor.execute(query,params)
            self.conn.commit()       

    def get_count(self, name):
        self.cursor.execute("SELECT count(*) FROM dbo."+name+" ")
        row = self.cursor.fetchone()
        print(row[0])
        return row[0]

class FillData():
    def __init__():
        pass


#cursor.execute(sql)


if __name__ == "__main__":
    print("Siema byku")
    db = DBdriver()
    db.get_table_list()
