import pyodbc  # a open source data base driver
import datetime
import random
import string
import getpass
import json
import sys


class Settings():
    def __init__(self):
        map_method_list = []
        self.map_method_list = map_method_list
        with open("ad.json", "r", encoding="utf8") as read_file:
            self.data = json.load(read_file)

        for i in self.data:
            map_method_list.append(i)
        # reading-out json data from file with path

    def generate_result(self, name):  # random choice from map vlaues
        x = random.choice(self.data[name])
        return x


class DBdriver():
    def __init__(self):
        server_name = input("Podaj nazwę serwera:")
        table_name = input("Podaj nazwę bazy:")
        user_name = input("Podaj nazwę użytkownika")
        password = getpass.getpass("Podaj hasło")

        user_choice = input(
            "Logowanie z windows account wybierz 1, logowanie z konta sql wybierz 2::")
        if int(user_choice) == 1:
            connection = pyodbc.connect(
                'DRIVER={SQL Server};SERVER='
                + server_name + ';DATABASE='
                + table_name+';Trusted_Connection=yes;')
        else:
            try:
                connection = pyodbc.connect(
                    'DRIVER={SQL Server};SERVER='
                    + server_name+';DATABASE='
                    + table_name+';UID='+user_name+';PWD='+password) 
            except(pyodbc.Error, err):
                print("Connection error, check login creditials")
                sys.exit(0)
        cursor = connection.cursor()
        method_returning_value_list = [
            "", "", "", "get_decimal", "get_int",
            "", "", "", "", "get_date", "", "", "get_varchar"]

        self.settings = Settings()
        self.connection = connection
        self.cursor = cursor
        self.table_names = []     
        self.method_returning_value_list = method_returning_value_list
        self.settings_dict = dict()

    def get_table_list(self):
        self.cursor.execute("SELECT * FROM Sys.Tables")
        row = self.cursor.fetchall()
        print("Printing list of database tables")

        for i in row:
            print(" ", i[0])
            self.table_names.append(i[0])

        for i in self.table_names:
            print("Settings : %r" % i)
            value = input("Podaj wartość : ")
            self.settings_dict[i] = value

        self.get_extra_info()

    def get_extra_info(self):
        print("Start")
        for table in self.table_names:
            self.cursor.execute(
                """SELECT object_name(parent_object_id),
            object_name(referenced_object_id), name FROM sys.foreign_keys
            WHERE parent_object_id = object_id('{0}')""".format(table))

            fk_tables = self.cursor.fetchall()
            self.cursor.execute('sp_Columns '+'"'+i+'"')
            colums = self.cursor.fetchall()
            self.insert_data_to_db(table, colums, fk_tables)

    def insert_data_to_db(self, table_name, colums, fk_tables):
        related_table_with_keys = dict()
        fk_names_list = []
        if len(fk_tables) != 0:
            for table in fk_tables:
                cut = table[2][:3] + table[2][(len(table_name) + 4):]
                if len(cut) < 4:
                    related_table_with_keys[table[2].lower()] = table[1].lower()
                    fk_names_list.append(table[2].lower())
                else:
                    related_table_with_keys[cut.lower()] = table[1].lower()
                    fk_names_list.append(cut.lower())

        for a in range(int(self.settings_dict[table_name])):
            parameters_value = []          
            for i in colums:
                print("Buggg")
                print(i[3].lower())
                print(fk_names_list)
                if i[3].lower() in fk_names_list:
                    x = self.get_count(related_table_with_keys[i[3].lower()])
                    print("Wartość %x" % x)
                    parameters_value.append(random.randrange(1, x-1, 1))
                else:
                    if i[3].lower() in self.settings.map_method_list:
                        x = self.settings.generate_result(i[3].lower())
                        parameters_value.append(x)
                    else:
                        result = getattr
                        (self,
                         self.method_returning_value_list[
                             abs(i[4])])(i[3].lower())
                        parameters_value.append(result)

            cuted_parameter_value = parameters_value[1:len(parameters_value)] 
            # cuted because public key isn't required
            # to insert (in depend of PK representaion)
            print(cuted_parameter_value)
            if (colums[0][3].lower() == 'pk_id'):
                query = "INSERT INTO {0} values ({1})".format(
                    table_name, ','.join('?' * (len(colums)-1)))
                # print(query)
                self.cursor.execute(query, cuted_parameter_value)
            else:
                query = "INSERT INTO {0} values ({1})".format(
                    table_name, ','.join('?' * len(colums)))
                self.cursor.execute(query, parameters_value)
            self.connection.commit()

    def get_count(self, name):
        self.cursor.execute("SELECT COUNT(*) FROM dbo."+name+" ")
        row = self.cursor.fetchone()
        # print("wiersz")
        # print(row[0])
        return row[0]

    def get_int(self, column_name):
        # print("int")
        return random.randrange(0, 1000, 1)

    def get_date(self, column_name):
        # print("date")
        year = random.randrange(1999, 2019, 1)
        month = random.randrange(1, 12, 1)
        day = random.randrange(1, 27, 1)
        return str(year)+"-"+str(month)+"-"+str(day)

    def get_decimal(self, column_name):
        # print("decimal")
        return round(random.uniform(1, 70), 3)

    def get_varchar(self, column_name):
        # print("varchar")
        return ''.join(
            [
                random.choice(string.ascii_letters + string.digits)
                for n in range(10)])

if __name__ == "__main__":
    print("Siema byku")
    db = DBdriver()
    db.get_table_list()
    print("Done")