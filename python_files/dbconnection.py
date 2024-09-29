import mysql.connector
class DBConnection:

    def get_connection(self):
        conn = mysql.connector.connect(host="localhost", user="root", passwd="")
        mycursor = conn.cursor()
        sql = "use addb"
        mycursor.execute(sql)
        conn.commit()
        return conn
