import pypyodbc


class DBConnector():
    def get_db_connection(self, host, port, username, password, database):
        """
        connect to database using credential params
        returns connection
        """
        connection = pypyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
            'Server='+host+','+port+';'
            'Database='+database+';'
            'encrypt=yes;'
            'TrustServerCertificate=yes;'
            'UID='+username+';'
            'PWD='+password+';',autocommit = True)
        return connection
