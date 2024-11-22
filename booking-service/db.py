import mysql.connector
from mysql.connector import Error

class MySQLDatabase:
    def __init__(self, host, user, password, database):
        """Initialize connection parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish a connection to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Successfully connected to the database.")
        except Error as e:
            print(f"Error: {e}")
            self.connection = None

    def execute_query(self, query, params=None):
        """Execute a single query."""
        if not self.connection:
            print("Connection is not established.")
            return None
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()  # Return results of the query
        except Error as e:
            print(f"Error executing query: {e}")
            return None

    def execute_update(self, query, params=None):
        """Execute an update/insert/delete query."""
        if not self.connection:
            print("Connection is not established.")
            return None
        try:
            self.cursor.execute(query, params)
            self.connection.commit()  # Commit the changes
            print(f"Query executed and committed: {query}")
        except Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()  # Rollback on failure
            return None

    def start_transaction(self):
        """Start a new transaction."""
        if not self.connection:
            print("Connection is not established.")
            return
        self.connection.start_transaction()
        print("Transaction started.")

    def commit_transaction(self):
        """Commit the current transaction."""
        if not self.connection:
            print("Connection is not established.")
            return
        try:
            self.connection.commit()
            print("Transaction committed.")
        except Error as e:
            print(f"Error committing transaction: {e}")

    def rollback_transaction(self):
        """Rollback the current transaction."""
        if not self.connection:
            print("Connection is not established.")
            return
        try:
            self.connection.rollback()
            print("Transaction rolled back.")
        except Error as e:
            print(f"Error rolling back transaction: {e}")

    def close(self):
        """Close the connection to the database."""
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("Connection closed.")
        else:
            print("Connection not open.")

# # Example usage:
# if __name__ == "__main__":
#     db = MySQLDatabase(host="localhost", user="root", password="tirthraj07", database="test_db")

#     db.connect()

#     # Start a transaction
#     db.start_transaction()

#     try:
#         # Example query: Creating a new table
#         db.execute_update("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));")

#         # Example query: Inserting data
#         db.execute_update("INSERT INTO users (name) VALUES (%s);", ("Alice",))

#         # Commit the transaction
#         db.commit_transaction()
#     except Exception as e:
#         print(f"Error during transaction: {e}")
#         db.rollback_transaction()

#     # Query to fetch data
#     result = db.execute_query("SELECT * FROM users;")
#     print(result)

#     # Close the connection
#     db.close()
