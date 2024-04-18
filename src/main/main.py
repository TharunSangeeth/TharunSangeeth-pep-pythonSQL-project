import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
     with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            userId = row.get('userId')
            firstName_raw = row.get('firstName')
            lastName_raw = row.get('lastName')
            
            # Strip whitespace and check if not empty or contains only whitespace
            firstName = firstName_raw.strip() if firstName_raw and not firstName_raw.strip().isspace() else None
            lastName = lastName_raw.strip() if lastName_raw and not lastName_raw.strip().isspace() else None
            
            if userId and firstName and lastName:  # Check if all required fields are present and non-empty
                cursor.execute('''INSERT INTO users (userId, firstName, lastName) VALUES (?, ?, ?)''',
                               (userId, firstName, lastName))
            else:
                print(f"Incomplete record: {row}")
                print(f"Stripped firstName: '{firstName}', Stripped lastName: '{lastName}'")
    conn.commit()


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if all(row.values()):  # Check if all values are non-empty
                cursor.execute('''INSERT INTO callLogs (callId, phoneNumber, startTime, endTime, direction, userId) 
                                  VALUES (?, ?, ?, ?, ?, ?)''',
                               (row['callId'], row['phoneNumber'], row['startTime'], row['endTime'],
                                row['direction'], row['userId']))
            else:
                print(f"Skipping incomplete record: {row}")
    conn.commit()  # Commit the changes


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])

        cursor.execute('''
            SELECT userId, AVG(endTime - startTime) AS avgDuration, COUNT(*) AS numCalls
            FROM callLogs
            GROUP BY userId
        ''')
        rows = cursor.fetchall()
        for row in rows:
            writer.writerow(row)


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])

        cursor.execute('''
            SELECT *
            FROM callLogs
            ORDER BY userId, startTime
        ''')
        rows = cursor.fetchall()
        for row in rows:
            writer.writerow(row)


# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
