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


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(filepath):
    with open(filepath, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  
        for row in reader:
            
            if len(row) == 2:  
                first_name, last_name = row
               
                if first_name.strip() and last_name.strip():
                    cursor.execute('''INSERT INTO users (firstName, lastName)
                                      VALUES (?, ?)''', (first_name, last_name))
            else:
                print("Invalid row:", row)



def load_and_clean_call_logs(file_path):
    with open(file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  
        for row in reader:
           
            if len(row) == 5: 
                phone_number, start_time, end_time, direction, user_id = row
                
                if all(field.strip() for field in row):
                    cursor.execute('''INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId)
                                      VALUES (?, ?, ?, ?, ?)''', (phone_number, start_time, end_time, direction, user_id))
            else:
                print("Invalid row:", row)



def write_user_analytics(csv_file_path):
    
    user_data = {}

    
    cursor.execute('''SELECT userId, SUM(endTime - startTime) AS total_duration, COUNT(*) AS num_calls
                      FROM callLogs
                      GROUP BY userId''')
    results = cursor.fetchall()

    
    for row in results:
        user_id, total_duration, num_calls = row
        user_data[user_id] = (total_duration, num_calls)

   
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])
        for user_id, (total_duration, num_calls) in user_data.items():
            if num_calls > 0:
                avg_duration = total_duration / num_calls
                writer.writerow([user_id, avg_duration, num_calls])
            else:
                
                writer.writerow([user_id, 0, num_calls])

def write_ordered_calls(csv_file_path):
    
    cursor.execute('''SELECT * FROM callLogs ORDER BY userId, startTime''')
    ordered_calls = cursor.fetchall()

    
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
        for call_log in ordered_calls:
            writer.writerow(call_log)



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

if 'name' == 'main':
    main()