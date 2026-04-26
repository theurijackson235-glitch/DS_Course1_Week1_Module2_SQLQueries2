# CodeGrade step0

# Run this cell without changes

import pandas as pd
import sqlite3

# Create the connection
# Note the connect is 'conn1' since there will be multiple .db used
conn1 = sqlite3.connect('planets.db')

# Select all
pd.read_sql("""SELECT * FROM planets; """, conn1)
# CodeGrade step1
# Replace None with your code
df_no_moons = pd.read_sql("SELECT * FROM planets WHERE num_of_moons = 0;", conn1)
# CodeGrade step2
# Replace None with your code
df_name_seven = pd.read_sql("SELECT name, mass FROM planets WHERE LENGTH(name) = 7;", conn1)
# CodeGrade step3
# Replace None with your code
df_mass = pd.read_sql("SELECT name, mass FROM planets WHERE mass <= 1.00;", conn1)
# CodeGrade step4
# Replace None with your code
df_mass_moon = pd.read_sql("SELECT * FROM planets WHERE num_of_moons >= 1 AND mass < 1.00;", conn1)
# CodeGrade step5
# Replace None with your code
df_blue = pd.read_sql("SELECT name, color FROM planets WHERE color LIKE '%blue%';", conn1)
# CodeGrade step0

# Run this cell without changes

# Create a connection
# Note the connect is 'conn2' since they will be multiple .db used
conn2 = sqlite3.connect('dogs.db')

# Select all
pd.read_sql("SELECT * FROM dogs;", conn2)
# CodeGrade step6
# Replace None with your code
df_hungry = pd.read_sql("SELECT name, age, breed FROM dogs WHERE hungry = 1 ORDER BY age ASC;", conn2)
# CodeGrade step7
# Replace None with your code
df_hungry_ages = pd.read_sql("""
    SELECT name, age, hungry 
    FROM dogs 
    WHERE hungry = 1 
      AND age BETWEEN 2 AND 7 
    ORDER BY name ASC;
""", conn2)
# CodeGrade step8
# Replace None with your code
df_4_oldest =  pd.read_sql("""
    SELECT name, age, breed
    FROM dogs
    ORDER BY age DESC
    LIMIT 4;
""", conn2)
df_4_oldest = df_4_oldest.sort_values("breed").reset_index(drop=True)
# CodeGrade step0

# Run this cell without changes

# Create a connection
# Note the connect is 'conn3' since they will be multiple .db used
conn3 = sqlite3.connect('babe_ruth.db')

# Select all
pd.read_sql("""
SELECT * FROM babe_ruth_stats; """, conn3)
# CodeGrade step9
# Replace None with your code
df_ruth_years = pd.read_sql("""
    SELECT COUNT(year) AS total_years
    FROM babe_ruth_stats;
""", conn3)
# CodeGrade step10
# Replace None with your code
df_hr_total = pd.read_sql("""
    SELECT SUM(HR) AS total_homeruns
    FROM babe_ruth_stats;
""", conn3)
# CodeGrade step11
# Replace None with your code
df_teams_years = pd.read_sql("""
    SELECT team, COUNT(year) AS number_years
    FROM babe_ruth_stats
    GROUP BY team;
""", conn3)
# CodeGrade step12
# Replace None with your code
df_at_bats = pd.read_sql("""
    SELECT team, AVG(at_bats) AS average_at_bats
    FROM babe_ruth_stats
    GROUP BY team
    HAVING AVG(at_bats) > 200
""", conn3)
# Run this cell without changes

conn1.close()
conn2.close()
conn3.close()