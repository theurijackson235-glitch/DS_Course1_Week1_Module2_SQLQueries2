import pandas as pd
import sqlite3


def planets_queries():
    conn1 = sqlite3.connect('planets.db')

    print("\n--- PLANETS TABLE ---")
    print(pd.read_sql("SELECT * FROM planets;", conn1))

    df_no_moons = pd.read_sql(
        "SELECT * FROM planets WHERE num_of_moons = 0;", conn1)

    df_name_seven = pd.read_sql(
        "SELECT name, mass FROM planets WHERE LENGTH(name) = 7;", conn1)

    df_mass = pd.read_sql(
        "SELECT name, mass FROM planets WHERE mass <= 1.00;", conn1)

    df_mass_moon = pd.read_sql(
        "SELECT * FROM planets WHERE num_of_moons >= 1 AND mass < 1.00;", conn1)

    df_blue = pd.read_sql(
        "SELECT name, color FROM planets WHERE color LIKE '%blue%';", conn1)

    conn1.close()

    return {
        "no_moons": df_no_moons,
        "name_seven": df_name_seven,
        "mass": df_mass,
        "mass_moon": df_mass_moon,
        "blue": df_blue
    }


def dogs_queries():
    conn2 = sqlite3.connect('dogs.db')

    print("\n--- DOGS TABLE ---")
    print(pd.read_sql("SELECT * FROM dogs;", conn2))

    df_hungry = pd.read_sql(
        "SELECT name, age, breed FROM dogs WHERE hungry = 1 ORDER BY age ASC;",
        conn2
    )

    df_hungry_ages = pd.read_sql("""
        SELECT name, age, hungry
        FROM dogs
        WHERE hungry = 1
          AND age BETWEEN 2 AND 7
        ORDER BY name ASC;
    """, conn2)

    df_4_oldest = pd.read_sql("""
        SELECT name, age, breed
        FROM dogs
        ORDER BY age DESC
        LIMIT 4;
    """, conn2)

    df_4_oldest = df_4_oldest.sort_values("breed").reset_index(drop=True)

    conn2.close()

    return {
        "hungry": df_hungry,
        "hungry_ages": df_hungry_ages,
        "oldest": df_4_oldest
    }


def babe_ruth_queries():
    conn3 = sqlite3.connect('babe_ruth.db')

    print("\n--- BABE RUTH STATS ---")
    print(pd.read_sql("SELECT * FROM babe_ruth_stats;", conn3))

    df_ruth_years = pd.read_sql("""
        SELECT COUNT(year) AS total_years
        FROM babe_ruth_stats;
    """, conn3)

    df_hr_total = pd.read_sql("""
        SELECT SUM(HR) AS total_homeruns
        FROM babe_ruth_stats;
    """, conn3)

    df_teams_years = pd.read_sql("""
        SELECT team, COUNT(year) AS number_years
        FROM babe_ruth_stats
        GROUP BY team;
    """, conn3)

    df_at_bats = pd.read_sql("""
        SELECT team, AVG(at_bats) AS average_at_bats
        FROM babe_ruth_stats
        GROUP BY team
        HAVING AVG(at_bats) > 200;
    """, conn3)

    conn3.close()

    return {
        "years": df_ruth_years,
        "home_runs": df_hr_total,
        "teams_years": df_teams_years,
        "at_bats": df_at_bats
    }


def main():
    planets = planets_queries()
    dogs = dogs_queries()
    ruth = babe_ruth_queries()

    print("\n--- SAMPLE OUTPUT ---")
    print(planets["no_moons"].head())
    print(dogs["oldest"])
    print(ruth["home_runs"])


if __name__ == "__main__":
    main()