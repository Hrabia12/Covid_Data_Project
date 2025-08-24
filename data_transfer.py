import csv
import psycopg2
import mysql.connector

conn_pg = psycopg2.connect(
    host="aws-1-eu-north-1.pooler.supabase.com",
    database="postgres",
    user="postgres.iwdgjfmeaxqabnobmsfl",
    password="Hrabia-2580",
    port="6543",
)

conn_mysql = mysql.connector.connect(
    host="caboose.proxy.rlwy.net",
    database="railway",
    user="root",
    password="RprdIhOohGskZeGZlrqYREZWTuxmmjqC",
    port="32580",
)

cursor1 = conn_mysql.cursor()
cursor2 = conn_pg.cursor()

cursor1.execute("DROP TABLE IF EXISTS covid_deaths")

sql1 = """
    CREATE TABLE covid_deaths (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,
    total_cases DOUBLE PRECISION,
    new_cases DOUBLE PRECISION,
    new_cases_smoothed DOUBLE PRECISION,
    total_deaths DOUBLE PRECISION,
    new_deaths DOUBLE PRECISION,
    new_deaths_smoothed DOUBLE PRECISION,
    total_cases_per_million DOUBLE PRECISION,
    new_cases_per_million DOUBLE PRECISION,
    new_cases_smoothed_per_million DOUBLE PRECISION,
    total_deaths_per_million DOUBLE PRECISION,
    new_deaths_per_million DOUBLE PRECISION,
    new_deaths_smoothed_per_million DOUBLE PRECISION,
    reproduction_rate DOUBLE PRECISION,
    icu_patients DOUBLE PRECISION,
    icu_patients_per_million DOUBLE PRECISION,
    hosp_patients DOUBLE PRECISION,
    hosp_patients_per_million DOUBLE PRECISION,
    weekly_icu_admissions DOUBLE PRECISION,
    weekly_icu_admissions_per_million DOUBLE PRECISION,
    weekly_hosp_admissions DOUBLE PRECISION,
    weekly_hosp_admissions_per_million DOUBLE PRECISION,
    new_tests DOUBLE PRECISION,
    total_tests DOUBLE PRECISION,
    total_tests_per_thousand DOUBLE PRECISION,
    new_tests_per_thousand DOUBLE PRECISION,
    new_tests_smoothed DOUBLE PRECISION,
    new_tests_smoothed_per_thousand DOUBLE PRECISION,
    positive_rate DOUBLE PRECISION,
    tests_per_case DOUBLE PRECISION,
    tests_units TEXT,
    total_vaccinations DOUBLE PRECISION,
    people_vaccinated DOUBLE PRECISION,
    people_fully_vaccinated DOUBLE PRECISION,
    new_vaccinations DOUBLE PRECISION,
    new_vaccinations_smoothed DOUBLE PRECISION,
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    new_vaccinations_smoothed_per_million DOUBLE PRECISION,
    stringency_index DOUBLE PRECISION,
    population DOUBLE PRECISION,
    population_density DOUBLE PRECISION,
    median_age DOUBLE PRECISION,
    aged_65_older DOUBLE PRECISION,
    aged_70_older DOUBLE PRECISION,
    gdp_per_capita DOUBLE PRECISION,
    extreme_poverty DOUBLE PRECISION,
    cardiovasc_death_rate DOUBLE PRECISION,
    diabetes_prevalence DOUBLE PRECISION,
    female_smokers DOUBLE PRECISION,
    male_smokers DOUBLE PRECISION,
    handwashing_facilities DOUBLE PRECISION,
    hospital_beds_per_thousand DOUBLE PRECISION,
    life_expectancy DOUBLE PRECISION,
    human_development_index DOUBLE PRECISION
 )
 """
cursor1.execute(sql1)


sql2 = "COPY covid_deaths TO STDOUT DELIMITER ',' CSV HEADER"
with open("/home/michal/Covid_Data/covid_deaths.csv", "w", newline="") as f:
    cursor2.copy_expert(sql2, f)

sql3 = """
    CREATE TABLE covid_vaccinations (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,
    new_tests DOUBLE PRECISION,
    total_tests DOUBLE PRECISION,
    total_tests_per_thousand DOUBLE PRECISION,
    new_tests_per_thousand DOUBLE PRECISION,
    new_tests_smoothed DOUBLE PRECISION,
    new_tests_smoothed_per_thousand DOUBLE PRECISION,
    positive_rate DOUBLE PRECISION,
    tests_per_case DOUBLE PRECISION,
    tests_units TEXT,
    total_vaccinations DOUBLE PRECISION,
    people_vaccinated DOUBLE PRECISION,
    people_fully_vaccinated DOUBLE PRECISION,
    new_vaccinations DOUBLE PRECISION,
    new_vaccinations_smoothed DOUBLE PRECISION,
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    new_vaccinations_smoothed_per_million DOUBLE PRECISION,
    stringency_index DOUBLE PRECISION,
    population_density DOUBLE PRECISION,
    median_age DOUBLE PRECISION,
    aged_65_older DOUBLE PRECISION,
    aged_70_older DOUBLE PRECISION,
    gdp_per_capita DOUBLE PRECISION,
    extreme_poverty DOUBLE PRECISION,
    cardiovasc_death_rate DOUBLE PRECISION,
    diabetes_prevalence DOUBLE PRECISION,
    female_smokers DOUBLE PRECISION,
    male_smokers DOUBLE PRECISION,
    handwashing_facilities DOUBLE PRECISION,
    hospital_beds_per_thousand DOUBLE PRECISION,
    life_expectancy DOUBLE PRECISION,
    human_development_index DOUBLE PRECISION
 )
 """

cursor1.execute(sql3)

sql4 = "COPY covid_vaccinations TO STDOUT DELIMITER ',' CSV HEADER"
with open("/home/michal/Covid_Data/covid_vaccinations.csv", "w", newline="") as f:
    cursor2.copy_expert(sql4, f)

sql5 = """
    SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'gdp_data' 
    ORDER BY ordinal_position
"""
cursor2.execute(sql5)
gdp_schema = cursor2.fetchall()

cursor1.execute("DROP TABLE IF EXISTS gdp_data")

sql6 = """
    CREATE TABLE gdp_data AS 
    SELECT * FROM (
        SELECT 
            column_name,
            CASE 
                WHEN data_type = 'text' THEN 'TEXT'
                WHEN data_type = 'character varying' AND character_maximum_length IS NOT NULL THEN CONCAT('VARCHAR(', character_maximum_length, ')')
                WHEN data_type = 'character varying' THEN 'TEXT'
                WHEN data_type = 'integer' THEN 'INT'
                WHEN data_type = 'bigint' THEN 'BIGINT'
                WHEN data_type = 'double precision' THEN 'DOUBLE'
                WHEN data_type = 'numeric' THEN 'DECIMAL(65,30)'
                WHEN data_type = 'date' THEN 'DATE'
                WHEN data_type = 'timestamp' THEN 'TIMESTAMP'
                WHEN data_type = 'boolean' THEN 'BOOLEAN'
                ELSE 'TEXT'
            END as mysql_type,
            CASE WHEN is_nullable = 'YES' THEN '' ELSE ' NOT NULL' END as nullable
        FROM information_schema.columns 
        WHERE table_name = 'gdp_data'
        ORDER BY ordinal_position
    ) schema_info
"""
cursor1.execute(sql6)

sql7 = "COPY gdp_data TO STDOUT DELIMITER ',' CSV HEADER"
with open("/home/michal/Covid_Data/gdp_data.csv", "w", newline="") as f:
    cursor2.copy_expert(sql7, f)

cursor1.close()
cursor2.close()
conn_mysql.close()
conn_pg.close()
