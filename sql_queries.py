import configparser

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
config = configparser.ConfigParser()
config.read('dwh.cfg')

# ------------------------------------------------------------------------------
# DROP TABLES
# ------------------------------------------------------------------------------
staging_imm_table_drop   = "DROP TABLE IF EXISTS staging_imm"
staging_dem_table_drop   = "DROP TABLE IF EXISTS staging_dem"
staging_port_table_drop  = "DROP TABLE IF EXISTS staging_port"
staging_temp_table_drop  = "DROP TABLE IF EXISTS staging_temp"
staging_port_codes_table_drop = "DROP TABLE IF EXISTS staging_port_codes"
staging_country_codes_table_drop = "DROP TABLE IF EXISTS staging_country_codes"

dim_visitor_table_drop = "DROP TABLE IF EXISTS dim_visitor"
dim_date_table_drop    = "DROP TABLE IF EXISTS dim_date"
dim_state_table_drop   = "DROP TABLE IF EXISTS dim_state"
dim_port_table_drop    = "DROP TABLE IF EXISTS dim_port"

immigration_table_drop = "DROP TABLE IF EXISTS immigration"

# ------------------------------------------------------------------------------
# CREATE TABLES
# ------------------------------------------------------------------------------
# Staging Tables
# ------------------------------------------------------------------------------
staging_imm_create= (""" CREATE TABLE IF NOT EXISTS staging_imm
                             ( cicid    double precision,
                               i94yr    double precision,
                               i94mon   double precision,
                               i94cit   double precision,
                               i94res   double precision,
                               i94port  varchar,
                               arrdate  double precision,
                               i94mode  double precision,
                               i94addr  varchar,
                               depdate  double precision,
                               i94bir   double precision,
                               i94visa  double precision,
                               count    double precision,
                               dtadfile varchar,
                               visapost varchar,
                               occup    varchar,
                               entdepa  varchar,
                               entdepd  varchar,
                               entdepu  varchar,
                               matflag  varchar,
                               biryear  double precision,
                               dtaddto  varchar,
                               gender   varchar,
                               insnum   varchar,
                               airline  varchar,
                               admnum   double precision,
                               fltno    varchar,
                               visatype varchar);
                     """)

staging_dem_create= (""" CREATE TABLE IF NOT EXISTS staging_dem
                             ( City                varchar,
                               State               varchar,
                               Median_Age          double precision,
                               MalePopulation      numeric,
                               FemalePopulation    numeric,
                               TotalPopulation     int,
                               NumberofVeterans      numeric,
                               Foreignborn           numeric,
                               AverageHouseholdSize  double precision,
                               StateCode             varchar,
                               Race                  varchar,
                               Count                 int);
                     """)

staging_port_create= (""" CREATE TABLE IF NOT EXISTS staging_port
                             ( ident         varchar,
                               type          varchar,
                               name          varchar,
                               elevation_ft  double precision,
                               continent     varchar,
                               iso_country   varchar,
                               iso_region    varchar,
                               municipality  varchar,
                               gps_code      varchar,
                               iata_code     varchar,
                               local_code    varchar,
                               coordinates   varchar);
                     """)

staging_temp_create= (""" CREATE TABLE IF NOT EXISTS staging_temp
                             ( dt  varchar,
                               AverageTemperature              double precision,
                               AverageTemperatureUncertainty   double precision,
                               State       varchar,
                               Country     varchar);
                     """)

staging_port_codes_create= (""" CREATE TABLE IF NOT EXISTS staging_port_codes
                             ( port             varchar,
                               port_name        varchar,
                               state            varchar);
                     """)

staging_country_codes_create= (""" CREATE TABLE IF NOT EXISTS staging_country_codes
                             ( country_code             varchar,
                               country                  varchar);
                     """)
# ------------------------------------------------------------------------------
# Dimension Tables
# ------------------------------------------------------------------------------
dim_visitor_table_create = (""" CREATE TABLE IF NOT EXISTS dim_visitor
                             ( cic_id                   varchar   PRIMARY KEY,
                               citizenship_country      varchar,
                               age                      int,
                               gender                   varchar,
                               arrival_mode             varchar,
                               airline                  varchar,
                               flight_number            varchar
                               )
                               diststyle auto;
                         """)

dim_date_table_create = (""" CREATE TABLE IF NOT EXISTS dim_date
                             ( date date PRIMARY KEY SORTKEY,
                               day     int,
                               week    int,
                               month   int,
                               year    int)
                               diststyle all;
                         """)

dim_state_table_create = (""" CREATE TABLE IF NOT EXISTS dim_state
                             ( sate_code         varchar PRIMARY KEY SORTKEY,
                               state             varchar NOT NULL,
                               male_population   int,
                               female_population int,
                               total_population  int,
                               foreign_born      int,
                               average_household_size      double precision,
                               native_population           int,
                               african_american_population int,
                               hispanic_population         int,
                               asian_population            int,
                               white_population            int,
                               max_temperature            double precision)
                               diststyle all;
                         """)

dim_port_table_create = (""" CREATE TABLE IF NOT EXISTS dim_port
                             ( port_id    varchar  PRIMARY KEY,
                               state      varchar,
                               city       varchar,
                               type       varchar,
                               name       varchar,
                               elevation_ft    numeric)
                               diststyle all;
                         """)

# ------------------------------------------------------------------------------
# Fact Table
# ------------------------------------------------------------------------------
immigration_table_create = (""" CREATE TABLE IF NOT EXISTS immigration
                             ( arrival_id       int      PRIMARY KEY identity(1,1),
                               cic_id           varchar  NOT NULL,
                               arrival_date     date     NOT NULL,
                               arrival_state    varchar  NOT NULL,
                               arrival_port     varchar  NOT NULL,
                               count                     int,

                               FOREIGN KEY (cic_id) REFERENCES dim_visitor(cic_id),
                               FOREIGN KEY (arrival_date) REFERENCES dim_date(date),
                               FOREIGN KEY (arrival_state) REFERENCES dim_state(sate_code),
                               FOREIGN KEY (arrival_port) REFERENCES dim_port(port_id)
                               )
                         """)
# ------------------------------------------------------------------------------
# STAGING TABLES
# ------------------------------------------------------------------------------
staging_imm_copy = (""" COPY staging_imm
FROM '{}' credentials 'aws_iam_role={}' format {};
""").format(config.get('S3','IMM_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            'PARQUET')

staging_dem_copy = (""" COPY staging_dem
FROM '{}' credentials 'aws_iam_role={}' IGNOREHEADER {} DELIMITER '{}';
""").format(config.get('S3','DEM_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            1,
            ';'
            )
staging_port_copy = (""" COPY staging_port
FROM '{}' credentials 'aws_iam_role={}' IGNOREHEADER {} csv;
""").format(config.get('S3','PORT_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            1
            )

staging_temp_copy = (""" COPY staging_temp
FROM '{}' credentials 'aws_iam_role={}' IGNOREHEADER {} DELIMITER '{}';
""").format(config.get('S3','TEMP_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            1,
            ','
            )

staging_port_codes_copy = (""" COPY staging_port_codes
FROM '{}' credentials 'aws_iam_role={}' IGNOREHEADER {} DELIMITER '{}';
""").format(config.get('S3','PORT_CODE_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            1,
            ','
            )

staging_country_codes_copy = (""" COPY staging_country_codes
FROM '{}' credentials 'aws_iam_role={}' IGNOREHEADER {} DELIMITER '{}';
""").format(config.get('S3','COUNTRY_CODE_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            0,
            ';'
            )
# ------------------------------------------------------------------------------
# FINAL TABLES
# ------------------------------------------------------------------------------
date_table_insert = (""" INSERT INTO dim_date(date, day, week, month, year)
        SELECT DISTINCT
            date,
            EXTRACT(day from date)     AS day,
            EXTRACT(week from date)    AS week,
            EXTRACT(month from date)   AS month,
            EXTRACT(year from date)    AS year
        FROM
           (SELECT '1960-1-1'::date + (arrdate * interval '1day') AS date
            FROM staging_imm);
""")

dim_vistor_insert = (""" INSERT INTO dim_visitor(cic_id, citizenship_country,
                                                 age, gender, arrival_mode, airline, flight_number)
                        SELECT DISTINCT
                                       s.cicid,
                                       c.country,
                                       s.i94bir,
                                       s.gender,
                                       s.i94mode,
                                       s.airline,
                                       s.fltno
                        FROM staging_imm s, staging_country_codes c
                        WHERE s.i94cit = c.country_code;
""")

dim_state_insert = (""" INSERT INTO dim_state(sate_code, state, male_population, female_population,
                                              total_population, foreign_born, average_household_size,
                                              native_population, african_american_population, hispanic_population,
                                              asian_population, white_population, max_temperature)
                     SELECT
                           d.StateCode,
                           d.State,
                           d.MalePopulation,
                           d.FemalePopulation,
                           d.TotalPopulation,
                           d.Foreignborn,
                           d.AverageHouseholdSize,
                           n.NativePopulation,
                           b.AfricanAmericanPopulation,
                           h.HispanicPopulation,
                           a.AsianPopulation,
                           w.WhitePopulation,
                           t.MaxTemperature
                     FROM
                         ( SELECT
                             StateCode, State, SUM(MalePopulation) as MalePopulation, SUM(FemalePopulation) as FemalePopulation,
                             SUM(TotalPopulation) as TotalPopulation, SUM(Foreignborn) as Foreignborn,
                             AVG(AverageHouseholdSize) as AverageHouseholdSize
                       FROM staging_dem
                       GROUP BY StateCode, State ) d

                     JOIN (SELECT State, SUM(TotalPopulation) AS NativePopulation
                           FROM staging_dem WHERE Race LIKE 'American Indian and Alaska Native' GROUP BY State) n
                     ON d.State = n.State

                     JOIN (SELECT State, SUM(TotalPopulation) AS  AfricanAmericanPopulation
                           FROM staging_dem WHERE Race LIKE 'Black or African-American' GROUP BY State) b
                    ON d.State = b.State

                     JOIN (SELECT State, SUM(TotalPopulation) AS HispanicPopulation
                           FROM staging_dem WHERE Race LIKE 'Hispanic or Latino' GROUP BY State) h
                    ON d.State = h.State

                     JOIN (SELECT State, SUM(TotalPopulation) AS AsianPopulation
                           FROM staging_dem WHERE Race LIKE 'Asian' GROUP BY State) a
                    ON d.State = a.State

                     JOIN (SELECT State, SUM(TotalPopulation) AS WhitePopulation
                           FROM staging_dem WHERE Race LIKE 'White' GROUP BY State) w
                    ON d.State = w.State

                     JOIN (SELECT State, MAX(AverageTemperature) AS MaxTemperature
                           FROM staging_temp WHERE Country = 'United States'
                           GROUP BY State) t
                    ON d.State = t.State
""")

dim_port_insert = (""" INSERT INTO dim_port (port_id, state, city, type, name, elevation_ft)
                          SELECT DISTINCT
                                c.port,
                                c.state,
                                c.port_name,
                                p.type,
                                p.name,
                                p.elevation_ft
                          FROM staging_port_codes c

                          JOIN (SELECT type, name, elevation_ft, iso_country, municipality,
                                       SPLIT_PART(iso_region, '-', 2) AS state_code
                                 FROM staging_port WHERE iso_country='US') p
                          ON p.state_code= c.state
                          AND UPPER(p.municipality)= UPPER(c.port_name)
                          WHERE c.port IS NOT NULL
""")

imm_insert = (""" INSERT INTO immigration (cic_id, arrival_date, arrival_state, arrival_port, count)
                      SELECT DISTINCT
                                     cicid,
                                     '1960-1-1'::date + (arrdate * interval '1day') AS arrival_date,
                                     i94addr,
                                     i94port,
                                     count
                      FROM staging_imm
                      WHERE i94addr IS NOT NULL;
""")
# ------------------------------------------------------------------------------
# QUERY LISTS
# ------------------------------------------------------------------------------
drop_table_queries = [staging_imm_table_drop, staging_dem_table_drop, staging_port_table_drop, staging_temp_table_drop, staging_port_codes_table_drop,
                      staging_country_codes_table_drop,
                      immigration_table_drop,
                      dim_visitor_table_drop, dim_date_table_drop, dim_state_table_drop, dim_port_table_drop
                      ]

create_table_queries = [staging_imm_create, staging_dem_create, staging_port_create, staging_temp_create, staging_port_codes_create,
                        staging_country_codes_create,
                        dim_visitor_table_create, dim_date_table_create, dim_state_table_create, dim_port_table_create,
                        immigration_table_create
                        ]

copy_table_queries = [staging_imm_copy, staging_dem_copy, staging_port_copy, staging_temp_copy, staging_port_codes_copy, staging_country_codes_copy]

insert_table_queries = [date_table_insert, dim_vistor_insert, dim_state_insert, dim_port_insert, imm_insert]
