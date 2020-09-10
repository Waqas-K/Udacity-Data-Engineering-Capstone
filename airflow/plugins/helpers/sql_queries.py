class SqlQueries:

    date_table_insert = (""" (date, day, week, month, year)
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

    dim_vistor_insert = (""" (cic_id, citizenship_country,age, gender, arrival_mode, airline, flight_number)
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

    dim_state_insert = (""" (sate_code, state, male_population, female_population,
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

    dim_port_insert = (""" (port_id, state, city, type, name, elevation_ft)
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

    imm_insert = (""" (cic_id, arrival_date, arrival_state, arrival_port, count) 
                            SELECT DISTINCT
                                         cicid,
                                         '1960-1-1'::date + (arrdate * interval '1day') AS arrival_date,
                                         i94addr,
                                         i94port,
                                         count
                          FROM staging_imm
                          WHERE i94addr IS NOT NULL;
    """)
