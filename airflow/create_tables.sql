CREATE TABLE IF NOT EXISTS staging_imm
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


CREATE TABLE IF NOT EXISTS staging_dem
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


CREATE TABLE IF NOT EXISTS staging_port
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


CREATE TABLE IF NOT EXISTS staging_temp
                          ( dt  varchar,
                            AverageTemperature              double precision,
                            AverageTemperatureUncertainty   double precision,
                            State       varchar,
                            Country     varchar);


CREATE TABLE IF NOT EXISTS staging_port_codes
                             ( port             varchar,
                               port_name        varchar,
                               state            varchar);


CREATE TABLE IF NOT EXISTS staging_country_codes
                            ( country_code             varchar,
                              country                  varchar);


CREATE TABLE IF NOT EXISTS dim_visitor
                           ( cic_id                   varchar   PRIMARY KEY,
                             citizenship_country      varchar,
                             age                      int,
                             gender                   varchar,
                             arrival_mode             varchar,
                             airline                  varchar,
                             flight_number            varchar
                             )
                             diststyle auto;


CREATE TABLE IF NOT EXISTS dim_date
                          ( date date PRIMARY KEY SORTKEY,
                            day     int,
                            week    int,
                            month   int,
                            year    int)
                            diststyle all;


CREATE TABLE IF NOT EXISTS dim_state
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


CREATE TABLE IF NOT EXISTS dim_port
                            ( port_id    varchar  PRIMARY KEY,
                              state      varchar,
                              city       varchar,
                              type       varchar,
                              name       varchar,
                              elevation_ft    numeric)
                              diststyle all;


CREATE TABLE IF NOT EXISTS immigration
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
