# Project Title
### Data Engineering Capstone Project

#### Project Summary
In this project we will create a data warehouse then build and configure Apache Airflow to automate and monitor data warehouse ETL pipelines. We will be using **I94 Immigration Data**, **World Temperature Data**, **US Demographic Data**, and **Airport Codes Data** to allow users to make queries to answer the following questions:
- countries where most immigratnts are coming from and which state they are headed to
- relationship between citizenship of the immigrant and the temperature of the destination state
- relationship between citizenship of the immigrant and the demographics of the destination state
- what types of ports are immigrants utilizing the most

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data

### Step 1: Scope the Project and Gather Data

#### Scope
We will first load and explore the data in pandas to understand the datasets before proceeding to creating a dataware house in Redshift.

For the data warehouse we will create an ETL which will load the data from an **Amazon S3** bucket (parquet and csv files) and will create staging tables in **Amazon Redshift**. From there we will be executing SQL statements to create the fact and dimension tables in Redshift based on star schema. Fact table of this schema will comprise of statistics on immigration, whereas dimension tables will include visitor information, date, state information and port information

In the end wel will build and configure an **Apache Airflow** to schedule daily ETL jobs to populate data warehouse on Redshift.

#### Describe and Gather Data

**I94 Immigration Data:**

This data comes from the US National Tourism and Trade Office. A data dictionary is included also in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from.
The dataset includes international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries)

**Airport Code Data:**

This is a simple table of airport codes and corresponding cities. More information [here](https://datahub.io/core/airport-codes#data)

**World Temperature Data:**

This dataset came from Kaggle. More information [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

**U.S. Demographic Data:**

This data comes from OpenSoft. More information [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)


## <span style="color:green"> **I94 Immigration Data**

This data comes from the US National Tourism and Trade Office. A data dictionary is included also in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from.
The dataset includes international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries)


### Data Dictionary
| Feature |	Description |
| --- | --- |
| CICID |Unique record ID |
|I94YR	|4 digit year|
|I94MON	|Numeric month|
|I94CIT	|3 digit Citizenship Country Code|
|I94RES	|3 digit Residence Country Code|
|I94PORT |	Port addmitted through|
|ARRDATE|	Arrival date in the USA|
|I94MODE|	Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)|
|I94ADDR|	US State of arrival|
|DEPDATE|	Departure date from US|
|I94BIR	|Age of Respondent in Years|
|I94VISA|	Visa codes collapsed into three categories: (1 = Business; 2 = Pleasure; 3 = Student)|
|COUNT	|Field used for summary statistics|
|DTADFILE|	Character Date Field|
|VISAPOST|	Department of State where where Visa was issued|
|OCCUP	|Occupation that will be performed in U.S.|
|ENTDEPA|	Arrival Flag. Whether admitted or paroled into the US|
|ENTDEPD|	Departure Flag. Whether departed, lost visa, or deceased|
|ENTDEPU|	Update Flag. Update of visa, either apprehended, overstayed, or updated to PR|
|MATFLAG|	Match flag|
|BIRYEAR|	4 digit year of birth|
|DTADDTO|	Character date field to when admitted in the US|
|GENDER	|Gender|
|INSNUM	|INS number|
|AIRLINE|	Airline used to arrive in U.S.|
|ADMNUM	|Admission number, should be unique and not nullable|
|FLTNO	|Flight number of Airline used to arrive in U.S.|
|VISATYPE|	Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|


## <span style="color:green"> **Airport Code Data**

This is a simple table of airport codes and corresponding cities. More information [here](https://datahub.io/core/airport-codes#data)

### Data Dictionary

| Features |	Description |
| --- | --- |
ident|	Unique identifier
type|	Type of the airport
name|	Airport Name
elevation_ft|	Altitude of the airport
continent|	Continent
iso_country	|ISO code of the country of the airport
iso_region	|ISO code for the region of the airport
municipality|	City where the airport is located
gps_code	|GPS code of the airport
iata_code	|IATA code of the airport
local_code	|Local code of the airport
coordinates|	GPS coordinates of the airport


## <span style="color:green"> World Temperature Data

This dataset came from Kaggle. More information [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

### Data Dictionary
| Feature|	Description |
| --- | --- |
dt|	Date in format YYYY-MM-DD
AverageTemperature|	Average temperature of the city in a given date
AverageTemperature	|Uncertainity in Temperature measurement
State | State for which the temperature is reported
Country	|Country Name

## <span style="color:green"> US Demographics Data

This data comes from OpenSoft. More information [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)


### Data Dictionary
| Feature |	Description |
| --- | --- |
City	|Name of the city
State|	US state of the city
Median Age|	The median of the age of the population
Male Population	|Number of the male population
Female Population|	Number of the female population
Total Population|	Number of the total population
Number of Veterans	|Number of veterans living in the city
Foreign-born|	Number of residents of the city that were not born in the city
Average Household Size|	Average size of the houses in the city
State Code	|Code of the state of the city
Race	|Race class
Count	|Number of individual of each race


## <span style="color:green"> Other Dictionary Data

This is the dictionary data which comes as part of I94 Immigration dataset

### Step 2: Explore and Assess the Data
#### Explore the Data
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps

#### Identify Columns with highest percentage of missing data and exclude the columns with more than 50% of missing data from further analysis

Based on the missing values criteria and the statistics reported above we will ensure that we do not use the columns with more than 50% data in our data warehouse schema because they do not contain enough data to be used in analytics.

Accordingly entdepu, occup, insnum, visapost from i94 immigration dataset, and iata code from airport codes data were identified as columns not to be utilized for data warehouse


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

Below is the star schema model developed for the dataware house design. This Star schema makes it easy to query data to answer questions like
- countries where most immigratnts are coming from and which state they are headed to
- relationship between citizenship of the immigrant and the temperature of the destination state
- relationship between citizenship of the immigrant and the demographics of the destination state
- what types of ports are immigrants utilizing the most


### Fact Table :
#### immigration

| Column |	Type | KEY | DESCRIPTION| SOURCE OF DATA|
| ---    | ---   |---  |---         |--|
arrival_id    |   int   identity(1,1)  | PRIMARY KEY | Unique Row ID | i94 Immigration
cic_id         | varchar  NOT NULL     | FOREIGN KEY | Primary key of dim visitor table| i94 Immigration
arrival_date   | date     NOT NULL     | FOREIGN KEY | Primary key of dim date table| i94 Immigration
arrival_state  | varchar  NOT NULL     | FOREIGN KEY | Primary key of dim state table| i94 Immigration
arrival_port   | varchar  NOT NULL     | FOREIGN KEY | Primary key of dim port table| i94 Immigration
count          |          int          |       -     | Field used for summary statistics| i94 Immigration

### Dimension tables:
#### dim_visitor (cic_id as PRIMARY KEY and DISTSTYLE AUTO)

| Column |	Type | KEY | DESCRIPTION| SOURCE OF DATA|
| ---    | ---   |---  |---         |--|
cic_id| varchar| PRIMARY KEY| Unique ID of each visitor| i94 Immigration
citizenship_country| varchar|-|Citizenship country of visitor| i94 Country Codes
age       |               int |-| Age of visitor| i94 Immigration
gender     |              varchar|-| Gender of visitor| i94 Immigration
arrival_mode|             varchar|-|Arrival mode of visitor ( Air, Sea, Land etc.)| i94 Immigration
airline      |            varchar|-|Airline used to arrive in US| i94 Immigration
flight_number |           varchar|-|Flight numer of airline used to arrive in US| i94 Immigration


#### dim_date (date as PRIMARY KEY and DISTSTYLE ALL since it is a small table)

| Column |	Type | KEY | DESCRIPTION| SOURCE OF DATA|
| ---    | ---   |---  |---         |--|
date |date |PRIMARY KEY SORTKEY |Date| i94 Immigration arrdate converted to date
day   |  int | - | Day | i94 Immigration arrdate converted to day
week  | int| - | Week| i94 Immigration arrdate converted to week
month |int| - | Month| i94 Immigration arrdate converted to month
year  |  int| - | Year| i94 Immigration arrdate converted to year

#### dim_state    (state_code as PRIMARY KEY and DISTSTYLE ALL since it is a small table)

| Column |	Type | KEY | DESCRIPTION| SOURCE OF DATA|
| ---    | ---   |---  |---         |--|
state_code |        varchar | PRIMARY KEY SORTKEY | Unique ID of each State|US Demographics Aggregated for each State
state      |       varchar NOT NULL |-| Name of State|US Demographics Aggregated for each State
male_population |  int|-| Total Male Population in State|US Demographics Aggregated for each State
female_population |int|-| Total Female Population in State|US Demographics Aggregated for each State
total_population  |int|-| Total Population in State|US Demographics Aggregated for each State
foreign_born      |int|-| Total Population on Foreign Born in the State|US Demographics Aggregated for each State
average_household_size|      double precision|-| Average Size of Household in State|US Demographics Aggregated for each State
native_population      |     int|-| Total Size of Native Population in State|US Demographics Aggregated for each State and Native Race
african_american_population |int|-| Total Size of African American Population in State|US Demographics Aggregated for each State and African American Race
hispanic_population         |int|-| Total Size of Hispanic Population in State|US Demographics Aggregated for each State Hispanic Race
asian_population            |int|-| Total Size of Aisan Population in State|US Demographics Aggregated for each State and Asian Race
white_population            |int|-| Total Size of White Population in State|US Demographics Aggregated for each State and White Race
max_temperature            |double precision|-| Maximum recorded Temperature in State|World Temperature Aggregated for each US State

#### dim_port     (port_id as PRIMARY KEY and DISTSTYLE ALL since it is a small table)

| Column |	Type | KEY | DESCRIPTION| SOURCE OF DATA|
| ---    | ---   |---  |---         |--|
port_id   | varchar  |PRIMARY KEY| Unique ID of each Port| i94 port codes
state      |varchar |-| State where port is located| i94 port codes
city       |varchar |-| City where port is located| i94 port codes
type      | varchar|-| type of port| Airport Code
name      | varchar|-| name of port| Airport Code
elevation_ft|    numeric|-| elevation of port|Airport Code

#### 3.2 Mapping Out Data Pipelines

Following steps are needed to pipeline the data into the selected model:

* Create Staging tables in Redshift to store:
 * i94 immigration data
 * airport codes data
 * world temperature data
 * us demographics data
 * airport codes dictionary
 * country codes dictionary


* Create Fact and Dimensions Table in Redshift to store the Star Schema tables defined above


* Load data from S3 to Redshift Staging Tables


* Insert data from Staging Tables into star schema tables defined above using SQL statements


### Step 4: Run Pipelines to Model the Data
#### 4.1 Create the data model
Build the data pipelines to create the data model.

### Creating Data Warehouse and Running a Single Instance of ETL
The following scripts are utilized to create the data warehouse perform a single instance of ETL

#### Run Create Tables Script
%run -i create_tables.py

#### Run Etl.py script
%run -i etl.py

### Automating and Scheduling ETL through Airflow

The **airflow folder** in the project repository contains all the dag and operators scheduled to run daily.
Below is the graph of the dag implemented

![](dag.png)

### Rationale for the choice of tools and technologies for the project.

Used pandas to initially explore the data in python. Pandas library was used due to its ease of use and numerous features when it comes to manipulating and exploring structured data.

Since the input data was structured it was deemed fit to build a data warehouse using Amazon Redshift. Also since Redshift provides a Massive Parallel Processing database, it will allow for an easy and instantaneous scalability for our data warehouse without requiring any management and troubleshooting from the clients side.

Apache Airflow was then used to automate the entire pipeline based on the the required schedule. Airflow was used because it provides a great interface for running and monitoring the ETL jobs.

### How often the data should be updated

Based on the analytic needs the ETL could be run hourly, daily, monthly etc. However, since we are not dealing with real time and neither our goal is to perform streaming analytics we can rely on a daily update of the data.

### Scenario 1: If the data was increased by 100x
An increase in data should not be an issue for Redshift since it is an MPP and an Analytical database optimized for aggregation and read heavy loads. Also since we are using AWS Redshift clusters there should be no issues in increasing the nodes when the need arises.

### Scenario 2: If the data populates a dashboard that must be updated on a daily basis by 7am every day.
A pipeline is already configured using Airflow to handle scheduling. This task will require to simply update the schedule within the dag.

### Scenario 3: If the database needed to be accessed by 100+ people
As mentioned before this could be addressed by simply increasing the number of nodes by utilizing Redshift auto-scaling capabilities.
