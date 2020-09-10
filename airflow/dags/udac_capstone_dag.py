from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator,StageCsvToRedshiftOperator,StageCsv1ToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False
}

dag = DAG('udac_capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_imm_to_redshift = StageToRedshiftOperator(
    task_id='staging_imm',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='sas_data',
    table='staging_imm',
    dag=dag
)

stage_dem_to_redshift = StageCsvToRedshiftOperator(
    task_id='staging_dem',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='us-cities-demographics.csv',
    table='staging_dem',
    ignore_header=1,
    delimiter=';',
    dag=dag
)

stage_port_to_redshift = StageCsv1ToRedshiftOperator(
    task_id='staging_port',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='airport-codes_csv.csv',
    table='staging_port',
    ignore_header=1,
    dag=dag
)

stage_temp_to_redshift = StageCsvToRedshiftOperator(
    task_id='staging_temp',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='GlobalLandTemperaturesByState.csv',
    table='staging_temp',
    ignore_header=1,
    delimiter=',',
    dag=dag
)

stage_port_codes_to_redshift = StageCsvToRedshiftOperator(
    task_id='staging_port_codes',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='i94_airport_codes.csv',
    table='staging_port_codes',
    ignore_header=1,
    delimiter=',',
    dag=dag
)

stage_country_codes_to_redshift = StageCsvToRedshiftOperator(
    task_id='staging_country_codes',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udac-capstone-wk',
    s3_key='i94_Country_Codes.csv',
    table='staging_country_codes',
    ignore_header=0,
    delimiter=';',
    dag=dag
)

load_immigration_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    redshift_conn_id='redshift',
    table='immigration',
    load_sql_stmt=SqlQueries.imm_insert,
    dag=dag
)

load_visitor_dimension_table = LoadDimensionOperator(
    task_id='Load_visitor_dim_table',
    redshift_conn_id='redshift',
    table='dim_visitor',
    truncate_table=True,
    load_sql_stmt=SqlQueries.dim_vistor_insert,
    dag=dag
)

load_state_dimension_table = LoadDimensionOperator(
    task_id='Load_state_dim_table',
    redshift_conn_id='redshift',
    table='dim_state',
    truncate_table=True,
    load_sql_stmt=SqlQueries.dim_state_insert,
    dag=dag
)

load_port_dimension_table = LoadDimensionOperator(
    task_id='Load_port_dim_table',
    redshift_conn_id='redshift',
    table='dim_port',
    truncate_table=True,
    load_sql_stmt=SqlQueries.dim_port_insert,
    dag=dag
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    redshift_conn_id='redshift',
    table='dim_date',
    truncate_table=True,
    load_sql_stmt=SqlQueries.date_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    table=['immigration','dim_visitor','dim_state','dim_port','dim_date'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_imm_to_redshift
start_operator>>stage_dem_to_redshift
start_operator>>stage_port_to_redshift
start_operator>>stage_temp_to_redshift
start_operator>>stage_port_codes_to_redshift
start_operator>>stage_country_codes_to_redshift

stage_imm_to_redshift>>load_immigration_table
stage_dem_to_redshift>>load_immigration_table
stage_port_to_redshift>>load_immigration_table
stage_temp_to_redshift>>load_immigration_table
stage_port_codes_to_redshift>>load_immigration_table
stage_country_codes_to_redshift>>load_immigration_table

load_immigration_table>>load_visitor_dimension_table
load_visitor_dimension_table>>load_date_dimension_table
load_date_dimension_table>>load_state_dimension_table
load_state_dimension_table>>load_port_dimension_table

load_port_dimension_table>>run_quality_checks

run_quality_checks>>end_operator