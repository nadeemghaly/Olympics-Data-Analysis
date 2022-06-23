# step 1 - import modules
import requests
import json
import os
import numpy as np

from airflow import DAG
from datetime import datetime
from datetime import date

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

import pandas as pd

# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 2)
    }


# step 3 - instantiate DAG
dag = DAG(
    'mileStone1-DAG',
    default_args=default_args,
    description='Fetch covid data from API',
    schedule_interval='@once',
)



# step 4 Define task
def extract_data(**kwargs):
    df_olympic_history = pd.read_csv("dags/athlete_events.csv")
    df_noc_regions= pd.read_csv('dags/noc_regions.csv')
    df_continents= pd.read_csv('dags/continents2.csv')
    return df_olympic_history,df_noc_regions,df_continents

    
def transform_data(**context):
    df_olympic_history,df_noc_regions,df_continents = context['task_instance'].xcom_pull(task_ids='extract_data')

    
    df_olympic_history.replace('NA', np.nan, inplace=True)
    df_olympic_history.replace('Missing', np.nan, inplace=True)
    df_olympic_history.replace('NaN', np.nan, inplace=True)
    Q1=df_olympic_history.Age.quantile(0.25)
    Q3=df_olympic_history.Age.quantile(0.75)
    IQR=Q3-Q1
    lowqe_bound=Q1 - 1.5 * IQR
    upper_bound=Q3 + 1.5 * IQR
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Age'] < lowqe_bound].index, inplace = True)
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Age'] > upper_bound].index, inplace = True)
    df_olympic_history['Age'] = df_olympic_history.groupby(['Sex','Sport'])['Age'].transform(lambda x: x.fillna(x.mean()))
    
    Q1w=df_olympic_history.Weight.quantile(0.25)
    Q3w=df_olympic_history.Weight.quantile(0.75)
    IQRw=Q3w-Q1w
    lowqe_boundw=Q1w - 1.5 * IQRw
    upper_boundw=Q3w + 1.5 * IQRw
    
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Weight'] < lowqe_boundw].index, inplace = True)
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Weight'] > upper_boundw].index, inplace = True)
    
    df_olympic_history['Weight'] = df_olympic_history.groupby(['Sex','Sport'])['Weight'].transform(lambda x: x.fillna(x.mean()))
    df_olympic_history['Weight'] = df_olympic_history.groupby(['Sex'])['Weight'].transform(lambda x: x.fillna(x.mean()))
    
    
    Q1h=df_olympic_history.Height.quantile(0.25)
    Q3h=df_olympic_history.Height.quantile(0.75)
    IQRh=Q3h-Q1h
    lowqe_boundH=Q1h- 1.5 * IQRh
    upper_boundH=Q3h + 1.5 * IQRh
    
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Height'] < lowqe_boundH].index, inplace = True)
    df_olympic_history.drop(df_olympic_history[df_olympic_history['Height'] > upper_boundH].index, inplace = True)
    
    
    df_olympic_history['Height'] = df_olympic_history.groupby(['Sex','Sport'])['Height'].transform(lambda x: x.fillna(x.mean()))
    df_olympic_history['Height'] = df_olympic_history.groupby(['Sex'])['Height'].transform(lambda x: x.fillna(x.mean()))
    
    
    df_olympic_history['Medal'].replace(np.nan, value = "No Medal Received", inplace= True)
    
    df_olympic_history['BMI'] = (df_olympic_history['Weight'])/((df_olympic_history['Height']/100)**2)
    
    df_noc_regions.dropna(inplace=True)
    df_noc_regions= df_noc_regions.rename(columns={'region':'NewTeam','notes':'Team'})
    df_noc_regions = df_noc_regions[['NewTeam', 'Team']]
    
    df_olympic_history = pd.merge(df_olympic_history , df_noc_regions, how ='left')
    df_olympic_history['NewTeam'].fillna(df_olympic_history['Team'], inplace=True)
    df_olympic_history.drop('Team', inplace=True, axis=1)
    df_olympic_history= df_olympic_history.rename(columns={'NewTeam':'Team'})
    
    df_olympic_history.loc[df_olympic_history['Team'] == 'Congo (Brazzaville)', 'Team'] = 'Congo'
    df_olympic_history.loc[df_olympic_history['Team'] == 'West Germany', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'East Germany', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Czechoslovakia', 'Team'] = 'Czech Republic'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Czechoslovakia-1', 'Team'] = 'Czech Republic'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Czechoslovakia-2', 'Team'] = 'Czech Republic'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Czechoslovakia-3', 'Team'] = 'Czech Republic'
    df_olympic_history.loc[df_olympic_history['Team'] == 'United States Virgin Islands-2', 'Team'] = 'Virgin Islands (U.S.)'
    df_olympic_history.loc[df_olympic_history['Team'] == 'United States Virgin Islands', 'Team'] = 'Virgin Islands (U.S.)'
    df_olympic_history.loc[df_olympic_history['Team'] == 'British Virgin Islands', 'Team'] = 'Virgin Islands (British)'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Palestine', 'Team'] = 'Palestine, State of'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Burevestnik', 'Team'] = 'Russia'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Malaya', 'Team'] = 'Malaysia'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Brunei', 'Team'] = 'Brunei Darussalam'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Curacao', 'Team'] = 'Curaçao' 
    df_olympic_history.loc[df_olympic_history['Team'] == 'Rhodesia', 'Team'] = 'Zimbabwe' 
    df_olympic_history.loc[df_olympic_history['Team'] == 'Saar', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Antigua', 'Team'] = 'Antigua and Barbuda'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Chinese Taipei-1', 'Team'] = 'China'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Chinese Taipei-2', 'Team'] = 'China'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Chinese Taipei-3', 'Team'] = 'China'
    df_olympic_history.loc[df_olympic_history['Team'] == 'Trinidad', 'Team'] = 'Trinidad and Tobago'
    df_olympic_history.loc[df_olympic_history['Team'] == 'East Germany-3', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'West Germany-3', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'West Germany-2', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'East Germany-2', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'East Germany-1', 'Team'] = 'Germany'
    df_olympic_history.loc[df_olympic_history['Team'] == 'West Germany-1', 'Team'] = 'Germany'
    df_continents= df_continents.rename(columns={'alpha-3':'NOC','name':'Team'})
    df_continents_one = df_continents[['NOC', 'region']]

    
    df_continents_two = df_continents[['Team', 'region']]
    df_continents_two=df_continents_two.rename(columns={'region':'region1'})
    
    df_olympic_history1 = pd.merge(df_olympic_history,df_continents_one , how ='left')

    df_olympic_history2 = pd.merge(df_olympic_history1, df_continents_two, how ='left')
    df_olympic_history2['region'].fillna(df_olympic_history2['region1'], inplace=True)
    df_olympic_history2.drop(['region1'], axis=1,inplace=True)
    df_olympic_history=df_olympic_history2
    
    df_olympic_history_trial=df_olympic_history[['NOC','region']]
    df_olympic_history_trial.drop_duplicates(inplace=True)
    
    
    df_olympic_history_trial.dropna(inplace=True)
    df_olympic_history_trial= df_olympic_history_trial.rename(columns={'region':'region1'})
    df_olympic_history1 = pd.merge(df_olympic_history,df_olympic_history_trial , how ='left')
    df_olympic_history1['region'].fillna(df_olympic_history1['region1'], inplace=True)
    df_olympic_history1.drop(['region1'], axis=1,inplace=True)
    df_olympic_history=df_olympic_history1
    df_olympic_history= df_olympic_history.rename(columns={'region':'Continent'})
    return df_olympic_history

def load_data(**context):
    df_olympic_history= context['task_instance'].xcom_pull(task_ids='transform_data')
    print(df_olympic_history)
    df_olympic_history.to_csv("dags/final_dataset.csv",header=False,index=False,quoting=1)

t1 = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform_data,
    dag=dag,
)
t3 = PythonOperator(
    task_id='load_data',
    provide_context=True,
    python_callable=load_data,
    dag=dag,
)






t1>>t2>>t3