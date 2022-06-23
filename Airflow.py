# -*- coding: utf-8 -*-
"""Copy of cleaning.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1rcP29yhCXe77nbbsgk96-feUqsD06H0Q
"""

import os
from airflow.models.baseoperator import chain
import urllib.parse
import json
import http.client
from textblob import TextBlob
import csv
import time
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow import DAG
import airflow
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import missingno as msno
from matplotlib.pyplot import *

# LOF
from sklearn.neighbors import LocalOutlierFactor


def load_csv(**kwargs):
    run_id = kwargs["dag_run"].run_id
    olympicRecords = pd.read_csv(
        "https://raw.githubusercontent.com/Abzokhattab/120-years-of-Olympic-History-Analysis/main/athlete_events.csv", index_col=0)
    regions = pd.read_csv(
        "https://raw.githubusercontent.com/Abzokhattab/120-years-of-Olympic-History-Analysis/main/noc_regions.csv", index_col=0)

    olympicRecords.head()

    regions.head()

    """# Data Exploration

        Dataset exploration (count, mean, min, max, outliers, missing-values)
        """

    olympicRecords.shape

    olympicRecords.info()

    olympicRecords.describe()

    print(olympicRecords.isnull().sum())

    """The following bar chart shows which columns has missing values 
        as we can notice, the `Age`, `Height`,`Weight` and `Medal` columns have missing  values 
        """

    msno.bar(olympicRecords)

    """Also as shown in the bar char, the `Medal` column has a lot of missing values <br> so lets explore all columns that have missing values and plot then before and after the data cleaning process

        **`Distribution of  medals  across the dataset`**
        """

    print(olympicRecords.Medal.value_counts())
    print(olympicRecords.Medal.unique())

    """- Although the medal column contains NAN values, they do not appear in the graph, so we will hardcode these values later in this notebook by converting them from "NA" to "non" in the handling missing values section.
        - We also notice that the counts for all three medal types are similar (balanced)
        """

    plt.title(
        "Distribution of  medals  across the dataset before handling missing values")
    sns.countplot(x="Medal", data=olympicRecords)
    sns.despine()

    """For continuous variables, we need to understand the central tendency and spread of the variable. Univariate analysis is also used to highlight missing and outlier values.

        The graphs below show the distribution of age, height, and weight across the dataset, as well as the outlier values in the dataset, as shown in the boxplot.
        """

    sns.set(rc={'figure.figsize': (10, 9)})
    fig, ax = plt.subplots(1, 2)
    sns.distplot(olympicRecords["Age"],  ax=ax[0])
    sns.boxplot(olympicRecords["Age"],  ax=ax[1])
    fig.show()

    plt.figure(figsize=(10, 5), dpi=130)
    fig, ax = plt.subplots(1, 2)
    sns.distplot(olympicRecords["Height"],  ax=ax[0])
    sns.boxplot(olympicRecords["Height"],  ax=ax[1])
    fig.show()

    fig, ax = plt.subplots(1, 2)
    plt.figure(figsize=(10, 5), dpi=130)
    sns.distplot(olympicRecords["Weight"],  ax=ax[0])
    sns.boxplot(olympicRecords["Weight"],  ax=ax[1])
    fig.show()
    # reset size
    sns.reset_orig

    """We know that the height, age, and weight have outliers and missing values, so let's see if there is any relationship between vars before we deal with them.

        Visualize the correlation
        """

    fig = plt.figure(figsize=(8, 5), dpi=130)
    sns.heatmap(olympicRecords.corr(), annot=True)
    """the previous heatmap chart show a strong relationship between the height and the weight which means we can predict height's missing values depending on the weight value"""

    regions.to_csv(f"load_csv_regions_{run_id}.csv")
    olympicRecords.to_csv(f"load_csv_olympicRecords_{run_id}.csv")
    print("file saved in: ", os.getcwdb())


def data_cleaning(**kwargs):
    run_id = kwargs["dag_run"].run_id
    regions = pd.read_csv(f"load_csv_regions_{run_id}.csv")
    olympicRecords = pd.read_csv(
        f"load_csv_olympicRecords_{run_id}.csv")

    # Cleaning the Data
    """
        The notes column will be removed, because the notes column is irrelevant to our analysis.
        """

    regions.drop('notes', axis=1, inplace=True)
    regions.rename(columns={'region': 'Country'}, inplace=True)

    regions.head()

    """### Handling missing values

        **We  start by handling the missing values from the medal, weight, height and age attributes **

        By the above values, We can find that Age, Height, Weight and Medals have lot of missing values. The medal column have 231333 missing values. This is fine because not all the participants win a medal. So we will replace this values with `non`
        """

    olympicRecords['Medal'].fillna('non', inplace=True)

    print(olympicRecords.isnull().sum())

    """**here we checked if every NOC value is mapped to only a single team as it should be**"""

    print(olympicRecords.loc[:, ['NOC', 'Team']].drop_duplicates()[
        'NOC'].value_counts().head())

    """**we found that this is not the case and will try next to make every Noc value mapped to only one team by merging this data set with the regions dataset with NOC as the primary key**"""

    olympics_merge = olympicRecords.merge(regions,
                                          left_on='NOC',
                                          right_on='NOC',
                                          how='left')

    print(olympicRecords.loc[:, ['NOC', 'Team']].drop_duplicates()[
        'NOC'].value_counts().head())

    olympics_merge.loc[olympics_merge['Country'].isnull(), [
        'NOC', 'Team']].drop_duplicates()

    """**after checking we found some countries without NOC so will put it manually**"""

    olympics_merge['Country'] = np.where(
        olympics_merge['NOC'] == 'SGP', 'Singapore', olympics_merge['Country'])
    olympics_merge['Country'] = np.where(
        olympics_merge['NOC'] == 'ROT', 'Refugee Olympic Athletes', olympics_merge['Country'])
    olympics_merge['Country'] = np.where(
        olympics_merge['NOC'] == 'UNK', 'Unknown', olympics_merge['Country'])
    olympics_merge['Country'] = np.where(
        olympics_merge['NOC'] == 'TUV', 'Tuvalu', olympics_merge['Country'])

    olympics_merge.loc[olympics_merge['Country'].isnull(), [
        'NOC', 'Team']].drop_duplicates()

    print(olympics_merge.loc[:, ['NOC', 'Country']
                             ].drop_duplicates()['NOC'].value_counts().head())

    olympics_merge.head()

    """now dropping the old team attribute and replace it with the new one"""

    olympics_merge.drop('Team', axis=1, inplace=True)
    olympics_merge.rename(columns={'Country': 'Team'}, inplace=True)

    print(olympics_merge.loc[:, ['NOC', 'Team']].drop_duplicates()[
        'NOC'].value_counts().head())

    olympics_merge.head()

    """Now we will add a new column that represents the hosting country as we think it will be very important for answering our upcoming questions in the next milestones """

    olympics_merge[['Year', 'City']].drop_duplicates().sort_values('Year')

    """since we only have the hosting cities we will add the hosting countries manually"""

    country_dict = {'Athina': 'Greece',
                    'Paris': 'France',
                    'St. Louis': 'USA',
                    'London': 'UK',
                    'Stockholm': "Sweden",
                    'Antwerpen': 'Belgium',
                    'Amsterdam': 'Netherlands',
                    'Los Angeles': 'USA',
                    'Berlin': 'Germany',
                    'Helsinki': 'Finland',
                    'Melbourne': 'Australia',
                    'Roma': 'Italy',
                    'Tokyo': 'Japan',
                    'Mexico City': 'Mexico',
                    'Munich': 'Germany',
                    'Montreal': 'Canada',
                    'Moskva': 'Russia',
                    'Seoul': 'South Korea',
                    'Barcelona': 'Spain',
                    'Atlanta': 'USA',
                    'Sydney': 'Australia',
                    'Beijing': 'China',
                    'Rio de Janeiro': 'Brazil'}

    olympics_merge['Host_Country'] = olympics_merge['City'].map(country_dict)
    olympics_merge.head()

    print(olympics_merge.isnull().sum())

    """we missed some cities so wi add them one by one """

    olympics_merge.loc[olympics_merge['Host_Country'].isnull(), [
        'City']].drop_duplicates()

    country_dict = {'Athina': 'Greece',
                    'Paris': 'France',
                    'St. Louis': 'USA',
                    'London': 'UK',
                    'Stockholm': "Sweden",
                    'Antwerpen': 'Belgium',
                    'Amsterdam': 'Netherlands',
                    'Los Angeles': 'USA',
                    'Berlin': 'Germany',
                    'Helsinki': 'Finland',
                    'Melbourne': 'Australia',
                    'Roma': 'Italy',
                    'Tokyo': 'Japan',
                    'Mexico City': 'Mexico',
                    'Munich': 'Germany',
                    'Montreal': 'Canada',
                    'Moskva': 'Russia',
                    'Seoul': 'South Korea',
                    'Barcelona': 'Spain',
                    'Atlanta': 'USA',
                    'Sydney': 'Australia',
                    'Beijing': 'China',
                    'Rio de Janeiro': 'Brazil',
                    'Calgary': 'Canada',
                    'Albertville': 'France',
                    'Oslo': 'Norway',
                    'Lillehammer': 'Norway',
                    'Salt Lake City': 'USA',
                    'Lake Placid': 'USA',
                    'Sochi': 'Russia',
                    'Nagano': 'Japan',
                    'Torino': 'Italy',
                    'Squaw Valley': 'USA',
                    'Innsbruck': 'Austria',
                    'Sarajevo': 'Bosnia and Herzegovina',
                    "Cortina d'Ampezzo": 'Italy',
                    'Vancouver': 'Canada',
                    'Grenoble': 'France',
                    'Sapporo': 'Japan',
                    'Chamonix': 'France',
                    'Sankt Moritz': 'Switzerland',
                    'Garmisch-Partenkirchen': 'Germany'}

    olympics_merge['Host_Country'] = olympics_merge['City'].map(country_dict)
    olympics_merge.head()

    print(olympics_merge.isnull().sum())

    """Imputing the missing values of Age, Height and Weight with the respective means for males and females as it is not realistic for males and females to have the same mean for height and weight for example """

    olympics_merge.groupby(["Sex"])["Height"].mean()

    olympics_merge['Height'] = olympics_merge['Height'].fillna(
        olympics_merge['Sex'].map({'M': 178.858463, 'F': 167.839740}))

    print(olympics_merge.isnull().sum())

    olympics_merge.groupby(["Sex"])["Age"].mean()

    olympics_merge['Age'] = olympics_merge['Age'].fillna(
        olympics_merge['Sex'].map({'M': 26.277562, 'F': 23.732881}))

    print(olympics_merge.isnull().sum())

    olympics_merge.groupby(["Sex"])["Weight"].mean()

    olympics_merge['Weight'] = olympics_merge['Weight'].fillna(
        olympics_merge['Sex'].map({'M': 75.743677, 'F': 60.021252}))

    print(olympics_merge.isnull().sum())

    olympics_merge.head(20)

    """### Handling outliers

        ####Height & Weight

        Now that our data is clean and have no missing values we next have to handle the outliers

        So when we think that we cant just drop the outliers from the height and the weight directly
        we think we should treat this case as a multivariate outlier and consider only the unrealistic weight values with respect to their height
        """

    plt.figure(figsize=(12, 12))
    sns.regplot(olympics_merge['Height'], olympics_merge['Weight'])

    clf = LocalOutlierFactor()

    X = olympics_merge[['Height', 'Weight']].values
    y_pred = clf.fit_predict(X)

    """Now we plot the height and weight after predicting the outliers usin Local outlier factor and the red dots presents the outliers detected """

    plt.figure(figsize=(12, 12))
    # plot the level sets of the decision function

    in_mask = [True if l == 1 else False for l in y_pred]
    out_mask = [True if l == -1 else False for l in y_pred]

    plt.title("Local Outlier Factor (LOF)")
    # inliers
    a = plt.scatter(X[in_mask, 0], X[in_mask, 1], c='blue',
                    edgecolor='k', s=30)
    # outliers
    b = plt.scatter(X[out_mask, 0], X[out_mask, 1], c='red',
                    edgecolor='k', s=30)
    plt.axis('tight')
    plt.xlabel('Height')
    plt.ylabel('Weight')
    plt.show()

    """As we see the LOF was able to detect real outliers even within the normal range which would not possible to detect if we  treated the height and weight seperatly """

    len(y_pred)

    len(X[in_mask])

    len(X[out_mask])

    olympics_merge['outlier'] = y_pred

    olympics_merge.head(10)

    """Dropping outliers for values greater than `0`"""

    olympics_merge_outliersDropped = olympics_merge.loc[(
        olympics_merge["outlier"] > 0)]

    olympics_merge_outliersDropped.drop(columns="outlier", inplace=True)

    """

        ```
        # This is formatted as code
        ```

        now we plot the height and weight after removing the outliers
        """

    plt.figure(figsize=(12, 12))
    sns.regplot(olympics_merge_outliersDropped['Height'],
                olympics_merge_outliersDropped['Weight'])

    """we see that now we have less outliers than before the LOF """

    olympics_merge_outliersDropped.shape

    """####Age

        The age values range from `[10 to 97]` as shown in the boxplot figure of the outlier in the visualtization before cleaning section. So, because all of these values are real, we can't get rid of any of them, so we've decided to keep all age outliers.
        """

    olympics_merge_outliersDropped.to_csv(
        f"data_cleaning_{run_id}.csv")
    print("file saved in: ", os.getcwdb())


def data_integration(**kwargs):
    """
    # Data integration
    Previously in milestone 1 we did already merge the dataset NOC regions and then we did the first feature Engineering by creating the column 'Host_Country'.
    So now we will merge the additional data specificly 2 more datasets one that is the GDP for each country across the years and the other dataset have information about the population of the countries, the 2 new datasets are very useful for us to answer our research question
    """

    run_id = kwargs["dag_run"].run_id

    olympics_merge_outliersDropped = pd.read_csv(
        f"data_cleaning_{run_id}.csv")

    w_gdp = pd.read_csv(
        'https://raw.githubusercontent.com/abzokhattab/120-years-of-Olympic-History-Analysis/main/world_gdp.csv', skiprows=3)

    w_gdp.drop(['Indicator Name', 'Indicator Code'], axis=1, inplace=True)

    w_gdp.head()

    w_gdp = pd.melt(w_gdp, id_vars=[
                    'Country Name', 'Country Code'], var_name='Year', value_name='GDP')

    w_gdp['Year'] = pd.to_numeric(w_gdp['Year'])

    w_gdp.head()

    len(list(set(olympics_merge_outliersDropped['NOC'].unique(
    )) - set(w_gdp['Country Code'].unique())))

    len(list(set(olympics_merge_outliersDropped['Team'].unique(
    )) - set(w_gdp['Country Name'].unique())))

    # Merge to get country code
    olympics_merge_ccode = olympics_merge_outliersDropped.merge(w_gdp[['Country Name', 'Country Code']].drop_duplicates(),
                                                                left_on='Team',
                                                                right_on='Country Name',
                                                                how='left')

    olympics_merge_ccode.drop('Country Name', axis=1, inplace=True)

    # Merge to get gdp too
    olympics_merge_gdp = olympics_merge_ccode.merge(w_gdp,
                                                    left_on=[
                                                        'Country Code', 'Year'],
                                                    right_on=[
                                                        'Country Code', 'Year'],
                                                    how='left')

    olympics_merge_gdp.drop('Country Name', axis=1, inplace=True)

    w_pop = pd.read_csv(
        'https://raw.githubusercontent.com/abzokhattab/120-years-of-Olympic-History-Analysis/main/world_pop.csv')

    w_pop.drop(['Indicator Name', 'Indicator Code'], axis=1, inplace=True)

    w_pop = pd.melt(w_pop, id_vars=['Country', 'Country Code'],
                    var_name='Year', value_name='Population')

    # Change the Year to integer type
    w_pop['Year'] = pd.to_numeric(w_pop['Year'])

    w_pop.head()

    olympics_final = olympics_merge_gdp.merge(w_pop,
                                              left_on=['Country Code', 'Year'],
                                              right_on=[
                                                  'Country Code', 'Year'],
                                              how='left')

    olympics_final.drop('Country', axis=1, inplace=True)

    olympics_final.head()

    olympics_final.to_csv(f"data_integration_{run_id}.csv")
    print("file saved in: ", os.getcwdb())


def feature_engineering(**kwargs):
    """
    #Feature Engineering

    Now for further analysis we decided to create a new feature represents if in this row there is a medal won or not because this will help us figure the medals won for each team """

    run_id = kwargs["dag_run"].run_id

    olympics_final = pd.read_csv(f"data_integration_{run_id}.csv")

    olympics_final['Medal_Won'] = np.where(
        olympics_final.loc[:, 'Medal'] == 'non', 0, 1)

    olympics_final.head()

    """But this this new column is only saying if the athelete won a medal or not so if the event is a team event it will consider many incorrect medals for the medals won for the country as it is supposed to be considered a 1 medal for the country because it  is a team event for example like football each player hold 1 medal meaning a more than 11 medals for the whole team which is wrong the whole country only won a gold medal in football event so lets fix this """

    # Check whether number of medals won in a year for an event by a team exceeds 1. This indicates a team event.
    identify_team_events = pd.pivot_table(olympics_final,
                                          index=['Team', 'Year', 'Event'],
                                          columns='Medal',
                                          values='Medal_Won',
                                          aggfunc='sum',
                                          fill_value=0).drop('non', axis=1).reset_index()

    identify_team_events = identify_team_events.loc[identify_team_events['Gold'] > 1, :]

    team_sports = identify_team_events['Event'].unique()

    team_sports

    """After some research we found some events that are not really team events but our algorithm will identify them as so, so we will remove them manually"""

    remove_sports = ["Gymnastics Women's Balance Beam", "Gymnastics Men's Horizontal Bar",
                     "Swimming Women's 100 metres Freestyle", "Swimming Men's 50 metres Freestyle"]

    team_sports = list(set(team_sports) - set(remove_sports))

    """Lets create a mask that identify wether the event is a team or single event """

    # if an event name matches with one in team sports, then it is a team event. Others are singles events.
    team_event_mask = olympics_final['Event'].map(lambda x: x in team_sports)
    single_event_mask = [not i for i in team_event_mask]

    # rows where medal_won is 1
    medal_mask = olympics_final['Medal_Won'] == 1

    # Put 1 under team event if medal is won and event in team event list
    olympics_final['Team_Event'] = np.where(team_event_mask & medal_mask, 1, 0)

    # Put 1 under singles event if medal is won and event not in team event list
    olympics_final['Single_Event'] = np.where(
        single_event_mask & medal_mask, 1, 0)

    # Add an identifier for team/single event
    olympics_final['Event_Category'] = olympics_final['Single_Event'] + \
        olympics_final['Team_Event']

    olympics_final.head()

    """As another feature we will add a new column that we will use later in answering the upcoming questions the new feature (column) is GDP per capita which is simply the ratio between how rich the country is to it's population """

    olympics_final["GDP/Capita"] = olympics_final["GDP"] / \
        olympics_final["Population"]

    olympics_final.head()

    olympics_final.to_csv(f"feature_engineering_{run_id}.csv")

    print("file saved in: ", os.getcwdb())


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'data_engineering',
    default_args=default_args,
    description='A simple tutorial DAG',
    start_date=datetime.now(),
    schedule_interval="@daily",
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='load_csv',
        provide_context=True,
        python_callable=load_csv,
        dag=dag)

    t2 = PythonOperator(
        task_id='data_cleaning',
        provide_context=True,
        python_callable=data_cleaning,
        dag=dag)

    t3 = PythonOperator(
        task_id='data_integration',
        provide_context=True,
        python_callable=data_integration,
        dag=dag)

    t4 = PythonOperator(
        task_id='feature_engineering',
        provide_context=True,
        python_callable=feature_engineering,
        dag=dag)

    t1 >> t2 >> t3 >> t4
