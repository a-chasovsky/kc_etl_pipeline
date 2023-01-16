import pandas as pd
import pandahouse as ph
import datetime as dt

from airflow.decorators import dag, task

schedule_interval = '0 23 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_pipeline():

    @task()
    def extract_feed():
        
        query_feed = """
            select
                toDate(time) as event_date,
                user_id as user_id,
                sum(case action when 'view' then 1 else 0 end) as views,
                sum(case action when 'like' then 1 else 0 end) as likes,
                gender as gender,
                age as age,
                os as os
            from 
                *таблица из базы данных №1*
            where 
                toDate(time) = yesterday()
            group by 
                event_date,
                user_id,
                gender, 
                age, 
                os
        """

        df_feed = ph.read_clickhouse(query_feed, connection=connection)
        
        return df_feed

    @task
    def extract_messages():
        
        query_messages = """
            select 
                event_date as event_date,
                user_id,
                messages_received,
                messages_sent,
                users_received,
                users_sent,
                gender as gender,
                age as age,
                os as os
            from 
                (
                select
                    toDate(time) as event_date,
                    user_id as user_id,
                    count(user_id) as messages_sent,
                    uniqExact(reciever_id) as users_sent,
                    gender as gender,
                    age as age,
                    os as os
                from 
                    *таблица из базы данных №2*
                where 
                    toDate(time) = yesterday()
                group by
                    event_date,
                    user_id,
                    gender, 
                    age, 
                    os
                ) t1
            join
                (
                select
                    toDate(time) as event_date,
                    reciever_id as reciever_id,
                    count(reciever_id) as messages_received,
                    uniqExact(user_id) as users_received
                from 
                    simulator_20221120.message_actions
                where 
                    toDate(time) = yesterday()
                group by 
                    event_date,
                    reciever_id
                ) t2
            on t1.user_id = t2.reciever_id
        """

        df_messages = ph.read_clickhouse(query_messages, connection=connection)
        
        return df_messages
    
    @task
    def concat_tables(df1, df2):
        
        df = pd.merge(df1, df2, on=['user_id', 'gender', 'age', 'os', 'event_date'], how='outer')
        
        df = df.fillna(0)
        df[df.describe().columns] = df[df.describe().columns].astype(int)
        
        return df
    
    
    @task
    def gender_agg(df):
        
        features_list = ['event_date', 'gender', 'views', 'likes', 
                         'messages_received', 'messages_sent', 
                         'users_received', 'users_sent']
        
        df_gender = df[features_list].groupby(['gender', 'event_date']) \
                                        .sum() \
                                        .reset_index() \
                                        .rename(columns={'gender': 'dimension_value'})
        
        df_gender['dimension'] = 'gender'
        
        cols = list(df_gender)
        cols.insert(0, cols.pop(cols.index('dimension')))
        df_gender = df_gender.loc[:, cols]
        cols.insert(0, cols.pop(cols.index('event_date')))
        df_gender = df_gender.loc[:, cols]
        
        return df_gender
    
    
    @task
    def age_agg(df):
        
        features_list = ['event_date', 'age', 'views', 'likes', 
                         'messages_received', 'messages_sent', 
                         'users_received', 'users_sent']
        
        df_age = df[features_list].groupby(['age', 'event_date']) \
                                    .sum() \
                                    .reset_index() \
                                    .rename(columns={'age': 'dimension_value'})
        
        df_age['dimension'] = 'age'
        
        cols = list(df_age)
        cols.insert(0, cols.pop(cols.index('dimension')))
        df_age = df_age.loc[:, cols]
        cols.insert(0, cols.pop(cols.index('event_date')))
        df_age = df_age.loc[:, cols]
        
        return df_age


    @task
    def os_agg(df):

        features_list = ['event_date', 'os', 'views', 'likes', 
                         'messages_received', 'messages_sent', 
                         'users_received', 'users_sent']
        
        df_os = df[features_list].groupby(['os', 'event_date']) \
                                    .sum() \
                                    .reset_index() \
                                    .rename(columns={'os': 'dimension_value'})
        
        df_os['dimension'] = 'os'
        
        cols = list(df_os)
        cols.insert(0, cols.pop(cols.index('dimension')))
        df_os = df_os.loc[:, cols]
        cols.insert(0, cols.pop(cols.index('event_date')))
        df_os = df_os.loc[:, cols]
        
        return df_os


    @task
    def df_concat(df1, df2, df3):
        
        df_concat = pd.concat([df1, df2, df3], axis=0)
        
        return df_concat


    @task
    def load(df):
        
        query_create_table = """
            create table if not exists schema.table (
                event_date Date,
                dimension varchar(50),
                dimension_value varchar(50),
                views int,
                likes int,
                messages_received int,
                messages_sent int,
                users_received int,
                users_sent int
            ) engine = Log()
        """

        ph.execute(query_create_table, connection=connection_test)
        ph.to_clickhouse(df, 'table', index=False, connection=connection_test)


    feed_df = extract_feed()
    messages_df = extract_messages()
    df = concat_tables(feed_df, messages_df)
    
    gender_df = gender_agg(df)
    age_df = age_agg(df)
    os_df = os_agg(df)
    
    df_final = df_concat(gender_df, age_df, os_df)

    load(df_final)


etl_pipeline = etl_pipeline()