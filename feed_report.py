# importing libraries
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import requests
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
from datetime import date
from airflow.decorators import dag, task

chat_id = -802518328
my_token = '5505931949:AAEmb_WfLfF0ydF5327CaJjU932_DLNckKY'
bot = telegram.Bot(token=my_token) 

# connecting to db
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# standard arguments for tasks
default_args = {
    'owner': 'val_ks',
    'depends_on_post': False, 
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25),
}
@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False, )
def dag_val_ks_task7_1():
    @task
    def get_yesterday_feed():
        yesterday_feed_query = """select user_id, toDate(time) as event_date,
                                countIf(action='like') as likes,
                                countIf(action = 'view') as views 
                                from simulator_20230420.feed_actions
                                where toDate(time)= today()-1
                                group by event_date, user_id 
                                format TSVWithNames"""
        yesterday_feed = ch_get_df(query=yesterday_feed_query)
        return yesterday_feed
    
    @task
    def get_dau(yesterday_feed):
        dau = yesterday_feed.user_id.nunique()
        return dau

    @task
    def get_likes(yesterday_feed):
        likes = yesterday_feed.likes.sum()
        return likes

    @task
    def get_views(yesterday_feed):
        views = yesterday_feed.views.sum()
        return views

    @task
    def get_ctr(likes, views):
        if views != 0:
            ctr = (likes / views).round(2)
        else:
            ctr = 0
        return ctr
    
    @task
    def get_weekly_feed():
        weekly_query = """SELECT user_id, toDate(time) date, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230420.feed_actions
                WHERE toDate(time) < today() and toDate(time) >= today()-7
                GROUP BY date, user_id
                order by date asc
                format TSVWithNames"""
        weekly_feed = ch_get_df(query=weekly_query)
        return weekly_feed

    @task
    def send_report(dau, likes, views, ctr, weekly_feed):
        print('Trying to send message...')
        bot.send_message(chat_id, f'''Hello! This is daily report:
DAU: {dau}
Likes: {likes}
Views: {views}
CTR: {ctr}

Metrics for last 7 days:''')
        print('The message was sent.')
        
        graphs = []

        def append_graphs(data, y_axis, title):
            plt.figure()
            sns.lineplot(x='date', y=y_axis, data=data)
            plt.xticks(rotation=20)
            plt.title(title)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = title + '.png'
            plt.close()
            graphs.append(telegram.InputMediaPhoto(plot_object))

        print('Trying to fill graphs group...')
        append_graphs(weekly_feed.groupby('date').views.sum().reset_index(), 'views', 'Views')
        append_graphs(weekly_feed.groupby('date').likes.sum().reset_index(), 'likes', 'Likes')
        append_graphs(
            weekly_feed.groupby('date').user_id.nunique().reset_index().rename(columns={'user_id': 'users_nunique'}),
            'users_nunique', 'DAU')
                  
        likes_views_week = weekly_feed[['date', 'likes', 'views']].groupby('date').sum().reset_index()
        likes_views_week['ctr'] = (likes_views_week['likes'] / likes_views_week['views']).round(2)
        append_graphs(likes_views_week, 'ctr', 'CTR')
        print('trying to send graphs group')
        bot.send_media_group(chat_id, graphs)
        print('Media group was sent.')

    yesterday_feed = get_yesterday_feed()
    dau = get_dau(yesterday_feed)
    likes = get_likes(yesterday_feed)
    views = get_views(yesterday_feed)
    ctr = get_ctr(likes, views)
    weekly_feed = get_weekly_feed()
    send_report(dau, likes, views, ctr, weekly_feed)
    
dag_val_ks_task7_1 = dag_val_ks_task7_1()