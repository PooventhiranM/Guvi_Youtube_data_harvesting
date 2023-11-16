import pandas as pd
import streamlit as st
import mysql.connector as mysql
import pymongo
from googleapiclient.discovery import build
from PIL import Image
import re 
from datetime import timedelta

# Setting page configuration
icon = Image.open("C:/Users/poove/Downloads/Youtube_logo.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing | by Pooventhiran",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={'About':"# This app is created by pooven!..."}
                  )

# Creating option menu
tab1,tab2,tab3=st.tabs(["Home","Extract & Transform","view"])

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("localhost:27017")
db = client.Youtube_Data

# BUILDING CONNECTION WITH YOUTUBE API
api_key="AIzaSyD_j-EpbZc0YlMS97b1fAzazktYKrKWmBQ" 
youtube=build('youtube','v3',developerKey=api_key)

# CONNECTING WITH MYSQL DATABASE
mydb = mysql.connect(host="localhost",
                   user="root",
                   password="Pooventhiran2",
                   port = "3306"
                  )
mycursor = mydb.cursor(buffered=True)
mycursor.execute("CREATE DATABASE if not exists youtube")
mycursor.execute("USE youtube")
mycursor.execute("CREATE TABLE if not exists channels(Channel_id VARCHAR(100) primary key,Channel_name VARCHAR(100),Playlist_id VARCHAR(100),Subscribers BIGINT,Views BIGINT,Total_videos INT,Description TEXT)")
mycursor.execute("CREATE TABLE if not exists videos(Channel_id VARCHAR(100),Channel_name VARCHAR(100),Video_id VARCHAR(100) PRIMARY KEY,Title VARCHAR(100),Thumbnail VARCHAR(100),Published_date DATE,Duration INT,Caption_status VARCHAR(15),Views BIGINT,Likes BIGINT,Dislikes INT,Total_comments INT)")
mycursor.execute("CREATE TABLE if not exists comments(Comment_id VARCHAR(100),Video_id VARCHAR(100),Comment_text TEXT,Comment_author TEXT(10000),Comment_published DATE)")

# Function to channel details
def get_channel_details(channel_ids):
    all_data=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response=request.execute()
    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_ids[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description']
                    )
        all_data.append(data)
    return all_data

# FUNCTION TO GET VIDEO IDS
def get_video_ids(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(video_ids):
    video_stats=[]
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        
        hours_pattern = re.compile(r'(\d+)H')
        minutes_pattern = re.compile(r'(\d+)M')
        seconds_pattern = re.compile(r'(\d+)S')
        
        for video in response['items']:
            def total_seconds():
                duration=video['contentDetails']['duration']
            
                hours=hours_pattern.search(duration)
                minutes=minutes_pattern.search(duration)
                seconds=seconds_pattern.search(duration)
                               
                hours = int(hours.group(1)) if hours else 0
                minutes = int(minutes.group(1)) if minutes else 0
                seconds = int(seconds.group(1)) if seconds else 0
                                
                video_seconds=timedelta(hours=hours,minutes=minutes,seconds=seconds).total_seconds()
                return video_seconds                
                                
            video_details = dict(Channel_id = video['snippet']['channelId'],
                                Channel_name = video['snippet']['channelTitle'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Published_date = video['snippet']['publishedAt'][:10],
                                Duration = int(total_seconds()),
                                Caption_status = video['contentDetails']['caption'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Dislikes = video['statistics'].get('dislikeCount'),
                                Total_comments = video['statistics'].get('commentCount')
                                )
            
            video_stats.append(video_details)
            
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(video_id):
    try:
        comment_data = []
        next_page_token = None
        while True:
            request = youtube.commentThreads().list(part="snippet",
                                                    videoId=video_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token)
            response = request.execute()
            for cmt in response['items']:
                
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'][:10],
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name=[]
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

# Home page
with tab1:
    
    st.markdown("## :blue[Domain] : Social Media")
    st.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")

# EXTRACT AND TRANSFORM PAGE
with tab2:
    tab4,tab5 = st.tabs(["$\huge 📝 EXTRACT $","$\huge 🚀 TRANSFORM $"])

    # EXTRACT TAB
    with tab4:
        st.markdown("#  ")
        st.write("### Enter Youtube Channel_ID below :")
        channel_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if channel_id and st.button("Extract Data"):
            channel_details = get_channel_details(channel_id)
            st.write(f'### Extracted data from :green["{channel_details[0]["Channel_name"]}"]channel')
            st.table(channel_details)

        if st.button("Upload to MongoDB"):
            with st.spinner("Please Wait for it..."):
                channel_details = get_channel_details(channel_id)
                video_ids=get_video_ids(channel_id)
                video_details=get_video_details(video_ids)

                def comments():
                    com_d = []
                    for i in video_ids:
                        com_d += get_comments_details(i)
                    return com_d
                comm_details=comments()

                collections = db["channel_details"]
                collections.insert_many(channel_details) 

                collections1 = db["video_details"]
                collections1.insert_many(video_details)

                collections2 = db["comment_details"]
                collections2.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")

    #Transform tab
    with tab5:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_inp = st.selectbox("select channel",options=ch_names)

        def insert_into_channel_details():
            ch_list=[]
            collections = db["channel_details"]     
            for i in collections.find({'Channel_name':user_inp},{'_id':0}):
                ch_list.append(i)
            df=pd.DataFrame(ch_list)

            for index,row in df.iterrows():
                insert_query = '''INSERT into channels(Channel_id,Channel_name,Playlist_id,Subscribers,Views,Total_videos,
                                  Description) VALUES(%s,%s,%s,%s,%s,%s,%s)'''
                values =(row['Channel_id'],row['Channel_name'],row['Playlist_id'],row['Subscribers'],row['Views'],row['Total_videos'],
                        row['Description'])
                mycursor.execute(insert_query,values)
                mydb.commit()

            vi_list = []
            collections1 = db["video_details"]
            for vi_data in collections1.find({"Channel_name":user_inp},{"_id":0}):
                vi_list.append(vi_data)
            df2=pd.DataFrame(vi_list)
            df2['Duration']=pd.to_numeric(df2['Duration'])
         
            for index, row in df2.iterrows():
                insert_query1 = '''INSERT INTO videos (Channel_id,Channel_name,Video_id,Title,Thumbnail,
                                  Published_date,Duration,Caption_status,Views,Likes,Total_comments)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                values1 = (row['Channel_id'],row['Channel_name'],row['Video_id'],row['Title'],row['Thumbnail'],
                          row['Published_date'],row['Duration'],row['Caption_status'],row['Views'],row['Likes'],row['Total_comments'])
                mycursor.execute(insert_query1,values1)
                mydb.commit()

            com_list = []
            collections2 = db['comment_details']
            for vid in collections1.find({'Channel_name':user_inp},{"_id":0}):
                for i in collections2.find({'Video_id':vid['Video_id']},{"_id":0}):
                    com_list.append(i)
            df3 = pd.DataFrame(com_list)

            for index, row in df3.iterrows():
                insert_query2 = '''INSERT INTO comments (Comment_id,Video_id ,Comment_text,Comment_author,Comment_published)
                                VALUES (%s, %s, %s, %s, %s)'''
                values2 = (row['Comment_id'],row['Video_id'],row['Comment_text'],row['Comment_author'],row['Comment_posted_date'])
                mycursor.execute(insert_query2,values2)
                mydb.commit()

        if st.button("Submit"):
            try:
                insert_into_channel_details()
                st.success("Transformation to MYSQL successful !!")
            except:
                st.error("Channel details already transformed !!")

# VIEW PAGE
with tab3:
    
    questions=st.selectbox('Questions',['1. What are the names of all the videos and their corresponding channels?',
                             '2. Which channels have the most number of videos, and how many videos do they have?',
                             '3. What are the top 10 most viewed videos and their respective channels?',
                             '4. How many comments were made on each video, and what are their corresponding video names?',
                             '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                             '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                             '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                             '8. What are the names of all the channels that have published videos in the year 2022?',
                             '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                             '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("SELECT Title AS Video_name, Channel_name FROM videos ORDER BY Channel_name")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("SELECT Channel_name, Total_videos FROM channels ORDER BY Total_videos DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("SELECT Channel_name, Title, Views FROM videos ORDER BY Views DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.Video_id,Title,b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT Video_id,COUNT(Comment_id) AS Total_Comments
                            FROM comments GROUP BY Video_id) AS b
                            ON a.Video_id = b.Video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("SELECT Channel_name,Title,Likes AS Like_Count FROM videos ORDER BY Like_count DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("SELECT Title,Likes FROM videos ORDER BY Likes DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("SELECT Channel_name, Views AS Views FROM channels ORDER BY Views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("SELECT Channel_Name FROM videos WHERE Published_date LIKE '2022%' GROUP BY Channel_name ORDER BY Channel_name")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name,
                            AVG(Duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY Channel_name
                            ORDER BY AVG(Duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("SELECT Channel_name,Video_id,Total_Comments FROM videos ORDER BY Total_Comments DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
