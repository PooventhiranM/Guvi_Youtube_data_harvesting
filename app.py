import pandas as pd
import streamlit as st
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
icon = Image.open("C:/Users/poove/Downloads/Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Pooventhiran",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by Pooven!*"""})

# CREATING OPTION MENU
selected = st.selectbox("Select any one", ["Home","Extract & Transform","View"])
    
# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("localhost:27017")
db = client.Youtube_Data

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="127.0.0.1",
                   user="root",
                   password="Pooventhiran2",
                   database="youtube",
                   port = "3306"
                  )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDZ39_2kemSWS3XgSTayYD-mzOrSPUwjjw" 
youtube = build('youtube','v3',developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_ids):
    all_data=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response=request.execute()
    for i in range(len(response['items'])):
        data = dict(Channel_ids = channel_ids[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
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
                    part="snippet,statistics",
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        for video in response['items']:
            video_details = dict(Channel_id = video['snippet']['channelId'],
                                Channel_name = video['snippet']['channelTitle'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                #Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                #Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                #Duration = video['contentDetails']['duration'],
                                #Definition = video['contentDetails']['definition'],
                                #Caption_status = video['contentDetails']['caption'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Dislikes = video['statistics'].get('dislikeCount'),
                                #Comments = video['statistics'].get('commentCount'),
                                #Favorite_count = video['statistics']['favoriteCount']
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
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            #Like_count = cmt['snippet']['topLevelComment']['snippet'].get('likeCount'),
                            #Reply_count = cmt['snippet'].get('totalReplyCount')
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
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

st.header("Youtube Data Harvesting and Warehousing")
# Home page
if selected == "Home":
    # Title Image
    st.image(r"C:\Users\poove\Downloads\youtube img.jpg",width=50)
    st.markdown("## :blue[Domain] : Social Media")
    st.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")


# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    
    tab1,tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])
    
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        channel_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if channel_id and st.button("Extract Data"):
            channel_details = get_channel_details(channel_id)
            st.write(f'#### Extracted data from :green["{channel_details[0]["Channel_name"]}"] channel')
            st.table(channel_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                channel_details=get_channel_details(channel_id)
                video_ids=get_video_ids(channel_id)
                video_details=get_video_details(video_ids)
                
                def comments():
                    com_d = []
                    for i in video_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections = db.channel_details
                collections.insert_many(channel_details)

                collections1 = db.video_details
                collections1.insert_many(video_details)

                collections2 = db.comments_details
                collections2.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")

# TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()  
        user_inp = st.selectbox("Select channel",options= ch_names)
        
        def insert_into_channels():
            collections = db.channel_details
            query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            mycursor.execute("""create table channels(Channel_ids varchar(100) primary key,Channel_name varchar(100),Playlist_id varchar(100),Subscribers int,Views int,Total_videos int ,Description varchar(100000),Country varchar(10))""")   
            
            for i in collections.find({"channel_name" : user_inp},{'_id' : 0}):
                mycursor.execute(query,tuple(i.values()))
                mydb.commit()
                
        def insert_into_videos():
            collections1 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            mycursor.execute("""create table videos(Channel_id varchar(100),Channel_name varchar(100),Video_id varchar(100) primary key,Title varchar(100),Tags varchar(1000),Published_date date,Duration time,Views int,Likes int,Dislikes int,comments varchar(100000))""")

            for i in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
                mycursor.execute(query1, tuple(values))
                mydb.commit()

        def insert_into_comments():
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""
            mycursor.execute("""create table comments(Comment_id varchar(100),Video_id varchar(100),Comment_text varchar(100000),Comment_author varchar(1000),Comment_posted_date date)""")
            
            for vid in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

        if st.button("Submit"):
         try:
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            st.success("Transformation to MySQL Successful !!")
         except:
            st.error("Channel details already transformed !!")

# VIEW PAGE
if selected == "View":
    
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
        mycursor.execute("""SELECT title AS Video_name, Channel_name
                            FROM videos
                            ORDER BY Channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT Channel_name, Total_videos
                            FROM channels
                            ORDER BY Total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT Channel_name, title AS Video_Title,Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name,title AS Title,Likes AS Like_Count 
                            FROM videos
                            ORDER BY Like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, Likes AS Like_count
                            FROM videos
                            ORDER BY Like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name, Views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY Channel_name
                            ORDER BY Channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY Channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name,Video_id AS Video_ID,Comment_count AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
