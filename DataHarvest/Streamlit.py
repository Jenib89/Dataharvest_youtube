import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import re


api_key = 'AIzaSyDuMK9gMktmmEUBZIZxRjbJ33uw-foxaGE'
youtube = build('youtube', 'v3', developerKey=api_key)

client =MongoClient("mongodb://localhost:27017")
db=client["YoutubeData"]

newcollection = db["Channels"]
Playlistcollection = db["Playlists"]
Videocollection  = db["Videos"]
Commentcollection = db["Comments"]

myconnection = mysql.connector.connect( 
host = "localhost",
user = "root",
password = "Jeni27589$",
database = "youtube",
auth_plugin ="mysql_native_password"

)
cur=myconnection.cursor()

def get_Channel(youtube,channel_ids):
    allchannel_data =[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id =channel_ids
        
    )
    response = request.execute()
    data = dict(      
        Channel_id = response['items'][0]['id'],  
        Channel_name=response['items'][0]['snippet']['title'],
        Subscribers=response['items'][0]['statistics']['subscriberCount'],
        Views=response['items'][0]['statistics']['viewCount'],
        Total_videos=response['items'][0]['statistics']['videoCount'],
        Description=response['items'][0]['snippet']['description']
    )
    
   
     
    return data



def does_data_exist(collection, channel_id):
    existing_data = collection.find_one({"Channel_id": channel_id})
    return existing_data is not None

def get_channel_playlists(youtube, channel_id):
    playlist_data = []

    # First, get the channel's uploads playlist ID
    channel_info = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    uploads_playlist_id = channel_info['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Then, list all playlists associated with the channel
    playlists_info = youtube.playlists().list(
        part="snippet",
        channelId=channel_id,
        maxResults=50  # You can adjust this based on your needs
    ).execute()

    for playlist in playlists_info.get('items', []):
        playlist_id = playlist['id']
        playlist_name = playlist['snippet']['title']

        # Skip the uploads playlist as it was already captured
        if playlist_id != uploads_playlist_id:
            playlist_data.append({
                "Channel_id": channel_id,
                "Playlist_id": playlist_id,
                "Playlist_name": playlist_name
            })

    return playlist_data

def get_video(youtube,playlist_id):
    video_ids =[]
    
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId = playlist_id, # playlistId should be as it is 
        maxResults=50, 
    )
    
    response = request.execute()  
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
   
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages :
        if next_page_token is None:
                more_pages = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId = playlist_id, # playlistId should be as it is   
                maxResults=50, 
                pageToken=next_page_token
                )
            response = request.execute()  
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
                
            next_page_token = response.get('nextPageToken')    
    
        
    return video_ids

def get_video_details(youtube,video_ids,Playlist_id,user_chinp):
    allvideo_details =[] 
    for i in range(0,len(video_ids), 20):         
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= ','.join(video_ids[i:i+20])
        )

        response = request.execute()
        
        for j in response['items']:
            video_details  = dict(Video_id = j['id'],
				  Playlist_id =Playlist_id,
                                  Channel_name = user_chinp,
                                  Video_name = j['snippet']['title'],
                                  Published_Date= j['snippet']['publishedAt'],
                                  video_description= j['snippet']['description'],
                                  Viewcount = j['statistics']['viewCount'],
                                  Likecount = j['statistics']['likeCount'],
                                  Favouritecount = j['statistics']['favoriteCount'],
                                  Commentcount = j['statistics']['commentCount'],
                                  Duration = j['contentDetails']['duration'],
                                  Caption_Status = j['contentDetails']['caption'],
                                  thumbnail = j['snippet']['thumbnails']['default']['url'],
                                  #Dislikecount = j['statistics']['dislikeCount'] no dislike
                                  
                                 )
            allvideo_details.append(video_details)
           
        return allvideo_details

def does_data_exist_playlist(Videocollection, Playlist_id):
    existing_playlistdata = Videocollection.find_one({"Playlist_id": Playlist_id})
    return existing_playlistdata is not None

def get_video_comments(youtube, video_ids,user_chinp):
    comments = []

    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            textFormat="plainText",
            maxResults=50  # You can adjust the number of comments to retrieve
        )

        response = request.execute()

        for comment_thread in response["items"]:
            comment_details  = dict(
                                  video_id = comment_thread["snippet"]["videoId"],
                                  Channel_name =user_chinp,                
                                  comment = comment_thread["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                  comment_author = comment_thread["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                  comment_Published_date = comment_thread["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                 )
            comments.append(comment_details)

    return comments

def channel_names():   
    ch_name = []
    for i in db.newcollection.find():
     st.write("i")
     ch_name.append(i['channel_name'])
    return ch_name


def migrate_data_to_mysql(user_chinp,Playlist_id):
 #client = MongoClient("mongodb+srv://saikarthi20:<PASSWORD>@cluster0.0chpi84.mongodb.net/?retryWrites=true&w=majority")

 client =MongoClient("mongodb://localhost:27017")
 db=client["YoutubeData"]
        
 # Connect to MySQL
 conn = mysql.connector.connect(host="localhost", user="root", port='3306', database="Youtube", password="Jeni27589$",auth_plugin ="mysql_native_password")
 cur = conn.cursor()
                   
                    
 # Create channel table
 cur.execute('''CREATE TABLE IF NOT EXISTS channel (
     channel_id           varchar(100) PRIMARY KEY,
     channel_name         varchar(100)  NOT NULL,     
     channel_description  text,
     video_count          int,
     view_count           int,
     subscriber_count     int
        )''')    
 # Create playlist table
 cur.execute('''CREATE TABLE IF NOT EXISTS playlist (
     playlist_id          varchar(100)  PRIMARY KEY,
     channel_id           varchar(100),
     Playlist_name         varchar(500) NOT NULL,
     
     FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
                       )''')              

# Create videos table
 cur.execute('''CREATE TABLE IF NOT EXISTS videos (
    video_channel_name    varchar(255) NOT NULL,
    video_id              varchar(50) NOT NULL ,
    playlist_id1          varchar(100)  NOT NULL, 
    video_name            varchar(100) NOT NULL,
    video_description     text,
    published_date        TIMESTAMP,
    view_count            int,
    like_count            int,
    #dislike_count         int,
    favourite_count       int,
    comments_count        int,
    Duration_count        int
                       )''')

# Create comments table
 cur.execute('''CREATE TABLE IF NOT EXISTS comments (     
     video_id               varchar(100),
     comment_name           text,                  
     comment_author         NVARCHAR(200) NOT NULL,
     comment_published_at   TIMESTAMP,
     Channel_name           varchar(100) NOT NULL
                       )''')

 
 cur.execute("SELECT channel_id FROM channel WHERE channel_name = %s", (user_chinp,))
 result = cur.fetchone()
 if result:
  st.warning("Channel already exists in MySQL. Skipping migration.")
  return


 for channel_details in db["Channels"].find():
  if channel_details:
   channel_name = channel_details.get('Channel_name')
   channel_id = channel_details.get('Channel_id')
   if channel_name ==  user_chinp:
    
    try:
     cur.execute('''INSERT INTO channel (channel_name, channel_id, channel_description, video_count, view_count, subscriber_count)
     VALUES (%s, %s, %s, %s, %s, %s)''',
     (channel_details.get('Channel_name', ''), channel_details.get('Channel_id', ''),
     channel_details.get('Description', ''), channel_details.get('Total_videos', 0),
     channel_details.get('Views', 0), channel_details.get('Subscribers', 0)))
    except Exception as e:
     print("Error occurred while inserting data into channel table:", e)

    # Query playlist details
    
    playlists = list(db["Playlists"].find({"Channel_id": channel_id}))
    for playlist in playlists:
     playlist_id = playlist.get("Playlist_id")
     channel_id = playlist.get("Channel_id")
     playlist_name = playlist.get("Playlist_name")
    
     insert_sql = "INSERT INTO playlist (playlist_id, channel_id, Playlist_name) VALUES (%s, %s, %s)"
     values = (playlist_id, channel_id, playlist_name)
     cur.execute(insert_sql, values)
    
                                          
    # Query video details
    
    video_details = list(db["Videos"].find({"Channel_name": channel_name}))
    
    for videos in video_details:
     published_date_str = videos.get('Published_Date')     
     formatted_published_date = None
     published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%S%z')
     formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S') 
     video_channel_name = videos.get("Channel_name")
     video_id = videos.get("Video_id")
     video_Playlist_id = videos.get("Playlist_id")
     video_name =   videos.get("Video_name") 
     video_desc = videos.get("video_description")
     view_count = videos.get("Viewcount")
     like_count =  videos.get("Likecount")
     favourite_count = videos.get("Favouritecount")
     comment_count =videos.get("Commentcount")
     duration =videos.get("Duration")
     match = re.match(r'PT(\d+)M(\d+)S', duration)
      
     if match:
      minutes = int(match.group(1))
      seconds = int(match.group(2))
      total_seconds = minutes * 60 + seconds
     
     #cur.execute("ALTER TABLE videos MODIFY COLUMN video_description TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
     video_sql = "INSERT INTO videos (video_channel_name,video_id, playlist_id1, video_name, video_description, published_date, view_count,like_count, favourite_count, comments_count,Duration_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
     values1 = (video_channel_name,video_id,video_Playlist_id,video_name,'',formatted_published_date,view_count,like_count,favourite_count,comment_count,total_seconds)
         
     cur.execute(video_sql, values1)
       
    
    
     # Query comments details
     comments_details = list(db["Comments"].find({"Channel_name": channel_name}))
    
     for comments in comments_details:
      updated_at = comments.get('comment_Published_date')  
      published_date1 = datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%S%z')
      formatted_published_date1 = published_date1.strftime('%Y-%m-%d %H:%M:%S')
      video_comment_id = comments.get("video_id")
      video_comment = comments.get("comment")
      comment_author = comments.get("comment_author") 
      comment_string = re.sub(r'[^\w\s,]', '', video_comment)   
      cleaned_string = re.sub(r'[^\w\s,]', '', comment_author)  
      #st.write(comment_string)
      #st.write(cleaned_string) 
        
      comment_sql = "INSERT INTO comments ( video_id, comment_name,comment_author, comment_published_at,Channel_name) VALUES (%s, %s, %s, %s, %s)" 
      commentvalues = (video_comment_id,'','' ,formatted_published_date1,channel_name)
      cur.execute(comment_sql, commentvalues)
                               
                                      
 conn.commit()
 st.success("Migration Successfull")

def mysql_query_questions(query,selected_question):
    cur = None
    df = pd.DataFrame()  
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(host="localhost", user="root", port='3306', database="Youtube", password="Jeni27589$",auth_plugin ="mysql_native_password")
        cur = conn.cursor()
       
          # Execute the query
        if selected_question == "Names of all the videos and their corresponding channels":
                         query = "SELECT video_name, video_channel_name FROM videos"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video_name", "channel_name"])
        elif selected_question == "Channels have the most number of videos,how many videos do they have":
                         query = "SELECT channel_name, video_count FROM channel ORDER BY video_count DESC LIMIT 5"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["channel Name", "video Count"])
        
        elif selected_question == "Top 10 most viewed videos,their respective channels":
                         query = "SELECT videos.video_name, channel.channel_name, MAX(videos.view_count) AS max_view_count FROM videos JOIN channel ON videos.video_channel_name = channel.channel_name GROUP BY videos.video_name, channel.channel_name ORDER BY max_view_count DESC LIMIT 10"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video Name", "channel Name", "view count"])
        elif selected_question == "How many comments were made on each video,their corresponding video names":
                         query = "SELECT video_name, comments_count FROM videos"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video_name", "comments_count"])
        elif selected_question == "videos have the highest number of likes,and what are their corresponding channel names":
                         query = "SELECT videos.video_name, channel.channel_name, MAX(videos.like_count) AS max_like_count FROM videos JOIN channel ON videos.video_channel_name = channel.channel_name GROUP BY videos.video_name, channel.channel_name ORDER BY max_like_count DESC LIMIT 10"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video Name", "channel Name", "max_like_count"])
        elif selected_question == "Total number of likes ,dislikes of each video,what are their corresponding video names":    
                         query = "SELECT videos.video_name, SUM(videos.like_count) AS Total_Likes FROM videos GROUP BY videos.video_name"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video Name", "Total Likes"])
        elif selected_question == "Total number of views of each channel,what are their corresponding channel names":
                         query = "SELECT channel_name, view_count FROM channel"
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["channel_name", "view_count"])
        elif selected_question == "Names of the channels that have published videos of the year 2022":
                         query = "SELECT channel.channel_name FROM channel INNER JOIN videos ON channel.channel_name = videos.video_channel_name WHERE YEAR(videos.published_date) = 2022 GROUP BY channel.channel_name"                     
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["channel_name"])
        elif selected_question == "Channels have the highest number of subscribers out of these channels":
                         query = "SELECT channel.channel_name, channel.subscriber_count FROM channel ORDER BY channel.subscriber_count DESC LIMIT 5"                   
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["channel_name", "subscriber_count"])
        elif selected_question == "Videos have the highest number of comments and their corresponding channel names":
                         query = "SELECT videos.video_name, channel.channel_name, MAX(videos.comments_count) AS max_comments_count FROM videos JOIN channel ON videos.video_channel_name = channel.channel_name GROUP BY videos.video_name, channel.channel_name ORDER BY max_comments_count DESC LIMIT 10"                  
                         cur.execute(query)
                         results = cur.fetchall()
                         df = pd.DataFrame(results, columns=["video_name", "channel_name","max_comments_count"])

       
        
        
    except connection.Error as error:
        print("Error occurred during the execution of the query:", error)

    finally:
        # Close the cursor and connection
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return df



# Create two divs, one for each column
col1, col2,col3 = st.columns(3)

# Add content to the first div

with col1:
    #st.header("Div 1")
    
    user_input = st.text_input("Enter Channel ID", "")   
   
    
    if st.button("Get Channel Detail",key="Channel_Submit"):
     channel_details = get_Channel(youtube,user_input)   
     channel_data = pd.DataFrame([channel_details], columns=channel_details.keys())    
     st.dataframe(channel_data)

    if st.button("Insert Channel Detail",key="Channel_insert"):   
     if not does_data_exist(newcollection, user_input):
      channel_details = get_Channel(youtube,user_input)             
      insert_result =newcollection.insert_one(channel_details)
      playlist_details =get_channel_playlists(youtube,user_input) 
      insert_playlist  = Playlistcollection.insert_many(playlist_details)

      if insert_playlist.acknowledged:
       st.success("Channel Data inserted successfully!")
      else:
       st.error("Data insertion failed!")
     else:
      st.warning("Channel already exists.")
    
    


# Add content to the second div
with col2:
    #st.header("Div 2")
    channel_from_mongodb = [doc for doc in newcollection.find()]
    channel_options = [doc["Channel_name"] for doc in channel_from_mongodb]

    Playlist_from_mongodb = [doc for doc in Playlistcollection.find()]
    playlist_options = [doc["Playlist_name"] for doc in Playlist_from_mongodb]


    selected_option = st.selectbox("Select a Channel:", channel_options)
    if selected_option:
     selected_channeldata = [ch for ch in channel_from_mongodb if ch["Channel_name"] == selected_option][0]
     filtered_playlists = [p for p in Playlist_from_mongodb if p["Channel_id"] == selected_channeldata["Channel_id"]]
     playlist_options = [p["Playlist_name"] for p in filtered_playlists]        
    
     selected_playlist_option = st.selectbox("Select a Playlist:", playlist_options)
    if selected_playlist_option:
     selected_playlist_data = [p for p in filtered_playlists if p["Playlist_name"] == selected_playlist_option][0]
     Playlist_id = selected_playlist_data["Playlist_id"]
     user_chinp =selected_channeldata["Channel_name"]

     video_ids = get_video(youtube,Playlist_id)     
     video_details= get_video_details(youtube,video_ids,Playlist_id,user_chinp)
     
     video_statistics = pd.DataFrame(video_details)

     if st.button("Insert Video Details",key="Video_insert"):   
      if not does_data_exist_playlist(Videocollection, Playlist_id):     
       insert_vidoelist  = Videocollection.insert_many(video_details)       
       comments_details = get_video_comments(youtube, video_ids,user_chinp)
       insert_vidoecomments  = Commentcollection.insert_many(comments_details)

       if insert_vidoelist.acknowledged:
        st.success("Video details Inserted")
       else:
        st.error("Insertion failed!")
      else:
       st.warning("Playlist already exists.")

    
    if user_chinp and st.button("Migrate to MySQL"):            
     migrate_data_to_mysql(user_chinp,Playlist_id)
      
with col3: 

 ques=["Names of all the videos and their corresponding channels",
          "Channels have the most number of videos,how many videos do they have",
          "Top 10 most viewed videos,their respective channels",
          "How many comments were made on each video,their corresponding video names",
          "videos have the highest number of likes,and what are their corresponding channel names",
          "Total number of likes of each video,what are their corresponding video names", 
          "Total number of views of each channel,what are their corresponding channel names",
          "Names of the channels that have published videos of the year 2022",
          "Channels have the highest number of subscribers out of these channels",
          "Videos have the highest number of comments and their corresponding channel names"]
    
 selected_question  = st.selectbox("Select Your Question", (ques))
 b = mysql_query_questions("", selected_question)
 st.dataframe(b)
client.close()