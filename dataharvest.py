import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import streamlit as st
import mysql.connector

api_service_name = "youtube"
api_version = "v3"
api_key= "AIzaSyDqtu0CPDYZT05SJUNnZifUi15V4hyLNWs"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey= api_key)

def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id
    )
    response = request.execute()

    data = {
        "channel_Id" : str(channel_id) ,
        "channel_name" : str(response['items'][0]['snippet']['title']),       #channel_name gets the name of the channel)
        "channel_des" : str(response['items'][0]['snippet']['description']),   #channel_des gets and stores the description of the channel
        "channel_vidcnt" : int(response['items'][0]['statistics']['videoCount']), #channel_vidcnt gets the total video count of the channel
        "channel_subs" : int(response['items'][0]['statistics']['subscriberCount']), #channel_subs gets number of subscribers of the channel
        "channel_viwcnt" : int(response['items'][0]['statistics']['viewCount']) #channel_viwcnt gets the view count of the channel

         }
    return data

def playlist(channel_id):
    # Request to retrieve channel data
    request = youtube.channels().list(
        part="snippet,contentDetails",
        id=channel_id,
        maxResults=1
    )
    response = request.execute()

    # Construct playlist info dictionary
    playlist_info = {
        "Channel_Id": str(channel_id) ,
        "Playlist_Id": str(response['items'][0]['contentDetails']['relatedPlaylists']['uploads']), #channel_pid gets the upload playlist id of the channel
        "Playlist_Name":str(response['items'][0]['snippet']['title'])
    }

    return playlist_info

def iso8601_to_seconds(duration_string):
    # Define regex pattern to match ISO 8601 duration format
    pattern = r'PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?$'

    # Use regex to extract components from duration string
    match = re.match(pattern, duration_string)
    if not match:
        raise ValueError("Invalid duration string format")

    # Extract hours, minutes, and seconds from regex match
    hours = int(match.group('hours')) if match.group('hours') else 0
    minutes = int(match.group('minutes')) if match.group('minutes') else 0
    seconds = int(match.group('seconds')) if match.group('seconds') else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds # Calculate total seconds

    return total_seconds

def playlist_videos(channel_id):
    videos = []
    next_page_token = None
    playlist_id=f"UU{channel_id[2:]}"

    while True:
        # Request to retrieve playlist items
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,  # Maximum number of results per page
            pageToken=next_page_token
        )
        response = request.execute()
        
        # Extract video information from response
        for item in response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video_info = get_video_details(video_id,playlist_id)
            videos.append(video_info)
        # Check if there are more pages of results
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # No more pages, exit loop

    return videos

def get_video_details(video_id,playlist_id):
    # Request to retrieve video details
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id,
        maxResults=1
    )
    response = request.execute()
    published= response['items'][0]['snippet']['publishedAt']
    published_at = datetime.fromisoformat(published)
    duration = iso8601_to_seconds(response['items'][0]['contentDetails']['duration'])
    # Extract relevant video information
    video_info = {
        "Playlist_Id":str(playlist_id),
        "Video_Id": video_id,
        "Video_Title": response['items'][0]['snippet']['title'],
        "Video_Description": str(response['items'][0]['snippet']['description']),
        "Published_At": published_at.date(),
        "View_Count": int(response['items'][0]['statistics']['viewCount']),
        "Like_Count": int(response['items'][0]['statistics'].get('likeCount', 0)),
        "Comment_Count":int(response['items'][0]['statistics'].get('commentCount',0)),
        "Duration": duration ,
        "Thumbnail_URL": response['items'][0]['snippet']['thumbnails']['default']['url'], # it gets the url of the video thumbnail
        "Caption_Status": response['items'][0]['contentDetails'].get('caption','caption unavailable') # will lwt us know whether there is caption available or not
    }
    return video_info

def retrieve_comments(channel_id):
    # Retrieve video IDs from the playlist
    playlist_id=f"UU{channel_id[2:]}"
    videos = playlist_videos(playlist_id)
    video_ids = [video['Video_Id'] for video in videos]

    comment_info = []

    for video_id in video_ids:
        try:
            # Request to retrieve comments for a video
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()

            # Extract comment information from response
            for item in response['items']:
                cpublished = item['snippet']['topLevelComment']['snippet']['publishedAt']
                cpublished_at = datetime.fromisoformat(cpublished)
                comment_info.append({                  # Appending comment information to comment_info list
                    "Comment_Id": item['id'] ,
                    "Video_Id": video_id,
                    "Comment_Text":  item['snippet']['topLevelComment']['snippet']['textDisplay'] ,
                    "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'] ,
                    "Published_At": cpublished_at.date()
                })
        except HttpError as e:
            if e.resp.status == 403:
                pass  # Handle HTTP 403 error (comments disabled) as needed
            else:
                print(f"Error occurred while retrieving comments for video with ID {video_id}: {e}")
        # Handle other HTTP errors if needed
        except IntegrityError as e:
            pass  # Handle IntegrityError 
    return comment_info

def process_channel_data(channel_id):
    try:
        #calling the channel_data function while passing a channel id as a parameter
        ch_result = channel_data(channel_id) 
        channel_df = pd.DataFrame([ch_result])
        
        #call playlist function by passing the channel Id
        playlist_result = playlist(channel_id)
        playlist_df = pd.DataFrame([playlist_result])
        
        #Calling playlist_videos function by passing channel_id to retrieve video data
        vid_result=playlist_videos(channel_id)
        video_df=pd.DataFrame(vid_result)
        
        #Calling retrieve_comments function to retrieve comments of the videos respectively
        comm_result = retrieve_comments(channel_id)
        comment_df = pd.DataFrame(comm_result)
        return channel_df, playlist_df, video_df, comment_df

    except Exception as e:
        return None

 #SQL part 

# Connect to MySQL database
def connect_to_database():
    try:
        # Create SQLAlchemy engine
        engine = create_engine('mysql+mysqlconnector://root:@localhost/youtube_project')
        st.success("Connected to database")
        return engine

    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None
    

def insert_data_into_database(result):
    try:
        engine=connect_to_database()
        channel_df, playlist_df, video_df, comment_df = result
        if engine:
            # Insert data into MySQL tables
            with engine.connect() as connection:
                channel_df.to_sql('channel', con=connection, if_exists='append', index=False)
                playlist_df.to_sql('playlist', con=connection, if_exists='append', index=False)
                video_df.to_sql('video', con=connection, if_exists='append', index=False)
                comment_df.to_sql('comments', con=connection, if_exists='append', index=False)

            st.success("Data loaded into database successfully!")
            return True  # Return True if data insertion was successful
        else:
            st.error("Failed to connect to the database")
            return False
             
    except IntegrityError as e:  # Catch IntegrityError
        st.error(f"Failed to load data into MySQL database due to duplicate values: {e}")
        return False
    except Exception as e:
        st.error(f"Failed to load data into MySQL database: {e}")
        return False
    finally:
        if engine:
            engine.dispose()  # Close the database connection
    
queries={
        "What are the names of all the videos and their corresponding channels?": '''SELECT v.Video_Title AS Video_Name, c.Channel_name AS Channel_Name
            FROM video v
            JOIN playlist p ON v.Playlist_Id = p.Playlist_id
            JOIN channel c on c.Channel_id = p. Channel_id ''',

        "Which channels have the most number of videos, and how many videos do they have?":
        '''SELECT Channel_name,Channel_vidcnt FROM  channel ORDER BY Channel_vidcnt DESC LIMIT 5''',

        "What are the top 10 most viewed videos and their respective channels?":
         '''SELECT c.Channel_name AS Channel_name, v.Video_Title AS Video_name, v.View_count
            FROM video v
            JOIN playlist p ON v.Playlist_Id = p.Playlist_id
            JOIN channel c ON c.Channel_Id = P.Channel_Id
            ORDER BY v.View_Count DESC LIMIT 10''',

        "How many comments were made on each video, and what are their corresponding video names?":
         '''SELECT v.Video_Title AS Video_name , v.Comment_Count AS CommentCount
            FROM video v
            ORDER BY v.Comment_Count DESC ''',

        "Which videos have the highest number of likes, and what are their corresponding channel names?":
         '''SELECT c.channel_name AS Channel_Name, v.Video_Title AS Video_Name, v.Like_Count AS Likes
            FROM playlist p
            JOIN video v ON p.Playlist_Id = v.Playlist_Id
            JOIN channel c ON c.channel_id = p.channel_id
            ORDER BY v.Like_Count DESC LIMIT 10''',

        "What is the total number of likes and what are their corresponding video names?":
           'SELECT Video_Title AS VideoName , Like_Count  FROM video ORDER BY Like_Count DESC',

        "What is the total number of views for each channel, and what are their corresponding channel names?":
        'SELECT Channel_Name , Channel_viwcnt FROM channel',

        "What are the names of all the channels that have published videos in the year 2022?":
        '''SELECT c.Channel_name, v.Video_Title AS Video_Name , v.Published_At
                    FROM video v JOIN playlist p ON v.Playlist_Id = p.Playlist_id
                    JOIN channel c ON p.Channel_id = c.Channel_id
                    WHERE YEAR(v.Published_At) = 2022''',

        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        '''SELECT c.Channel_name, AVG(v.Duration) AS Avg_duration
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_id
                    JOIN channel c ON p.Channel_id = c.Channel_id
                    GROUP BY c.Channel_name''',

        "Which videos have the highest number of comments, and what are their corresponding channel names?":
        '''SELECT c.Channel_name, v.Video_Title , v.Comment_Count
                    FROM channel c
                    JOIN playlist p ON c.Channel_Id = p.Channel_Id
                    JOIN video v ON p.Playlist_Id = v.Playlist_Id
                    ORDER BY v.Comment_Count DESC LIMIT 10'''}


def execute_query(query_name):
    query = queries.get(query_name)
    engine = connect_to_database()
    if query is not None and query.strip():
        try:
            result = pd.read_sql_query(query, engine)
            return result
        except mysql.connector.Error as e:
            st.error(f"Error executing query '{query_name}': {e}")
            return None
    else:
        st.error(f"Query '{query_name}' not found or is empty.")
        return None

# Define main function
def main():
    st.title("Welcome to YouTube Data Extraction")
    channel_id = st.text_input("Enter YouTube Channel ID:")
    result = process_channel_data(channel_id)
    if st.button("Extract Data"):
        if channel_id:
            if result is not None:
                channel_df, playlist_df, video_df, comment_df = result
                st.subheader("Channel Data:")
                st.write(channel_df)
                # Display playlist data
                st.subheader("Playlist Data:")
                st.write(playlist_df)
                # Display video data
                st.subheader("Video Data:")
                st.write(video_df)
                # Display comment data
                st.subheader("Comment Data:")
                st.write(comment_df)
                st.success("Data extraction successful!")
                
                
        else:
            st.warning("Please enter a valid YouTube Channel ID.") 
    
    if st.button("Load to Database"):  
        bool=insert_data_into_database(result)
        if bool:
                st.write(":+1:") 

     # Select query
    selected_query = st.selectbox("Select a question:", list(queries.keys()), index = None)
    # Execute query if selected
    if selected_query:
        result1 = execute_query(selected_query)
        if result1 is not None:
            st.subheader("Query Result:")
            st.write(result1)
        else:
            st.error("Failed to execute query.")

# Run the main function
if __name__ == "__main__":
    main()

    



 
        








