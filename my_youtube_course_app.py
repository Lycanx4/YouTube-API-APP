from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import urllib.request
import urllib.parse as p
import re
import os
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import json
from embeddify import Embedder
from waitress import serve
from flask import jsonify
from flask import Flask,render_template, url_for,request,redirect

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# authenticate to YouTube API
def youtube_authenticate():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "myYouTubeApp.json"
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build(api_service_name, api_version, credentials=creds)

# authenticate to YouTube API
youtube = youtube_authenticate()
print("youtube status: ", youtube)
#format: search(youtube, q="python", maxResults=200)
def search(youtube, **kwargs):
    two_year_ago = (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d')
    date2yrago = two_year_ago + "T00:00:00Z"
    videos = youtube.search().list(
        part="snippet",
        **kwargs,
        publishedAfter=date2yrago, type="video", videoDuration="long"
    ).execute()
    moreVideos = videos
    result = []
    items = videos.get("items")
    for vid in items:
        result.append(vid)
    
    token = videos['nextPageToken']

    while(token):
        moreVideos = youtube.search().list(
        part="snippet",
        **kwargs,
        pageToken=token,
        publishedAfter=date2yrago, type="video", videoDuration="long"
        ).execute()
        moreItems = moreVideos.get("items")
        for vid in moreItems:
            result.append(vid)
        if(len(result) >= 100):
            break        
        try:
            token = moreVideos['nextPageToken']
        except:
            break
        
    return result   

def get_video_details(youtube, **kwargs):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        **kwargs
    ).execute()

def write_to_csv(file_name, df):
  if(os.path.exists(file_name) and os.path.isfile(file_name)):
    os.remove(file_name)
    df.to_csv(file_name, index=False)
  else:
    df.to_csv(file_name, index=False)

#get video date and parse to date
def get_item_parsed_date(item):
    date_str = item["snippet"]["publishedAt"]
    parsed_date = datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%SZ")
    new_format = "%Y-%m-%d"
    date_obj = parsed_date.strftime(new_format)

    return date_obj

#get channel detail to pull out subscriber count
def get_channel_details(youtube, **kwargs):
    return youtube.channels().list(
        part="statistics,snippet,contentDetails",
        **kwargs
    ).execute()

#Crawl data and save as csv file 
# !!!Don't call this method if not nessary there is quota limit for this method (1 run cost around 300 quota)!!!
def get_data_frame(key_word):
    videos = search(youtube, q=key_word, maxResults=50)

    dict_channel = dict()
    result = []
    embedder = Embedder()
    
    for vido in videos:
        try:
            vid_items = []
            videoId = vido["id"]["videoId"]
            video = get_video_details(youtube, id=videoId)
            vid = video.get("items")[0]

            url2emb = "https://www.youtube.com/watch?v=" + videoId
            channelID = vid["snippet"]["channelId"]
            subscribers = 0
            if channelID in dict_channel:
                subscribers = dict_channel[channelID]
            else:
                channel_detail = get_channel_details(youtube, id=channelID)
                subscribers = channel_detail.get("items")[0]["statistics"]["subscriberCount"]
                dict_channel[channelID]= subscribers
                
            videoImage = vid["snippet"]["thumbnails"]["medium"]["url"]
            channelName = vid["snippet"]["channelTitle"]
            videoTitle = vid["snippet"]["title"]
            description = vid["snippet"]["description"]
            duration = vid["contentDetails"]["duration"]
            uploadDate= get_item_parsed_date(vid)
            likes = vid["statistics"]["likeCount"]
            viewCounts = vid["statistics"]["viewCount"]
            embeddedURL = embedder(url2emb)
            
            jsonX = { 
                'courseId' : videoId ,
                'courseTitle' : videoTitle,
                'embeddedURL': embeddedURL,
                'views': viewCounts,
                'likes': likes,
                'subscribers': subscribers,
                'channelName': channelName,
                'videoTitle': videoTitle,
                'uploadDate': uploadDate,
                'thumbnail': videoImage,
                'description': description,
                'duration': duration,
                'skill' : key_word
                }
            result.append(jsonX)
        except:
            print("error occur")
    
    df_vid = pd.DataFrame(result)
    csv_name = key_word.lower() + "_Data.csv"
    write_to_csv(csv_name, df_vid)

    return df_vid

def getData(keyword):
    file = keyword.lower() + "_Data.csv"
    df = pd.read_csv(file)
    df2Json = df.to_json(orient="records")
    data = json.loads(df2Json)
    return data

def crawlData(keyword):
    get_data_frame(keyword)
    result = getData(keyword)
    return result

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello, this is Kaung Myat!"

@app.route('/crawl_data/', methods=['GET','POST'])
def request_crawl():
    keyword = request.args.get('keyword') or request.form.get('keyword') 
    return crawlData(keyword)

@app.route('/request_data/', methods=['GET','POST'])
def request_data():
    keyword = request.args.get('keyword') or request.form.get('keyword') 
    try:
        data = getData(keyword)
        return data
    except:
        print("No Available data")
    return "No data found"

# run the server
if __name__ == '__main__':
    print("Starting the server.....")
    serve(app, host="0.0.0.0", port=8080)