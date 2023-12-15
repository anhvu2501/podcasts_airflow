from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum

import requests
import xmltodict
import os
import json

from google.cloud import speech

EPISODE_FOLDER = "episodes"
FRAME_RATE = 16000

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ""

@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 12, 13),
    catchup=False
)

def podcast_summary():

    # to download podcast metadata
    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f'Found {len(episodes)} episodes.')
        return episodes
    
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id="podcasts"
    )

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        stored_episodes = hook.get_pandas_df("SELECT * FROM episodes;")
        new_episodes = []
        for episode in episodes:
            if episode['link'] not in stored_episodes["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode['link'], episode['title'], episode['pubDate'], episode['description'], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=['link', 'title', 'published', 'description', 'filename'])

    new_episodes = load_episodes(podcast_episodes)
    
    # download episodes to local
    @task()
    def download_episodes(episodes):
        # for episode in episodes:
        name_end = episodes[0]['link'].split('/')[-1]
        filename = f"{name_end}.mp3"
        audio_path = os.path.join(EPISODE_FOLDER, filename)
        if not os.path.exists(audio_path):
            print(f'Downloading {filename}')
            audio = requests.get(episodes[0]['enclosure']['@url'])
            with open(audio_path, "wb+") as f:
                f.write(audio.content)

    audio_files = download_episodes(podcast_episodes)

    @task()
    def speech_to_text(episodes):
        client = speech.SpeechClient()

        # the name of the audio file to transcribe
        name_end = episodes[0]['link'].split('/')[-1]
        filename = f"{name_end}.mp3"
        audio_path = os.path.join(EPISODE_FOLDER, filename)

        client = speech.SpeechClient()

        with open(audio_path, "rb") as audio_file:
            content = audio_file.read() 

        audio = speech.RecognitionAudio(content=content)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.MP3,
            sample_rate_hertz=16000,
            language_code="en-US",
        )

        response = client.recognize(config=config, audio=audio)

        # Each result is for a consecutive portion of the audio. Iterate through
        # them to get the transcripts for the entire audio file.
        for result in response.results:
            # The first alternative is the most likely one for this portion.
            print(f"Transcript: {result.alternatives[0].transcript}")
    speech_to_text(podcast_episodes)

summary = podcast_summary()