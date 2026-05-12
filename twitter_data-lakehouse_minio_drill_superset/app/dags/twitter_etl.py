import os
import requests
from uuid import uuid4
from datetime import datetime
from airflow.decorators import dag, task


@task
def get_twitter_data():
    TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

    # Get tweets using Twitter API v2 & Bearer Token
    BASE_URL = "https://api.twitter.com/2/tweets/search/recent"
    USERNAME = "elonmusk"
    FIELDS = {"created_at", "lang", "attachments", "public_metrics", "text", "author_id"}

    url = f"{BASE_URL}?query=from:{USERNAME}&tweet.fields={','.join(FIELDS)}&expansions=author_id&max_results=50"
    response = requests.get(url=url, headers={"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"})
    response_content = response.json()

    data = response_content["data"]
    includes = response_content["includes"]
    return data, includes


@task
def clean_twitter_data(tweets_data):
    data, includes = tweets_data

    batchId = uuid4().hex
    batchDatetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Refine tweets data
    tweet_list = []
    for tweet in data:
        refined_tweet = {
            "tweet_id": tweet["id"],
            "username": includes["users"][0]["username"],  # Get username from the included data
            "user_id": tweet["author_id"],
            "text": tweet["text"],
            "like_count": tweet["public_metrics"]["like_count"],
            "retweet_count": tweet["public_metrics"]["retweet_count"],
            "created_at": tweet["created_at"],
            "batchID": batchId,
            "batchDatetime": batchDatetime,
        }
        tweet_list.append(refined_tweet)
    return tweet_list, batchDatetime, batchId


@task
def write_to_bucket(data):
    tweet_list, batchDatetime_str, batchId = data
    batchDatetime = datetime.strptime(batchDatetime_str, "%Y-%m-%d %H:%M:%S")

    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    df = pd.DataFrame(tweet_list)
    file_data = df.to_parquet(index=False)

    client = Minio("minio:9000", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    # Put parquet data in the bucket
    filename = (
        f"tweets/{batchDatetime.strftime('%Y/%m/%d')}/elon_tweets_{batchDatetime.strftime('%H%M%S')}_{batchId}.parquet"
    )
    client.put_object(
        MINIO_BUCKET_NAME, filename, data=BytesIO(file_data), length=len(file_data), content_type="application/csv"
    )


@dag(
    schedule="0 * * * *",
    start_date=datetime(2023, 1, 10),
    catchup=False,
    tags=["twitter", "etl"],
)
def twitter_etl():
    raw_data = get_twitter_data()
    cleaned_data = clean_twitter_data(raw_data)
    write_to_bucket(cleaned_data)


twitter_etl()
