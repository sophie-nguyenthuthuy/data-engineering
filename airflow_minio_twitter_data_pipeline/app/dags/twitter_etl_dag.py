import os
import json
import requests
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
    response = json.loads(response.content)

    data = response["data"]
    includes = response["includes"]

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
        }
        tweet_list.append(refined_tweet)
    return tweet_list


@task
def dump_data_to_bucket(tweet_list: list):
    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    df = pd.DataFrame(tweet_list)
    csv = df.to_csv(index=False).encode("utf-8")

    client = Minio("minio:9000", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    # Put csv data in the bucket
    client.put_object(
        "airflow-bucket", "twitter_elon_musk.csv", data=BytesIO(csv), length=len(csv), content_type="application/csv"
    )


@dag(
    schedule="0 */2 * * *",
    start_date=datetime(2022, 12, 26),
    catchup=False,
    tags=["twitter", "etl"],
)
def twitter_etl():
    dump_data_to_bucket(get_twitter_data())


twitter_etl()
