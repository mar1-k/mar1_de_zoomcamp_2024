import requests
from google.cloud import storage

def download_and_upload(request):
    # GCS bucket details
    bucket_name = "zoomcamp_week_3_data"

    # Iterate through months
    for month in range(1, 13):
        # Generate the URL for each month
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month:02d}.parquet"
        destination_blob_name = f"green_tripdata_2022-{month:02d}.parquet"

        # Download data from URL
        response = requests.get(url)
        data = response.content

        # Upload data to GCS bucket
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data)

        print(f"File {destination_blob_name} uploaded to {bucket_name}")

    return "All files uploaded successfully"
