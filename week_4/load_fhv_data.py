import requests
from io import BytesIO  # Import BytesIO
from google.cloud import storage

def download_and_upload(request):
    # GCS bucket details
    bucket_name = "zoomcamp_week_4_data"

    # Iterate through months
    for month in range(1, 13):
        # Generate the URL for each month
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz"
        destination_blob_name = f"fhv_data_2019-{month:02d}.csv"

        try:
            # Download data from URL
            with requests.get(url, stream=True) as response:
                response.raise_for_status()  # Raise an HTTPError for bad responses

                # Upload compressed data to GCS bucket
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)

                # Upload the file directly from the response content
                blob.upload_from_file(BytesIO(response.content), content_type='application/gzip')

                print(f"File {destination_blob_name} uploaded to {bucket_name}")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading or uploading file: {e}")

    return "All files downloaded and uploaded successfully"
