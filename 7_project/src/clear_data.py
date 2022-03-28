from pathlib import Path
from glob import glob
from tqdm import tqdm
from google.cloud import storage


"""
Clears data in local filesystem and in GCS bucket.

This needs to be done only because Luigi has no elegant way of overwriting Targets.
"""

def remove_local_files():
    print(f"Removing files in data subfolder")
    files = glob("data/*")
    for file_ in tqdm(files):
        Path(file_).unlink()


def remove_gcs_blobs():
    print(f"Removing blobs in GCS bucket")
    storage_client = storage.Client()
    bucket = storage.Bucket(storage_client, "dtc_de_imdb", user_project="dtc-de-course-339115")
    blobs = storage_client.list_blobs(bucket)
    for blob in tqdm(blobs):
        blob.delete()

def main():

    remove_local_files()

    remove_gcs_blobs()


if __name__ == "__main__":
    main()