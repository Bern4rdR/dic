import os
import gdown
import zipfile
from pathlib import Path

DATASET_DIR = "./datasets"

DATASETS = {
	"air_quality.zip": "https://drive.google.com/file/d/1NGT8RR-NtBf-4xxII4pfwBanE_BgBvoW/view?usp=drive_link",
	"weather.csv": "https://drive.google.com/file/d/1q-Lw24XFqJ42XSJRuUV_ced3aJ6kKQ2M/view?usp=sharing",
	"taxi_zone_lookup.csv": "https://drive.google.com/file/d/1-gO-MhXTIPbHRkyJQo8nTxly72gT3r9t/view?usp=drive_link",
	"lock_hourly.csv": "https://drive.google.com/file/d/1fr8nkPmsSwTv27FfnPOnxHt3V3Z-L5fT/view?usp=drive_link",
	"yellow_tripdata_1.parquet": "https://drive.google.com/file/d/17v0eFEontYEKtGoqyB0v9rj_BEDc7snA/view?usp=drive_link",
	"yellow_tripdata_2.parquet": "https://drive.google.com/file/d/1N-dRuGdd_lOYGAbdbgJJWMyIsV_lz057/view?usp=drive_link",
	"yellow_tripdata_3.parquet": "https://drive.google.com/file/d/1oUxC0cLWqOatddyT8aB06VvFFZY0-lu2/view?usp=drive_link",
}


def download_datasets():
	for name, url in DATASETS.items():
		save_path = f"{DATASET_DIR}/{name}"
		print(f"Downloading {name}")
		gdown.download(url=url, output=save_path, quiet=False)

		# special zip case
		if ".zip" in save_path:
			print("Extracting zip...")
			with zipfile.ZipFile(save_path, "r") as zip:
				zip.extractall(f'{DATASET_DIR}/{name.replace(".zip", "")}')
			os.remove(save_path)


if __name__ == "__main__":
	download_datasets()
