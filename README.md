## Installation
This project uses `uv` to manage dependencies

Run `uv sync` to update dependencies


## Running

### Creating the table
**1. Download the datasets**
```
uv run download_datasets.py
```
- this will download the datasets to the `data/` directory

**2. Format the datasets**
```
uv run main.py
```
- this will format the datasets and store them in `spark_project/`

**3. Enrich the datasets**
```
uv run enrich.py
```
- this will create an enriched table from the validated data


### Creating the tables (Manually)
**1. Download the datasets**
```
uv run download_datasets.py
```
- this will download the datasets to the `data/` directory


**2. Ingest the datasets**
```
uv run ingestion.py
```
- this will ingest the datasets and store them in `spark_project/`


**3. Validate the tables**
```
uv run validation.py
```
- this validates the table contsents and removes invalid rows


**4. Create enriched table**
```
uv run enrich.py
```
- this creates a enriched table from the validated data
	- containing weather and air quality data connected to each taxi trip
