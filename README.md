## Installation
This project uses `uv` to manage dependencies

Run `uv sync` to update dependencies


## Running

### Creating the table
```
uv run main.py
```
- this will run the installer (if necessary) and create the tables, transform, and validate them


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

**5. Benchmark partitioned tables**

The code for creating and benchmarking the two partitioned versions of `taxi_trips` is available in `task_6.ipynb`. All sections should be run in sequence with possible exception of the last one, the 'Drop partitioned tables' section, which is useful for returing to the clean previous state before benchmarking. 
