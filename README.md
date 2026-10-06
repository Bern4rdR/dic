## Installation
This project uses `uv` to manage dependencies

Run `uv add -r requirements.txt` to import dependencies

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

# Week 2: Analysis

The implementations for all tasks in Week 2 can be seen in the Jupyter Notebooks in the `Week 2` folder. 

# Week 3: Operating and Maintaining the Urban Data Platform

To generate the incremental update datasets, the Task 1 notebooks in `Week 3/` corresponding to each dataset should be run first. These can then be ingested using `Task 1 Pipeline.ipynb`. To monitor the pipeline executions just performed, `Task 3 Monitoring.ipynb` can the run. 

Schema evolution is handled through custom configuration files seen in `ingestion_update_configuration/` and all evaluation experiment results found in the report can be retrieved through checking the `Task 3 Monitoring.ipynb` notebook output as well as the logging files produced under `logs/`.

# Week 4: Building Machine Learning Pipelines on the Urban Data Platform

By running `Week 4/Task_2.py`, the training dataset can be generated, the feature engineering pipeline applied, and the model trained and evaluated. 

The two approaches A and B mentioned in the report differ in setting `compute_features_from_source = True | False`. The model can be retrained when new data becomes available by re-running the python script. 
