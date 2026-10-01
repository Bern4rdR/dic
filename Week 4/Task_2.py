# Use delta features if needed (DeltaTable, etc.)
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from custom_builder import builder


def evaluate_model(model, test_set):
	from pyspark.ml.evaluation import RegressionEvaluator

	label_col = "demand"

	# --------------------------------------------------
	# 8. Predict
	# --------------------------------------------------

	predictions = model.transform(test_set)

	predictions = predictions.withColumn(
		"prediction_count",
		F.greatest(
			F.lit(0),
			F.round("prediction")
		).cast("int")
	)

	predictions.select(
		# "id",
		label_col,
		"prediction",
		"prediction_count"
	).show()

	# Evalution

	evaluator_rmse = RegressionEvaluator(
		labelCol=label_col,
		predictionCol="prediction",
		metricName="rmse"
	)

	evaluator_mae = RegressionEvaluator(
		labelCol=label_col,
		predictionCol="prediction",
		metricName="mae"
	)

	evaluator_r2 = RegressionEvaluator(
		labelCol=label_col,
		predictionCol="prediction",
		metricName="r2"
	)

	rmse = evaluator_rmse.evaluate(predictions)
	mae = evaluator_mae.evaluate(predictions)
	r2 = evaluator_r2.evaluate(predictions)

	print("RMSE:", rmse)
	print("MAE :", mae)
	print("R²  :", r2)

	return rmse, mae, r2


def train_model(features_df, nr_trees=80, max_depth=25):
	# ---------#
	# ML setup #
	# ---------#

	from pyspark.ml import Pipeline
	from pyspark.ml.feature import (
		StringIndexer,
		OneHotEncoder,
		VectorAssembler
	)
	from pyspark.ml.regression import RandomForestRegressor

	#------------#
	# 1. Columns #
	#------------#

	categorical_cols = [
		"pu_weekday",
		"pu_hour",
		"pu_county",
		"cldc"
	]

	numeric_cols = [
		"temp",
		"rhum",
		"prcp",
		#"snwd"
	]

	label_col = "demand"

	# --------------------------------------------------
	# 2. Encode categorical features
	# --------------------------------------------------
	
	from pyspark.ml.feature import Imputer

	# Impute missing values for numeric columns
	imputer = Imputer(
		inputCols=numeric_cols,
		outputCols=[f"{c}_imputed" for c in numeric_cols],
		strategy="median"
	)

	# for c in categorical_cols:
	# 	features_df = features_df.withColumn(
	# 		c,
	# 		F.coalesce(F.col(c), F.lit("__MISSING__"))
	# 	)

	# Encode categorical columns using StringIndexer and OneHotEncoder
	indexers = [
		StringIndexer(
			inputCol=c,
			outputCol=f"{c}_idx",
			handleInvalid="keep"
		)
		for c in categorical_cols
	]
	encoder = OneHotEncoder(
		inputCols=[f"{c}_idx" for c in categorical_cols],
		outputCols=[f"{c}_ohe" for c in categorical_cols]
	)

	imputed_cols = [f"{c}_imputed" for c in numeric_cols]
	
	# --------------------------------------------------
	# 3. Assemble everything
	# --------------------------------------------------

	assembler = VectorAssembler(
		inputCols=(
			imputed_cols +
			[f"{c}_ohe" for c in categorical_cols]
		),
		outputCol="features",
		handleInvalid="keep"
	)

	# --------------------------------------------------
	# 4. Regression model
	# --------------------------------------------------

	rf = RandomForestRegressor(
		featuresCol="features",
		labelCol=label_col,
		numTrees=nr_trees,
		maxDepth=max_depth,
		minInstancesPerNode=5,
		seed=42
	)


	# --------------------------------------------------
	# 5. Pipeline
	# --------------------------------------------------

	pipeline = Pipeline(
		stages=[
			imputer,
			*indexers,
			encoder,
			assembler,
			rf
		]
	)


	# --------------------------------------------------
	# 6. Train/test split
	# --------------------------------------------------

	train, test = features_df.randomSplit(
		[0.8, 0.2],
		seed=42
	)


	# --------------------------------------------------
	# 7. Train
	# --------------------------------------------------

	model = pipeline.fit(train)

	return model, train, test


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()

	print(f'current database: {spark.catalog.currentDatabase()}')
	print(f'spark tables: {spark.catalog.listTables()}')

	recompute_features = False

	do_features_summary_stats = True

	grid_search = False
	do_train = True
	do_evaluate = True

	#-------------------#
	# Feature selection #
	#-------------------#

	if recompute_features or not spark.catalog.tableExists("features"):
		spark.sql("DROP TABLE IF EXISTS features")

		query = f"""
			SELECT
				TO_DATE(pu_datetime) AS pu_date,
				date_format(pu_datetime, 'EEE') AS pu_weekday,
				hour(pu_datetime) AS pu_hour,
				pu_county,
				temp, rhum, prcp, cldc,
				COUNT(*) AS demand
			FROM integrated_taxi_trips
			GROUP BY pu_date, pu_weekday, pu_hour, pu_county, temp, rhum, prcp, cldc
			ORDER BY demand DESC
			"""
		features_df = spark.sql(query)

		features_df = features_df.filter(F.col("demand").isNotNull()) # just to be sure we don't have any nulls in the demand column

		# write features_df to features table
		features_df.write.format("delta").mode("overwrite").saveAsTable("features")
	else:
		features_df = spark.table("features")
	
		

	features_df.show(10)
		
	#----------------#
	# features stats #
	#----------------#

	if do_features_summary_stats:
		print(features_df.columns)
		features_df.summary().show()

	# features_df.select(
	# 	F.min("demand").alias("min"),
	# 	F.max("demand").alias("max"),
	# 	F.avg("demand").alias("mean"),
	# 	F.variance("demand").alias("variance")
	# ).show()


	#----------------#
	# Grid search    #
	#----------------#

	if grid_search:
		features_df.persist(StorageLevel.MEMORY_AND_DISK)

		best_test_rmse = float("inf")
		best_test_config = None

		best_train_rmse = float("inf")
		best_train_config = None

		for nr_trees in range(10, 90, 10):
			for max_depth in range(5, 30, 5):
				print(f"Training model with nr_trees={nr_trees}, max_depth={max_depth}")
				model, train_set, test_set = train_model(features_df, nr_trees=nr_trees, max_depth=max_depth)
				rmse, mae, r2 = evaluate_model(model, test_set)

				if rmse < best_test_rmse:
					best_test_rmse = rmse
					best_test_config = (nr_trees, max_depth)

				train_rmse, train_mae, train_r2 = evaluate_model(model, train_set)
				if train_rmse < best_train_rmse:
					best_train_rmse = train_rmse
					best_train_config = (nr_trees, max_depth)

		print(f"Best config: nr_trees={best_test_config[0]}, max_depth={best_test_config[1]}, RMSE={best_test_rmse}")
		print(f"Best train config: nr_trees={best_train_config[0]}, max_depth={best_train_config[1]}, RMSE={best_train_rmse}")

	#-----------------#
	# Train the model #
	#-----------------#

	features_df.persist(StorageLevel.MEMORY_AND_DISK)

	if do_train:
		model, _, test_set = train_model(features_df)

		# only evaluate / save model if a new one has been trained
		if do_evaluate:
			evaluate_model(model, test_set)

		# prompt user to save model
		if input("Save model? (Y/n): ").capitalize() == "Y":
			model.write().overwrite().save("models/")
