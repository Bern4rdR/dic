# Use delta features if needed (DeltaTable, etc.)
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
from pathlib import Path

from pyspark.sql import functions as F

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from custom_builder import builder


def train(spark):
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
	from pyspark.ml.evaluation import RegressionEvaluator

	features_df = spark.table("features")



	#------------#
	# 1. Columns #
	#------------#

	categorical_cols = [
		"pu_weekday",
		"pu_hour",
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
		numTrees=30,
		maxDepth=5,
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


	# --------------------------------------------------
	# 8. Predict
	# --------------------------------------------------

	predictions = model.transform(test)

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

	print("RMSE:", evaluator_rmse.evaluate(predictions))
	print("MAE :", evaluator_mae.evaluate(predictions))
	print("R²  :", evaluator_r2.evaluate(predictions))


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()

	print(f'current database: {spark.catalog.currentDatabase()}')
	print(f'spark tables: {spark.catalog.listTables()}')

	#-------------------------#
	# Prediction Target Table #
	#-------------------------#

	if spark.catalog.tableExists("prediction_target"):
		prediction_target_df = spark.table("prediction_target")
	else:
		query = f"""
			SELECT date_format(pu_datetime, 'EEE') AS pu_weekday, hour(pu_datetime) AS pu_hour, COUNT(*) AS demand
			FROM integrated_taxi_trips
			GROUP BY pu_hour, pu_weekday
			ORDER BY pu_weekday, demand DESC
			"""
		prediction_target_df = spark.sql(query)
		prediction_target_df.show(10)

		# write prediction_target_df to prediction_target table
		prediction_target_df.write.format("delta").mode("overwrite").saveAsTable("prediction_target")

	prediction_target_df.show(10)

	
	#-------------------#
	# Feature selection #
	#-------------------#

	if spark.catalog.tableExists("features"):
		features_df = spark.table("features")
	else:
		# read integrated_taxi_trips from the default database
		integrated_taxi_trips_df = spark.read.table("integrated_taxi_trips")
		integrated_taxi_trips_df.show(10)

		# select feature columns from integrated_taxi_trips_df
		feature_columns = ["pu_datetime", "temp", "rhum", "prcp", "cldc"] # "snwd" would be good to have but it is Null everywhere?
		features_df = integrated_taxi_trips_df.select(*feature_columns)
		features_df.show(10)

		# transform features_df to add pu_weekday and pu_hour columns and drop pu_datetime column
		features_df = features_df.withColumn("pu_weekday", F.date_format("pu_datetime", "EEE")).withColumn("pu_hour", F.hour("pu_datetime")).drop("pu_datetime")

		# Join features_df with prediction_target_df on pu_weekday and pu_hour
		features_df = features_df.join(prediction_target_df, on=["pu_weekday", "pu_hour"], how="left")
		features_df = features_df.filter(F.col("demand").isNotNull()) # just to be sure we don't have any nulls in the demand column

		features_df.write.format("delta").mode("overwrite").saveAsTable("features")

	features_df.show(10)

	#--------------#
	# demand stats #
	#--------------#

	# features_df.groupBy("demand") \
	# 	.count() \
	# 	.orderBy("demand") \
	# 	.show(50)

	features_df.select(
		F.min("demand").alias("min"),
		F.max("demand").alias("max"),
		F.avg("demand").alias("mean"),
		F.variance("demand").alias("variance")
	).show()


	




