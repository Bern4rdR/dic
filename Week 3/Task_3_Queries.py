def table_most_failures(spark):
    res = spark.sql(f"""
        SELECT table_name, COUNT(*) AS failure_count
        FROM pipeline_monitor
        WHERE validation_failures > 0
        GROUP BY table_name
        ORDER BY failure_count DESC
    """)
    return res

def table_average_pipeline_execution_time(spark):
    res = spark.sql(f"""
        SELECT table_name, AVG(timestampdiff(SECOND, execution_start_time, execution_end_time)) AS average_execution_time
        FROM pipeline_monitor
        GROUP BY table_name
        ORDER BY average_execution_time DESC
    """)
    return res
