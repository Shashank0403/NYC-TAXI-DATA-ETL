import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1767737656823 = glueContext.create_dynamic_frame.from_catalog(database="csvtransform", table_name="inputdatastore_file_transform", transformation_ctx="AWSGlueDataCatalog_node1767737656823")

# Script generated for node SQL Query
SqlQuery5395 = '''
select * from myDataSource

'''
SQLQuery_node1767737708846 = sparkSqlQuery(glueContext, query = SqlQuery5395, mapping = {"myDataSource":AWSGlueDataCatalog_node1767737656823}, transformation_ctx = "SQLQuery_node1767737708846")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1767905104449_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [ColumnExists "locationid"
        
    ]
"""

EvaluateDataQuality_node1767905104449 = EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1767737656823, ruleset=EvaluateDataQuality_node1767905104449_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767905104449", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767737708846, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767737647416", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767737724267 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1767737708846, connection_type="s3", format="glueparquet", connection_options={"path": "s3://inputdatastore-file-transform", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1767737724267")

job.commit()