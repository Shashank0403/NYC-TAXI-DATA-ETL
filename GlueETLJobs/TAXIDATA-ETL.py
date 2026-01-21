import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import re

# Script generated for node removing null values
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    cleaned_df = df.select(non_null_cols)
    result = DynamicFrame.fromDF(cleaned_df, glueContext, "result")
    return DynamicFrameCollection({"CustomTransform0": result}, glueContext)
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

# Script generated for node Amazon S3
AmazonS3_node1767727861126 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://shashank-datalake/bronze/ingest/batch-taxidata/yellow_tripdata_2025-08.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1767727861126")

# Script generated for node Amazon S3
AmazonS3_node1767730455149 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://shashank-datalake/bronze/ingest/batch-taxidata/taxi_zone_lookup.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1767730455149")

# Script generated for node Amazon S3
AmazonS3_node1767729423769 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://shashank-datalake/bronze/ingest/batch-taxidata/taxi_zone_lookup.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1767729423769")

# Script generated for node removing null values
removingnullvalues_node1767728160178 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1767727861126": AmazonS3_node1767727861126}, glueContext))

# Script generated for node Change Schema
ChangeSchema_node1767730500438 = ApplyMapping.apply(frame=AmazonS3_node1767730455149, mappings=[("locationid", "string", "nydo_locationid", "int"), ("borough", "string", "nydo_borough", "string"), ("zone", "string", "nydo_zone", "string"), ("service_zone", "string", "nydo_service_zone", "string")], transformation_ctx="ChangeSchema_node1767730500438")

# Script generated for node Change Schema
ChangeSchema_node1767729467813 = ApplyMapping.apply(frame=AmazonS3_node1767729423769, mappings=[("locationid", "string", "ny_locationid", "int"), ("borough", "string", "ny_borough", "string"), ("zone", "string", "ny_zone", "string"), ("service_zone", "string", "ny_service_zone", "string")], transformation_ctx="ChangeSchema_node1767729467813")

# Script generated for node Select From Collection
SelectFromCollection_node1767728770633 = SelectFromCollection.apply(dfc=removingnullvalues_node1767728160178, key=list(removingnullvalues_node1767728160178.keys())[0], transformation_ctx="SelectFromCollection_node1767728770633")

# Script generated for node Filter
Filter_node1767729261016 = Filter.apply(frame=SelectFromCollection_node1767728770633, f=lambda row: (row["passenger_count"] >= 2 and row["trip_distance"] >= 1), transformation_ctx="Filter_node1767729261016")

# Script generated for node Join
Join_node1767729555036 = Join.apply(frame1=ChangeSchema_node1767729467813, frame2=Filter_node1767729261016, keys1=["ny_locationid"], keys2=["PULocationID"], transformation_ctx="Join_node1767729555036")

# Script generated for node Renamed keys for Join-2
RenamedkeysforJoin2_node1767732511622 = ApplyMapping.apply(frame=Join_node1767729555036, mappings=[("ny_locationid", "int", "ny_locationid", "int"), ("ny_borough", "string", "right_ny_borough", "string"), ("ny_zone", "string", "right_ny_zone", "string"), ("ny_service_zone", "string", "right_ny_service_zone", "string"), ("VendorID", "int", "right_VendorID", "int"), ("passenger_count", "long", "right_passenger_count", "long"), ("trip_distance", "double", "right_trip_distance", "double"), ("RatecodeID", "long", "right_RatecodeID", "long"), ("store_and_fwd_flag", "string", "right_store_and_fwd_flag", "string"), ("PULocationID", "int", "right_PULocationID", "int"), ("DOLocationID", "int", "right_DOLocationID", "int"), ("payment_type", "long", "right_payment_type", "long"), ("fare_amount", "double", "right_fare_amount", "double"), ("extra", "double", "right_extra", "double"), ("mta_tax", "double", "right_mta_tax", "double"), ("tip_amount", "double", "right_tip_amount", "double"), ("tolls_amount", "double", "right_tolls_amount", "double"), ("improvement_surcharge", "double", "right_improvement_surcharge", "double"), ("total_amount", "double", "right_total_amount", "double"), ("congestion_surcharge", "double", "right_congestion_surcharge", "double"), ("Airport_fee", "double", "right_Airport_fee", "double"), ("cbd_congestion_fee", "double", "right_cbd_congestion_fee", "double")], transformation_ctx="RenamedkeysforJoin2_node1767732511622")

# Script generated for node Join-2
Join2_node1767730600100 = Join.apply(frame1=ChangeSchema_node1767730500438, frame2=RenamedkeysforJoin2_node1767732511622, keys1=["nydo_locationid"], keys2=["right_DOLocationID"], transformation_ctx="Join2_node1767730600100")

# Script generated for node Change Schema
ChangeSchema_node1767730717055 = ApplyMapping.apply(frame=Join2_node1767730600100, mappings=[("nydo_locationid", "int", "nydo_locationid", "long"), ("nydo_borough", "string", "nydo_borough", "string"), ("nydo_zone", "string", "nydo_zone", "string"), ("nydo_service_zone", "string", "nydo_service_zone", "string"), ("ny_locationid", "int", "ny_locationid", "long"), ("right_ny_borough", "string", "right_ny_borough", "string"), ("right_ny_zone", "string", "right_ny_zone", "string"), ("right_ny_service_zone", "string", "right_ny_service_zone", "string"), ("right_VendorID", "int", "right_VendorID", "int"), ("right_passenger_count", "long", "right_passenger_count", "long"), ("right_trip_distance", "double", "right_trip_distance", "double"), ("right_RatecodeID", "long", "right_RatecodeID", "long"), ("right_store_and_fwd_flag", "string", "right_store_and_fwd_flag", "string"), ("right_PULocationID", "int", "right_PULocationID", "int"), ("right_DOLocationID", "int", "right_DOLocationID", "int"), ("right_payment_type", "long", "right_payment_type", "long"), ("right_fare_amount", "double", "right_fare_amount", "double"), ("right_extra", "double", "right_extra", "double"), ("right_mta_tax", "double", "right_mta_tax", "double"), ("right_tip_amount", "double", "right_tip_amount", "double"), ("right_tolls_amount", "double", "right_tolls_amount", "double"), ("right_improvement_surcharge", "double", "right_improvement_surcharge", "double"), ("right_total_amount", "double", "right_total_amount", "double"), ("right_congestion_surcharge", "double", "right_congestion_surcharge", "double"), ("right_Airport_fee", "double", "right_Airport_fee", "double"), ("right_cbd_congestion_fee", "double", "right_cbd_congestion_fee", "double")], transformation_ctx="ChangeSchema_node1767730717055")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1767730717055, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767727836659", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767730817701 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1767730717055, connection_type="s3", format="glueparquet", connection_options={"path": "s3://shashank-datalake/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1767730817701")

job.commit()