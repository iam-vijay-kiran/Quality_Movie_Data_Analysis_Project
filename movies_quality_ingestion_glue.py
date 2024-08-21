import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3-data-source
S3datasource_node1724170600317 = glueContext.create_dynamic_frame.from_catalog(database="redshift-desti-mv-data-analysis", table_name="imdb_movies_rating_csv", transformation_ctx="S3datasource_node1724170600317")

# Script generated for node data-quality-checks
dataqualitychecks_node1724170763782_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

dataqualitychecks_node1724170763782 = EvaluateDataQuality().process_rows(frame=S3datasource_node1724170600317, ruleset=dataqualitychecks_node1724170763782_ruleset, publishing_options={"dataQualityEvaluationContext": "dataqualitychecks_node1724170763782", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1724208219489 = SelectFromCollection.apply(dfc=dataqualitychecks_node1724170763782, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1724208219489")

# Script generated for node ruleOutcomes
ruleOutcomes_node1724208211796 = SelectFromCollection.apply(dfc=dataqualitychecks_node1724170763782, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1724208211796")

# Script generated for node Conditional Router
ConditionalRouter_node1724208909857 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1724208219489,
  group_filters = [GroupFilter(name = "output_group_1", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1724208910558 = SelectFromCollection.apply(dfc=ConditionalRouter_node1724208909857, key="default_group", transformation_ctx="default_group_node1724208910558")

# Script generated for node output_group_1
output_group_1_node1724208910712 = SelectFromCollection.apply(dfc=ConditionalRouter_node1724208909857, key="output_group_1", transformation_ctx="output_group_1_node1724208910712")

# Script generated for node drop-column
dropcolumn_node1724210794908 = ApplyMapping.apply(frame=default_group_node1724208910558, mappings=[("poster_link", "string", "poster_link", "string"), ("series_title", "string", "series_title", "string"), ("released_year", "string", "released_year", "string"), ("certificate", "string", "certificate", "string"), ("runtime", "string", "runtime", "string"), ("genre", "string", "genre", "string"), ("imdb_rating", "double", "imdb_rating", "double"), ("overview", "string", "overview", "string"), ("meta_score", "long", "meta_score", "long"), ("director", "string", "director", "string"), ("star1", "string", "star1", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("star4", "string", "star4", "string"), ("no_of_votes", "long", "no_of_votes", "long"), ("gross", "string", "gross", "string"), ("DataQualityRulesPass", "array", "DataQualityRulesPass", "array"), ("DataQualityRulesFail", "array", "DataQualityRulesFail", "array"), ("DataQualityRulesSkip", "array", "DataQualityRulesSkip", "array"), ("DataQualityEvaluationResult", "string", "DataQualityEvaluationResult", "string")], transformation_ctx="dropcolumn_node1724210794908")

# Script generated for node Amazon S3
AmazonS3_node1724208759834 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1724208211796, connection_type="s3", format="json", connection_options={"path": "s3://movie-data-analysis-pro1/rule_outcome/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724208759834")

# Script generated for node failed-records
failedrecords_node1724210698496 = glueContext.write_dynamic_frame.from_options(frame=output_group_1_node1724208910712, connection_type="s3", format="json", connection_options={"path": "s3://movie-data-analysis-pro1/bad_records/", "partitionKeys": []}, transformation_ctx="failedrecords_node1724210698496")

# Script generated for node redshift-load-data
redshiftloaddata_node1724210974241 = glueContext.write_dynamic_frame.from_catalog(frame=dropcolumn_node1724210794908, database="redshift-desti-mv-data-analysis", table_name="dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://noob2-gds-temp-1",additional_options={"aws_iam_role": "arn:aws:iam::513488784101:role/redshift_role"}, transformation_ctx="redshiftloaddata_node1724210974241")

job.commit()
