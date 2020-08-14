import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

# Criando o SC e GLUE Context
run_date = datetime.datetime.now().strftime("%Y%m%d%H%M")
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
...

# Escrevendo no log
logger.info("Processo de ETL Mtcars - Início")

# Carregando os arquivos que estão no Bucket
load_mtcars = spark.read.format(
    "com.databricks.spark.csv").option(
    "header", "true").option(
    "inferSchema", "true").option(
    "sep", "|").load(
    's3://meu-bucket-no-s3/datasets/mtcars/')

mtcars = load_mtcars.alias("mtcars")
mtcars.distinct().cache()

logger.info("Seleção das Variáveis")
select = mtcars.select(
    mtcars.model.cast(StringType()).alias('modelo'),
    mtcars.mpg.cast(DoubleType()).alias('miles_gallon'),
    mtcars.cyl.cast(IntegerType()).alias('number_of_cylinders'),
    mtcars.disp.cast(DoubleType()).alias('displacement'),
    mtcars.hp.cast(IntegerType()).alias('gross_horsepower'),
    mtcars.drat.cast(DoubleType()).alias('rear_axle_ratio'),
    mtcars.wt.cast(DoubleType()).alias('weight'),
    mtcars.qsec.cast(DoubleType()).alias('1_4_mile_time'),
    mtcars.vs.cast(IntegerType()).alias('v_s'),
    mtcars.am.cast(IntegerType()).alias('transmission'),
    mtcars.gear.cast(IntegerType()).alias('number_of_forward_gears'),
    mtcars.carb.cast(IntegerType()).alias('number_of_carburetors')
)

outpath = "{}/{}".format(
    "s3://meu-bucket-no-s3/output/mtcars/", run_date)

logger.info("Gravando os dados no Redshift e salvando uma cópia em Parquet no S3")
# Convertendo o Spark DataFrame para o Glue Dynamic Frame
glue_dynamic_frame_union = DynamicFrame.fromDF(select, glueContext, "select")
glueContext.write_dynamic_frame.from_options(
    frame=glue_dynamic_frame_union, connection_type="s3", connection_options={"path": outpath}, format="parquet")
glueContext.write_dynamic_frame.from_jdbc_conf(frame=glue_dynamic_frame_union, catalog_connection="Redshift", connection_options={
                                               "dbtable":  "meuschema.mtcars", "database": "seubancodedados"}, redshift_tmp_dir="s3://meu-bucket-no-s3/output/_redshift_tmpdir")
logger.info("Processo Finalizado!!!")
