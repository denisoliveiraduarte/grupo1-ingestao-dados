import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Bancos
Bancos_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://denis-ingestao-bancos-grupo1/banco/bancos.tsv"],
        "recurse": True,
    },
    transformation_ctx="Bancos_node1",
)

# Script generated for node Reclamacoes
Reclamacoes_node1691519591699 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ";",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://denis-ingestao-bancos-grupo1/reclamacao/"],
        "recurse": True,
    },
    transformation_ctx="Reclamacoes_node1691519591699",
)

# Script generated for node Join
Bancos_node1DF = Bancos_node1.toDF()
Reclamacoes_node1691519591699DF = Reclamacoes_node1691519591699.toDF()
Join_node1691519637656 = DynamicFrame.fromDF(
    Bancos_node1DF.join(
        Reclamacoes_node1691519591699DF,
        (Bancos_node1DF["CNPJ"] == Reclamacoes_node1691519591699DF["CNPJ IF"]),
        "right",
    ),
    glueContext,
    "Join_node1691519637656",
)

# Script generated for node Gravação join - bancos-reclamações
Gravaojoinbancosreclamaes_node1691519673782 = (
    glueContext.write_dynamic_frame.from_options(
        frame=Join_node1691519637656,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://denis-ingestao-bancos-grupo1/Join-Bancos-Reclamacoes/",
            "partitionKeys": ["Ano", "Trimestre", "CNPJ IF", "Instituicao financeira"],
        },
        transformation_ctx="Gravaojoinbancosreclamaes_node1691519673782",
    )
)

job.commit()
