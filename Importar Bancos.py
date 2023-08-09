import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Empregados
Empregados_node1691608237325 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://denis-ingestao-bancos-grupo1/empregados/"],
        "recurse": True,
    },
    transformation_ctx="Empregados_node1691608237325",
)

# Script generated for node Bancos
Bancos_node1691365495642 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="Bancos_node1691365495642",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691608459293 = ApplyMapping.apply(
    frame=Empregados_node1691608237325,
    mappings=[
        ("employer_name", "string", "right_employer_name", "string"),
        ("reviews_count", "string", "right_reviews_count", "string"),
        ("culture_count", "string", "right_culture_count", "string"),
        ("salaries_count", "string", "right_salaries_count", "string"),
        ("benefits_count", "string", "right_benefits_count", "string"),
        ("employer-website", "string", "right_employer-website", "string"),
        ("employer-headquarters", "string", "right_employer-headquarters", "string"),
        ("employer_founded", "string", "right_employer_founded", "string"),
        ("employer_industry", "string", "right_employer_industry", "string"),
        ("employer_revenue", "string", "right_employer_revenue", "string"),
        ("url", "string", "right_url", "string"),
        ("geral", "string", "right_geral", "string"),
        ("cultura_valores", "string", "right_cultura_valores", "string"),
        ("diversidade_inclusao", "string", "right_diversidade_inclusao", "string"),
        ("qualidade_vida", "string", "right_qualidade_vida", "string"),
        ("alta_lideranca", "string", "right_alta_lideranca", "string"),
        ("remuneracao_beneficios", "string", "right_remuneracao_beneficios", "string"),
        ("oportunidades_carreira", "string", "right_oportunidades_carreira", "string"),
        (
            "recomendam_para_outras_pessoas_porcentagem",
            "string",
            "right_recomendam_para_outras_pessoas_porcentagem",
            "string",
        ),
        (
            "perspectiva_positiva_da_empresa_porcentagem",
            "string",
            "right_perspectiva_positiva_da_empresa_porcentagem",
            "string",
        ),
        ("CNPJ", "string", "right_CNPJ", "string"),
        ("nome", "string", "right_nome", "string"),
        ("match_percent", "string", "right_match_percent", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691608459293",
)

# Script generated for node Join
Join_node1691608439971 = Join.apply(
    frame1=Bancos_node1691365495642,
    frame2=RenamedkeysforJoin_node1691608459293,
    keys1=["CNPJ"],
    keys2=["right_CNPJ"],
    transformation_ctx="Join_node1691608439971",
)

# Script generated for node Grava
Grava_node1691608642736 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1691608439971,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://denis-ingestao-bancos-grupo1/Join-Banco-Empregados/",
        "partitionKeys": ["CNPJ"],
    },
    transformation_ctx="Grava_node1691608642736",
)

job.commit()
