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

# Script generated for node Empregados
Empregados_node1691609162114 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://denis-ingestao-bancos-grupo1/Join-Banco-Empregados/"],
        "recurse": True,
    },
    transformation_ctx="Empregados_node1691609162114",
)

# Script generated for node Reclamacoes-2021-01
Reclamacoes202101_node1691609106074 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://denis-ingestao-bancos-grupo1/Join-Bancos-Reclamacoes/Ano=2021/Trimestre=1/"
        ],
        "recurse": True,
    },
    transformation_ctx="Reclamacoes202101_node1691609106074",
)

# Script generated for node Reclamacoes-2021-02
Reclamacoes202102_node1691612790046 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://denis-ingestao-bancos-grupo1/Join-Bancos-Reclamacoes/Ano=2021/Trimestre=2/"
        ],
        "recurse": True,
    },
    transformation_ctx="Reclamacoes202102_node1691612790046",
)

# Script generated for node Join-2021-01
Reclamacoes202101_node1691609106074DF = Reclamacoes202101_node1691609106074.toDF()
Empregados_node1691609162114DF = Empregados_node1691609162114.toDF()
Join202101_node1691612170840 = DynamicFrame.fromDF(
    Reclamacoes202101_node1691609106074DF.join(
        Empregados_node1691609162114DF,
        (
            Reclamacoes202101_node1691609106074DF["CNPJ"]
            == Empregados_node1691609162114DF["right_cnpj"]
        ),
        "outer",
    ),
    glueContext,
    "Join202101_node1691612170840",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691613395782 = ApplyMapping.apply(
    frame=Reclamacoes202102_node1691612790046,
    mappings=[
        ("Segmento", "null", "2021_02_Segmento", "null"),
        ("CNPJ", "null", "2021_02_CNPJ", "null"),
        ("Nome", "null", "2021_02_Nome", "null"),
        ("Categoria", "string", "2021_02_Categoria", "string"),
        ("Tipo", "string", "2021_02_Tipo", "string"),
        ("Indice", "string", "2021_02_Indice", "string"),
        (
            "Quantidade de reclamacoes reguladas procedentes",
            "string",
            "2021_02_Quantidade de reclamacoes reguladas procedentes",
            "string",
        ),
        (
            "Quantidade de reclamacoes reguladas outras",
            "string",
            "2021_02_Quantidade de reclamacoes reguladas outras",
            "string",
        ),
        (
            "Quantidade de reclamcoes nao reguladas",
            "string",
            "2021_02_Quantidade de reclamcoes nao reguladas",
            "string",
        ),
        (
            "Quantidade total de reclamacoes",
            "string",
            "2021_02_Quantidade total de reclamacoes",
            "string",
        ),
        (
            "Quantidade total de clientes CCS e SCR",
            "string",
            "2021_02_Quantidade total de clientes CCS e SCR",
            "string",
        ),
        (
            "Quantidade de clientes CCS",
            "string",
            "2021_02_Quantidade de clientes CCS",
            "string",
        ),
        (
            "Quantidade de clientes SCR",
            "string",
            "2021_02_Quantidade de clientes SCR",
            "string",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691613395782",
)

# Script generated for node Join-2021-02
Join202101_node1691612170840DF = Join202101_node1691612170840.toDF()
RenamedkeysforJoin_node1691613395782DF = RenamedkeysforJoin_node1691613395782.toDF()
Join202102_node1691613375473 = DynamicFrame.fromDF(
    Join202101_node1691612170840DF.join(
        RenamedkeysforJoin_node1691613395782DF,
        (
            Join202101_node1691612170840DF["CNPJ"]
            == RenamedkeysforJoin_node1691613395782DF["2021_02_CNPJ"]
        ),
        "outer",
    ),
    glueContext,
    "Join202102_node1691613375473",
)

# Script generated for node Change Schema
ChangeSchema_node1691613531692 = ApplyMapping.apply(
    frame=Join202102_node1691613375473,
    mappings=[
        ("Segmento", "string", "Segmento", "string"),
        ("CNPJ", "string", "CNPJ", "string"),
        ("Nome", "string", "Nome", "string"),
        ("Indice", "string", "Indice_reclamacoes_2021_01", "string"),
        (
            "Quantidade total de reclamacoes",
            "string",
            "Qtd_total_reclamacoes_2021_01",
            "string",
        ),
        (
            "Quantidade total de clientes CCS e SCR",
            "string",
            "Qtd_total_clientes_2021_01",
            "string",
        ),
        (
            "right_remuneracao_beneficios",
            "string",
            "right_remuneracao_beneficios",
            "string",
        ),
        ("right_geral", "string", "right_geral", "string"),
        ("2021_02_Indice", "string", "Indice_reclamacoes_2021_02", "string"),
        (
            "2021_02_Quantidade total de reclamacoes",
            "string",
            "Qtd_total_reclamacoes_2021_02",
            "string",
        ),
        (
            "2021_02_Quantidade total de clientes CCS e SCR",
            "string",
            "2021_02_Quantidade total de clientes CCS e SCR",
            "string",
        ),
    ],
    transformation_ctx="ChangeSchema_node1691613531692",
)

job.commit()
