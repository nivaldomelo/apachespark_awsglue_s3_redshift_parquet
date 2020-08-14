# Processo de ETL com Apache Spark e Python na plataforma AWS

## :rocket:  Pyspark e AWS

Este exemplo foi feito com o dataset mtcars, muito utilizado no aprendizado de análise de dados.

Os dados foram extraídos da revista Motor Trend US de 1974 e abrangem o consumo de combustível e 10 aspectos do design e desempenho de automóveis para 32 automóveis (modelos de 1973 a 1974).

Este processo de ETL foi criado para ser utilizado na plataforma AWS com o Glue.
Crie o crawler para ler os dados no Amazon S3 e uma conexão ao banco de dados Redshift.

O script cria o spark context e o glue context, lê todos os arquivos que estão dentro do bucket indicado. Joga dentro de um dataframe do spark, atualiza os nomes das colunas e especifica os tipos de dados das colunas.
**Obs.: Se você não fizer isso o spark vai mapear os tipos e criar as colunas na tabela com base neste mapeamento.**

Logo após, uma cópia dos dados será salvo no S3 no formato parquet e também na tabela indicada no Redshift.

Mais informações: 
[Spark](http://spark.apache.org/) |
[Amazon S3](https://aws.amazon.com/pt/s3/) | 
[Amazon Glue](https://aws.amazon.com/pt/glue/) |
[Amazon Redshift](https://aws.amazon.com/pt/redshift/) 
