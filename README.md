# Processo de ETL com Apache Spark e Python na plataforma AWS

## Pyspark e AWS

Este processo de ETL foi criado para ser utilizado na plataforma AWS com o Glue.
Crie o crawler para ler os dados no Amazon S3 e uma conexão ao banco de dados Redshift.

O script cria o spark context e o glue context, lê todos os arquivos que estão dentro do bucket indicado. Joga dentro de um dataframe do spark, atualiza os nomes das colunas e especifica os tipos de dados das colunas.
Obs.: Se você não fizer isso o spark vai mapear os tipos e criar as colunas na tabela com base neste mapeamento.

Logo após, uma cópia dos dados será salvo no S3 no formato parquet e também na tabela indicada no Redshift.
