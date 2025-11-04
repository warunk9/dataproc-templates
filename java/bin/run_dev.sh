----------sq-server-bq,gcs--------


export GCP_PROJECT=us106575-np-pm-da-3635-3
export REGION=europe-west1
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/
export JARS=gs://sqlserver_poc/driver-jar/mssql-jdbc-12.10.0.jre11.jar

bin/start.sh \
-- --template JDBCTOGCSANDBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty jdbc.url="jdbc:sqlserver://azr20302qtysql01.database.windows.net:1433;databaseName=azr20302qtysql01-db2;user=D2D-DP-INGESTION_RO;password={99O;N_&gKzl7}" \
--templateProperty jdbc.driver.class.name=com.microsoft.sqlserver.jdbc.SQLServerDriver \
--templateProperty jdbc.sql="select * from D2D_PO.TFGRITEM" \
--templateProperty gcs.output.location=gs://sqlserver_poc/output/ \
--templateProperty gcs.output.format=csv \
--templateProperty gcs.write.mode=Append \
--templateProperty bigquery.dataset.id="SQLSERVER_POC" \
--templateProperty bigquery.table.name="TFGRITEM" \
--templateProperty bigquery.write.mode=Overwrite \
--templateProperty bigquery.temp.gcs.bucket=gs://sqlserver_poc/temp_bq/ \
--templateProperty gcs.output.delimiter="^^\`\`" \  # optional
--templateProperty log.level=INFO # optional





export GCP_PROJECT=us106575-np-pm-da-3635-3
export REGION=europe-west1
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/
export JARS=gs://sqlserver_poc/driver-jar/mssql-jdbc-12.10.0.jre11.jar

bin/start.sh \
-- --template JDBCTOGCSANDBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty jdbc.url="jdbc:sqlserver://azr20302qtysql01.database.windows.net:1433;databaseName=azr20302qtysql01-db2;user=D2D-DP-INGESTION_RO;password={99O;N_&gKzl7}" \
--templateProperty jdbc.driver.class.name=com.microsoft.sqlserver.jdbc.SQLServerDriver \
--templateProperty jdbc.sql="select * from D2D_PO.TFGRITEM" \
--templateProperty gcs.output.location=gs://sqlserver_poc/output/ \
--templateProperty gcs.output.format=csv \
--templateProperty gcs.write.mode=Append \
--templateProperty bigquery.dataset.id="SQLSERVER_POC" \
--templateProperty bigquery.table.name="TFGRITEM" \
--templateProperty bigquery.write.mode=Overwrite \
--templateProperty bigquery.temp.gcs.bucket=gs://sqlserver_poc/temp_bq/









----------mongo-bq,gcs--------

export GCP_PROJECT=us106575-np-pm-da-3635-3 \
export JARS="gs://sqlserver_poc/bq-connector/mongo-spark-connector_2.12-2.4.0.jar,gs://sqlserver_poc/bq-connector/mongo-java-driver-3.9.1.jar" \
export REGION=europe-west1  \
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3 
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/

./bin/start.sh \
-- \
--template MONGOTOGCSANDBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty spark.mongodb.input.uri="mongodb+srv://fdpreadonly-edp-qa:ziLPwkwVvlEDKY1p@fdp-azr-qa-pl-2.7ybsb.mongodb.net/?retryWrites=true&w=majority&appName=fdp-azr-qa" \
--templateProperty mongo.input.database=AdminRef \
--templateProperty mongo.input.collection=orghierarchyedp \
--templateProperty mongo.bq.output.dataset=SQLSERVER_POC \
--templateProperty mongo.bq.output.table=MONGOGB_ORG_HIERARCHY \
--templateProperty mongo.bq.temp.bucket.name=gs://sqlserver_poc/temp/ \
--templateProperty gcs.output.location=gs://sqlserver_poc/OPERATIONS/ \
--templateProperty gcs.write.mode=Overwrite \
--templateProperty bigquery.write.mode=Overwrite 





export GCP_PROJECT=us106575-np-pm-da-3635-3 \
export JARS="gs://sqlserver_poc/bq-connector/mongo-spark-connector_2.12-2.4.0.jar,gs://sqlserver_poc/bq-connector/mongo-java-driver-3.9.1.jar" \
export REGION=europe-west1  \
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3 
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/

./bin/start.sh \
-- \
--template MONGOTOGCSANDBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty spark.mongodb.input.uri="mongodb+srv://fdpreadonly-edp-qa:ziLPwkwVvlEDKY1p@fdp-azr-qa-pl-2.7ybsb.mongodb.net/?retryWrites=true&w=majority&appName=fdp-azr-qa" \
--templateProperty mongo.input.database=ActualJob \
--templateProperty mongo.input.collection=actualoperationsedp \
--templateProperty mongo.bq.output.dataset=SQLSERVER_POC \
--templateProperty mongo.bq.output.table=MONGOGB_ACTUALOPERATIONS \
--templateProperty mongo.bq.temp.bucket.name=gs://sqlserver_poc/temp/ \
--templateProperty gcs.output.location=gs://sqlserver_poc/OPERATIONS/ \
--templateProperty gcs.write.mode=Overwrite \
--templateProperty bigquery.write.mode=Overwrite 


----------------api->bq,gcs--------------



export GCP_PROJECT=us106575-np-pm-da-3635-3
export REGION=europe-west1
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/

bin/start.sh \
-- --template APITOGCSANDBQ \
--templateProperty project.id="${GCP_PROJECT}" \
--templateProperty api.initial.collection="RIGS-THROUGH-TIME?" \
--templateProperty gcs.output.location=gs://sqlserver_poc/api_output_2/ \
--templateProperty gcs.output.format=csv \
--templateProperty gcs.write.mode=Append \
--templateProperty bigquery.dataset.id="SQLSERVER_POC" \
--templateProperty bigquery.table.name="RIGS-THROUGH-TIME_TABLE" \
--templateProperty bigquery.write.mode=Append \
--templateProperty bigquery.temp.gcs.bucket=gs://sqlserver_poc/temp_bq/ 





export GCP_PROJECT=us106575-np-pm-da-3635-3
export REGION=europe-west1
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/

bin/start.sh \
-- --template APITOGCSANDBQ \
--templateProperty project.id="${GCP_PROJECT}" \
--templateProperty api.initial.collection="ACTIVITY-RIGS?" \
--templateProperty gcs.output.location=gs://sqlserver_poc/api_output_3/ \
--templateProperty gcs.output.format=csv \
--templateProperty gcs.write.mode=Append \
--templateProperty bigquery.dataset.id="SQLSERVER_POC" \
--templateProperty bigquery.table.name="ACTIVITY-RIGS_TABLE" \
--templateProperty bigquery.write.mode=Append \
--templateProperty bigquery.temp.gcs.bucket=gs://sqlserver_poc/temp_bq/ 












export GCP_PROJECT=us106575-np-pm-da-3635-3
export REGION=europe-west1
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3
export GCS_STAGING_LOCATION=gs://sqlserver_poc/stg-nkj/

bin/start.sh \
-- --template APITOGCSANDBQ \
--templateProperty project.id="${GCP_PROJECT}" \
--templateProperty api.base.url="https://api.enverus.com/v3/direct-access/" \
--templateProperty api.initial.collection="FULL-DIRECTIONAL-SURVEYS?" \
--templateProperty gcs.output.location=gs://sqlserver_poc/api_output_9/ \
--templateProperty gcs.write.mode=Append \
--templateProperty bigquery.dataset.id="SQLSERVER_POC" \
--templateProperty bigquery.table.name="FULL_DIRECTIONAL_SURVEYS_TABLE_5" \
--templateProperty bigquery.write.mode=Append \
--templateProperty api.batch.size=200 \
--templateProperty bigquery.temp.gcs.bucket=gs://sqlserver_poc/temp_bq/ 










--------bq->iceberg -------------




# --- 1. Set Environment Variables ---
export GCP_PROJECT=us106575-np-pm-da-3635-3 \
export JARS="gs://sqlserver_poc/bq-connector/iceberg-spark-runtime-3.5_2.12-1.8.1.jar" \
export REGION=europe-west1 \
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3 \
export GCS_STAGING_LOCATION="gs://sqlserver_poc/template-jar/"

  bin/start.sh \
  -- --template BIGQUERYTOICEBERG \
    --templateProperty project.id="us102762-np-pm-da-25941" \
    --templateProperty bigquery.input.dataset.id=ECC_QAS1_E1Q_Y1Q_REPORTING \
    --templateProperty bigquery.input.table=PurchaseDocuments \
    --templateProperty iceberg.gcs.warehouse.location=gs://sqlserver_poc/iceberg-warehouse/ \
    --templateProperty iceberg.output.mode=Overwrite \
    --templateProperty iceberg.catalog.name=di_catalog \
    --templateProperty iceberg.database.name=di_database \
    --templateProperty iceberg.table.name=PurchaseDocuments 





    --us102762-np-pm-da-25941.ECC_QAS1_E1Q_Y1Q_REPORTING.PurchaseDocuments





export GCP_PROJECT=us106575-np-pm-da-3635-3 \
export JARS="gs://sqlserver_poc/bq-connector/iceberg-spark-runtime-3.5_2.12-1.8.1.jar" \
export REGION=europe-west1 \
export SUBNET=projects/slb-it-infrastructure-01/regions/europe-west1/subnetworks/subnet170-us106575-np-pm-da-3635-3 \
export GCS_STAGING_LOCATION="gs://sqlserver_poc/template-jar/"

  bin/start.sh \
  -- --template BIGQUERYTOICEBERG \
    --templateProperty project.id="${GCP_PROJECT}" \
    --templateProperty bigquery.input.dataset.id=SQLSERVER_POC \
    --templateProperty bigquery.input.table=TDPURCHREQ \
    --templateProperty iceberg.gcs.warehouse.location=gs://sqlserver_poc/output/ \
    --templateProperty iceberg.output.mode=Append \
    --templateProperty iceberg.catalog.name=di_catalog \
    --templateProperty iceberg.database.name=di_database \
    --templateProperty iceberg.table.name=TDPURCHREQ 






mvn spotless:apply

mvn clean install -DskipTests



https://api.github.com/search/repositories?q=react&per_page=2&page=1