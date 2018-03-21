# Beam Redshift Example Project

Demonstrates the proposed Redshift module for Apache Beam. See https://issues.apache.org/jira/browse/BEAM-3032

To build, grab [my Redshift Beam branch](https://github.com/jacobmarble/beam/tree/redshift) and install with `cd sdks/java; mvn clean install -DskipTests`

To run, you need credentials to a Redshift cluster and an S3 bucket in the same AWS region.

Read example; reads Redshift SQL query to PCollection:
```sh
java -classpath target/beam-redshift-example-0.1-shaded.jar com.jacobmarble.beam.ReadRedshiftExample \
  --awsRegion=my-region-1 \
  --redshiftEndpoint="my-redshift-cluster.redshift.amazonaws.com" \
  --redshiftPort=5439 \
  --redshiftDatabase=my-database \
  --redshiftUser=my-redshift-user \
  --redshiftPassword=my-redshift-password \
  --s3TempLocationPrefix="s3://my-bucket/my-path/" \
  --redshiftReadQuery="SELECT foo, bar FROM schema.table LIMIT 10"
```

Proof it's working: the results of your query appear in the log.

Write example; writes PCollection to Redshift table:
```sh
java -classpath target/beam-redshift-example-0.1-shaded.jar com.jacobmarble.beam.ReadRedshiftExample \
  --awsRegion=my-region-1 \
  --redshiftEndpoint="my-redshift-cluster.redshift.amazonaws.com" \
  --redshiftPort=5439 \
  --redshiftDatabase=my-database \
  --redshiftUser=my-redshift-user \
  --redshiftPassword=my-redshift-password \
  --s3TempLocationPrefix="s3://my-bucket/my-path/" \
  --redshiftWriteTableSpec="schema.table"
```

Proof it's working: records (1,alpha), (2,bravo), etc appear in your table.

Unload example; writes query results to S3 objects as CSV:
```sh
java -classpath target/beam-redshift-example-0.1-shaded.jar com.jacobmarble.beam.UnloadRedshiftExample \
  --awsRegion=my-region-1 \
  --redshiftEndpoint="my-redshift-cluster.redshift.amazonaws.com" \
  --redshiftPort=5439 \
  --redshiftDatabase=my-database \
  --redshiftUser=my-redshift-user \
  --redshiftPassword=my-redshift-password \
  --s3Destination="s3://my-bucket/my-path/" \
  --redshiftReadQuery="SELECT foo, bar FROM schema.table LIMIT 10"
```

Copy example; inserts S3 object CSV contents into Redshift table:
```sh
java -classpath target/beam-redshift-example-0.1-shaded.jar com.jacobmarble.beam.CopyRedshiftExample \
  --awsRegion=my-region-1 \
  --redshiftEndpoint="my-redshift-cluster.redshift.amazonaws.com" \
  --redshiftPort=5439 \
  --redshiftDatabase=my-database \
  --redshiftUser=my-redshift-user \
  --redshiftPassword=my-redshift-password \
  --s3SourceObjects="s3://my-bucket/my-path/f1.csv,s3://my-bucket/my-path/f2.csv" \
  --redshiftWriteTableSpec="schema.table"
```

Alternative S3 credentials specification:
```
  --awsCredentialsProvider="{\"@type\":\"AWSStaticCredentialsProvider\",\"awsAccessKeyId\":\"YOUR_KEY_ID\",\"awsSecretKey\":\"YOUR_SECRET_KEY\"}" \
```
