package com.jacobmarble.beam;

import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that apply to both example jobs.
 */
interface RedshiftExampleOptions extends PipelineOptions, AwsOptions{

  @Description("S3 temp location prefix, as in s3://bucket/path/")
  String getS3TempLocationPrefix();
  void setS3TempLocationPrefix(String value);

  @Description("Redshift cluster endpoint URL")
  String getRedshiftEndpoint();
  void setRedshiftEndpoint(String value);

  @Description("Redshift cluster port number")
  int getRedshiftPort();
  void setRedshiftPort(int value);

  @Description("Redshift database")
  String getRedshiftDatabase();
  void setRedshiftDatabase(String value);

  @Description("Redshift cluster username")
  String getRedshiftUser();
  void setRedshiftUser(String value);

  @Description("Redshift cluster password")
  String getRedshiftPassword();
  void setRedshiftPassword(String value);
}
