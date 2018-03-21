package com.jacobmarble.beam;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.aws.redshift.Copy;
import org.apache.beam.sdk.io.aws.redshift.Redshift;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Example of a pipeline that writes to Redshift.
 */
public class CopyRedshiftExample {

  interface CopyRedshiftExampleOptions extends RedshiftExampleOptions {

    @Description("Redshift write table spec")
    @Default.String("schema.table")
    String getRedshiftWriteTableSpec();
    void setRedshiftWriteTableSpec(String value);

    @Description("S3 source objects, comma-delimited as s3://bucket/path/f1.csv,s3://bucket/path/f2.csv")
    String getS3SourceObjects();
    void setS3SourceObjects(String value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(CopyRedshiftExampleOptions.class);
    CopyRedshiftExampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .create()
        .as(CopyRedshiftExampleOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    Copy copyFn = Copy.builder()
        .setDataSourceConfiguration(DataSourceConfiguration.create(
            options.getRedshiftEndpoint(),
            options.getRedshiftPort(),
            options.getRedshiftDatabase(),
            options.getRedshiftUser(),
            options.getRedshiftPassword()
        ))
        .setDestinationTableSpec(options.getRedshiftWriteTableSpec())
        .setDelimiter(',')
        .setSourceCompression(Compression.UNCOMPRESSED)
        .build();

    pipeline
        .apply(Create.of(Arrays.asList(options.getS3SourceObjects().split(","))))
        .apply(ParDo.of(copyFn));

    State resultState = pipeline.run().waitUntilFinish();
    if (State.FAILED == resultState || State.UNKNOWN == resultState) {
      System.exit(1);
    }
  }
}
