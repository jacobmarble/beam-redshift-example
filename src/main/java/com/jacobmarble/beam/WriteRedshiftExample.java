package com.jacobmarble.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.aws.redshift.Redshift;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

/**
 * Example of a pipeline that writes to Redshift.
 */
public class WriteRedshiftExample {

  interface WriteRedshiftExampleOptions extends RedshiftExampleOptions {

    @Description("Redshift write table spec")
    @Default.String("schema.table")
    String getRedshiftWriteTableSpec();
    void setRedshiftWriteTableSpec(String value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(WriteRedshiftExampleOptions.class);
    WriteRedshiftExampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .create()
        .as(WriteRedshiftExampleOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    Redshift.Write<String> writeFn = Redshift.Write.<String>builder()
        .setS3TempLocationPrefix(options.getS3TempLocationPrefix())
        .setDataSourceConfiguration(DataSourceConfiguration.create(
            options.getRedshiftEndpoint(),
            options.getRedshiftPort(),
            options.getRedshiftDatabase(),
            options.getRedshiftUser(),
            options.getRedshiftPassword()
        ))
        .setDestinationTableSpec(options.getRedshiftWriteTableSpec())
        .setRedshiftMarshaller(StringRedshiftMarshaller.create())
        .setCoder(StringUtf8Coder.of())
        .build();

    pipeline
        .apply(Create.of(
            "1,alpha",
            "2,bravo",
            "3,charlie",
            "4,delta",
            "5,echo",
            "6,fox",
            "7,golf",
            "8,hotel",
            "9,india",
            "10,juliet"
        ))
        .apply(writeFn);

    State resultState = pipeline.run().waitUntilFinish();
    if (State.FAILED == resultState || State.UNKNOWN == resultState) {
      System.exit(1);
    }
  }
}
