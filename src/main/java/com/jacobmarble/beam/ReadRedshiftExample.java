/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jacobmarble.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.aws.redshift.Redshift;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.io.aws.redshift.Redshift.Read;
import org.apache.beam.sdk.io.aws.redshift.Redshift.RedshiftMarshaller;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a pipeline that reads/writes to Redshift.
 */
public class ReadRedshiftExample {

  private static final Logger LOG = LoggerFactory.getLogger(ReadRedshiftExample.class);

  interface RedshiftExampleOptions extends PipelineOptions {

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

    @Description("Redshift read query")
    String getRedshiftReadQuery();
    void setRedshiftReadQuery(String value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(RedshiftExampleOptions.class);
    RedshiftExampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .create()
        .as(RedshiftExampleOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    Redshift.Read<String> readFn = Read.<String>builder()
        .setS3TempLocationPrefix(options.getS3TempLocationPrefix())
        .setDataSourceConfiguration(DataSourceConfiguration.create(
            options.getRedshiftEndpoint(),
            options.getRedshiftPort(),
            options.getRedshiftDatabase(),
            options.getRedshiftUser(),
            options.getRedshiftPassword()
        ))
        .setQuery(options.getRedshiftReadQuery())
        .setRedshiftMarshaller(new MyRedshiftMarshaller())
        .setCoder(StringUtf8Coder.of())
        .build();

    pipeline
        .apply(readFn)
        .apply(ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void peek(ProcessContext context) {
            LOG.info("read '{}' from Redshift query", context.element());
          }
        }));

    State resultState = pipeline.run().waitUntilFinish();
    if (State.FAILED == resultState || State.UNKNOWN == resultState) {
      System.exit(1);
    }
  }

  static class MyRedshiftMarshaller implements RedshiftMarshaller<String> {

    @Override
    public String unmarshalFromRedshift(String[] value) {
      return String.join(",", value);
    }

    @Override
    public String[] marshalToRedshift(String value) {
      return value.split(",");
    }
  }
}
