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
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.io.aws.redshift.Unload;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a pipeline that issues the UNLOAD Redshift SQL command.
 */
public class UnloadRedshiftExample {

  private static final Logger LOG = LoggerFactory.getLogger(UnloadRedshiftExample.class);

  interface UnloadRedshiftExampleOptions extends RedshiftExampleOptions {

    @Description("Redshift read query")
    @Default.String("SELECT foo, bar FROM schema.table LIMIT 10")
    String getRedshiftReadQuery();
    void setRedshiftReadQuery(String value);

    @Description("S3 destination")
    String getS3Destination();
    void setS3Destination(String value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(UnloadRedshiftExampleOptions.class);
    UnloadRedshiftExampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .create()
        .as(UnloadRedshiftExampleOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    Unload unloadFn = Unload.builder()
        .setDestination(options.getS3Destination())
        .setDataSourceConfiguration(DataSourceConfiguration.create(
            options.getRedshiftEndpoint(),
            options.getRedshiftPort(),
            options.getRedshiftDatabase(),
            options.getRedshiftUser(),
            options.getRedshiftPassword()
        ))
        .setSourceQuery(options.getRedshiftReadQuery())
        .setDelimiter(',')
        .setDestinationCompression(Compression.UNCOMPRESSED)
        .build();

    pipeline
        .apply(Create.of((Void) null))
        .apply(ParDo.of(unloadFn))
        .apply(ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void peek(ProcessContext context) {
            LOG.info("unloaded to '{}' from Redshift query", context.element());
          }
        }));

    State resultState = pipeline.run().waitUntilFinish();
    if (State.FAILED == resultState || State.UNKNOWN == resultState) {
      System.exit(1);
    }
  }
}
