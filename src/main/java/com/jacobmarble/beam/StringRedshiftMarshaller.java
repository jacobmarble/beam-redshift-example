package com.jacobmarble.beam;

import org.apache.beam.sdk.io.aws.redshift.Redshift.RedshiftMarshaller;

/**
 * Marshals to and from comma-delimited String.
 */
class StringRedshiftMarshaller implements RedshiftMarshaller<String> {

  private StringRedshiftMarshaller() {
  }

  static StringRedshiftMarshaller create() {
    return new StringRedshiftMarshaller();
  }

  @Override
  public String unmarshalFromRedshift(String[] value) {
    return String.join(",", value);
  }

  @Override
  public String[] marshalToRedshift(String value) {
    return value.split(",");
  }
}
