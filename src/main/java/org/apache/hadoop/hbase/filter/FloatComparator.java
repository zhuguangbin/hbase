package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a Float value.
 */
public class FloatComparator extends WritableByteArrayComparable {

  private Float floatValue;

  /** Nullary constructor for Writable, do not use */
  public FloatComparator() {
  }

  /**
   * Constructor
   * @param value value
   */
  public FloatComparator(Float value) {
    super(Bytes.toBytes(value));
    this.floatValue = value;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return this.floatValue.compareTo(Bytes.toFloat(value, offset));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Float floatValue = in.readFloat();
    this.value = Bytes.toBytes(floatValue);
    this.floatValue = floatValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(floatValue);
  }

}
