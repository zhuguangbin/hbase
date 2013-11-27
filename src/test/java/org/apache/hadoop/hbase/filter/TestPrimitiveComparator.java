package org.apache.hadoop.hbase.filter;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.filter.DoubleComparator;
import org.apache.hadoop.hbase.filter.FloatComparator;
import org.apache.hadoop.hbase.filter.IntComparator;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.ShortComparator;
import org.apache.hadoop.hbase.filter.StringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Unit test for Primitive Comparator:
 * ShortComparator/IntComparator/LongComparator/FloatComparator/DoubleComparator/StringComparator.
 */
public class TestPrimitiveComparator extends TestCase {

  public void testShortComparator() {
    ShortComparator shortComparator = new ShortComparator((short) 123);
    assertTrue(shortComparator.compareTo(Bytes.toBytes((short) 12)) > 0);
    assertTrue(shortComparator.compareTo(Bytes.toBytes((short) 123)) == 0);
    assertTrue(shortComparator.compareTo(Bytes.toBytes((short) 1234)) < 0);
  }

  public void testIntComparator() {
    IntComparator intComparator = new IntComparator(123456);
    assertTrue(intComparator.compareTo(Bytes.toBytes(123)) > 0);
    assertTrue(intComparator.compareTo(Bytes.toBytes(123456)) == 0);
    assertTrue(intComparator.compareTo(Bytes.toBytes(1234567)) < 0);
  }

  public void testLongComparator() {

    LongComparator longComparator = new LongComparator(3147483647L);
    assertTrue(longComparator.compareTo(Bytes.toBytes(147483647L)) > 0);
    assertTrue(longComparator.compareTo(Bytes.toBytes(3147483647L)) == 0);
    assertTrue(longComparator.compareTo(Bytes.toBytes(33147483647L)) < 0);
  }

  public void testFloatComparator() {
    FloatComparator floatComparator = new FloatComparator(10.001f);
    assertTrue(floatComparator.compareTo(Bytes.toBytes(1.001f)) > 0);
    assertTrue(floatComparator.compareTo(Bytes.toBytes(10.001f)) == 0);
    assertTrue(floatComparator.compareTo(Bytes.toBytes(100.001f)) < 0);
  }

  public void testDoubleComparator() {
    DoubleComparator doubleComparator = new DoubleComparator(100.000001);
    assertTrue(doubleComparator.compareTo(Bytes.toBytes(10.001)) > 0);
    assertTrue(doubleComparator.compareTo(Bytes.toBytes(100.000001)) == 0);
    assertTrue(doubleComparator.compareTo(Bytes.toBytes(1000.000001)) < 0);
  }

  public void testStringComparator() {
    StringComparator stringComparator = new StringComparator("teststring");
    assertTrue(stringComparator.compareTo(Bytes.toBytes("abc")) > 0);
    assertTrue(stringComparator.compareTo(Bytes.toBytes("teststring")) == 0);
    assertTrue(stringComparator.compareTo(Bytes.toBytes("xyz")) < 0);
  }

}
