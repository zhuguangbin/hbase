package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

public class ComparatorUtil {

	public static byte[] toSymbol(CompareOp compareOp) {
		switch (compareOp) {
		case LESS:
			return ParseConstants.LESS_THAN_ARRAY;
		case LESS_OR_EQUAL:
			return ParseConstants.LESS_THAN_OR_EQUAL_TO_ARRAY;
		case EQUAL:
			return ParseConstants.EQUAL_TO_ARRAY;
		case NOT_EQUAL:
			return ParseConstants.NOT_EQUAL_TO_ARRAY;
		case GREATER_OR_EQUAL:
			return ParseConstants.GREATER_THAN_OR_EQUAL_TO_ARRAY;
		case GREATER:
			return ParseConstants.GREATER_THAN_ARRAY;
		default:
			return null;
		}
	}
	
	public static byte[] getComparatorType(WritableByteArrayComparable comparator){
		if (comparator instanceof BinaryComparator) {
			return ParseConstants.binaryType;
		}else if (comparator instanceof BinaryPrefixComparator) {
			return ParseConstants.binaryPrefixType;
		}else if (comparator instanceof RegexStringComparator) {
			return ParseConstants.regexStringType;
		}else if (comparator instanceof SubstringComparator) {
			return ParseConstants.substringType;
		}else {
			throw new IllegalArgumentException("Comparator " + comparator.getClass() + " not supported");
		}
	}
	
}
