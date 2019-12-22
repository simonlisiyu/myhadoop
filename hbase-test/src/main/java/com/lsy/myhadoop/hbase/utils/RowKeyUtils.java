package com.lsy.myhadoop.hbase.utils;

import org.apache.commons.codec.digest.DigestUtils;

public class RowKeyUtils {

	public static String getReverseAndMd5PrefixRowKey(String id) {
        String reverseId = new StringBuffer(id).reverse().toString();
        String rowKey = DigestUtils.md5Hex(id).substring(0, 8) + reverseId;
		return rowKey;
	}

    public static String getMd5PrefixRowKey(String id) {
        String rowKey = DigestUtils.md5Hex(id).substring(0, 8) + id;
        return rowKey;
    }
}