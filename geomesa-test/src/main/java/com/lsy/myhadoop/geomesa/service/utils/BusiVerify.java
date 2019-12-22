package com.lsy.myhadoop.geomesa.service.utils;

import com.google.common.base.Strings;

/**
 * @author xiang.ji
 */
public class BusiVerify {

    public static void verify(boolean result, int errorCode, String errorInfo) {
        if (!result) {
            throw new BusinessException(errorCode, errorInfo);
        }
    }

    public static <T> T verifyNotNull(T obj, int errorCode, String errorInfo) {
        verify(obj != null, errorCode, errorInfo);

        return obj;
    }

    public static String verifyStrNotEmpty(String obj, int errorCode, String errorInfo) {
        verify(!Strings.isNullOrEmpty(obj), errorCode, errorInfo);

        return obj;
    }
}
