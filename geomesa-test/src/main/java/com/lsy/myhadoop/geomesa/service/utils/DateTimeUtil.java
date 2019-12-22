package com.lsy.myhadoop.geomesa.service.utils;

import com.google.common.base.Strings;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xiang.ji
 */

public class DateTimeUtil {

    public static final String DATE_PATTERN = "yyyyMMdd";

    public static final String DATE_PATTERN_SYMBOL = "yyyy-MM-dd";

    public static final String DATE_TIME_PATTERN = "yyyyMMddHHmmss";

    public static final String DATE_TIME_PATTERN_SYMBOL = "yyyy-MM-dd HH:mm:ss";

    public static final String LONG_DATE_PATTERN = "yyyyMMddHHmmssSSS";

    public static final String LONG_DATE_PATTERN_SYMBOL = "yyyy-MM-dd HH:mm:ss:SSS";


    public static String DateToString(Date date, String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        return date != null ? formatter.format(date) : null;
    }

    public static Date StringToDate(String date, String pattern) {
        if (Strings.isNullOrEmpty(date) || Strings.isNullOrEmpty(pattern)) {
            return null;
        }

        try {
            SimpleDateFormat formatter = new SimpleDateFormat(pattern);
            return formatter.parse(date);
        } catch (Exception ex) {
        }

        return null;
    }

    public static String DateToString(Date date) {
        return DateToString(date, DATE_PATTERN);
    }

    public static String DateToSymbolString(Date date) {
        return DateToString(date, DATE_PATTERN_SYMBOL);
    }

    public static String DateTimeToString(Date date) {
        return DateToString(date, DATE_TIME_PATTERN);
    }

    public static String DateTimeToSymbolString(Date date) {
        return DateToString(date, DATE_TIME_PATTERN_SYMBOL);
    }

    public static String DateTimeToLongString(Date date) {
        return DateToString(date, LONG_DATE_PATTERN);
    }

    public static String DateTimeToSymbolLongString(Date date) {
        return DateToString(date, LONG_DATE_PATTERN_SYMBOL);
    }

    public static Date StringToDate(String date) {
        return StringToDate(date, DATE_PATTERN);
    }

    public static Date StringSymbolToDate(String date) {
        return StringToDate(date, DATE_PATTERN_SYMBOL);
    }

    public static Date StringToDateTime(String date) {
        return StringToDate(date, DATE_TIME_PATTERN);
    }

    public static Date StringSymbolToDateTime(String date) {
        return StringToDate(date, DATE_TIME_PATTERN_SYMBOL);
    }

    public static Date StringToDateLongTime(String date) {
        return StringToDate(date, LONG_DATE_PATTERN);
    }

    public static Date StringSymbolToDateLongTime(String date) {
        return StringToDate(date, LONG_DATE_PATTERN_SYMBOL);
    }
}
