package com.lsy.myhadoop.hbase.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by lisiyu on 2016/12/28.
 */
public class ExceptionUtils {
    /**
     * 完整的堆栈信息
     *
     * @param e Exception
     * @return Full StackTrace
     */
    public static String getStackTrace(Exception e) {
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
        } finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (pw != null) {
                pw.close();
            }
        }
        return sw.toString();
    }

    /**
     * 将Exception.printStackTrace()转换为String输出
     * @param e
     * @param split
     * @return
     */
    public static String getErrorInfoStringFromException(Exception e, String split) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
//            return sw.toString();
            return sw.toString().replaceAll("\\r\\n", split);
        } catch (Exception e2) {
            return "fail getErrorInfoFromException";
        }
    }
    public static String getErrorInfoStringFromException(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
//            return sw.toString();
            return sw.toString().replaceAll("\\r\\n", ",");
        } catch (Exception e2) {
            return "fail getErrorInfoFromException";
        }
    }


//    public static void main(String[] args) {
//        int[] array = new int[2];
//
//
//        try {
//            int a = array[3];
//            System.out.println(a);
//        } catch (Exception e) {
//            System.out.println("===================");
//            System.out.println(ExceptionUtils.getErrorInfoStringFromException(e));
//            System.out.println("===================");
//
////            System.out.println("------------+"+e.toString()+"+------------------");
////            for(StackTraceElement elem : e.getStackTrace()) {
////                System.out.print(" at ");
////                System.out.print(elem);
////            }
//        }
//
////        throw new IllegalArgumentException("wrong args!");
//    }
}
