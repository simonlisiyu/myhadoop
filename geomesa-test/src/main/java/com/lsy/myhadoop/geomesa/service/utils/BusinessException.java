package com.lsy.myhadoop.geomesa.service.utils;

/**
 * @author xiang.ji
 */
public class BusinessException extends RuntimeException {

    private int errorCode;

    private String errorInfo;

    public BusinessException(int errorCode, String errorInfo) {
        this.errorCode = errorCode;
        this.errorInfo = errorInfo;
    }

    public BusinessException(int errorCode, String errorInfo, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
        this.errorInfo = errorInfo;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorInfo() {
        return errorInfo;
    }
}
