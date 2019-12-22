package com.lsy.myhadoop.geomesa.service.wrapper.builder.impl;


import com.lsy.myhadoop.geomesa.service.entity.OperationInfo;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.AbstractQueryBuilder;

public class GeoQueryBuilder extends AbstractQueryBuilder {

    public GeoQueryBuilder(OperationInfo operationInfo) {
        super(operationInfo);
    }

    @Override
    public String asCqlFilterString() {
        return operationInfo.getOperator() + " (" + operationInfo.getKey() + ", " + operationInfo.getValue() + ")";
    }
}
