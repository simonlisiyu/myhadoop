package com.lsy.myhadoop.geomesa.service.wrapper.builder.impl;


import com.lsy.myhadoop.geomesa.service.entity.OperationInfo;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.AbstractQueryBuilder;

public class AttrQueryBuilder extends AbstractQueryBuilder {

    public AttrQueryBuilder(OperationInfo operationInfo) {
        super(operationInfo);
    }

    @Override
    public String asCqlFilterString() {
        return (operationInfo.getKey() + " " + operationInfo.getOperator() + " " + operationInfo.getValue()).trim();
    }
}
