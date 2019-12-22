package com.lsy.myhadoop.geomesa.service.wrapper.builder;

import com.lsy.myhadoop.geomesa.service.entity.OperationInfo;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractQueryBuilder implements QueryBuilder {

    public OperationInfo operationInfo;

    public abstract String asCqlFilterString();
}
