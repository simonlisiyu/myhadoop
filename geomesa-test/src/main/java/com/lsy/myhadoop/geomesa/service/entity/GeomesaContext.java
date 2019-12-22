package com.lsy.myhadoop.geomesa.service.entity;

import lombok.Data;

import java.util.List;

@Data
public class GeomesaContext {

    private String zookeepers;

    private String catalog;

    private String feature;

    private Long timeRange;

    private List<ParamOperationInfo> paramList;

    private PageInfo pageInfo;

    private String cql;
}
