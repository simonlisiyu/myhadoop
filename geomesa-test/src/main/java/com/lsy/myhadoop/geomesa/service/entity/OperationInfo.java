package com.lsy.myhadoop.geomesa.service.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class OperationInfo {

    private String key;

    private String value;

    private String operator;
}
