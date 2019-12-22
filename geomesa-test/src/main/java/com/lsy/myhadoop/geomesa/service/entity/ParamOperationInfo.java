package com.lsy.myhadoop.geomesa.service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ParamOperationInfo {
    private String paramName;

    private String paramValue;

    private String operationCode;
}
