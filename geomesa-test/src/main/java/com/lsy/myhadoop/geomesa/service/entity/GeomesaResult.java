package com.lsy.myhadoop.geomesa.service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeomesaResult {

    private List<String> columnList;

    private int resultCount;

    private List<Map<String, Object>> resultList;

    @Override
    public String toString() {
        return "GeomesaResult{" +
                "columnList=" + columnList +
                ", result size=" + (CollectionUtils.isNotEmpty(resultList) ? resultList.size() : "0") +
                '}';
    }
}
