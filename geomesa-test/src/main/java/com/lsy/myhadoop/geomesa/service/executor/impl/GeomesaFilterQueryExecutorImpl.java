package com.lsy.myhadoop.geomesa.service.executor.impl;

import com.lsy.myhadoop.geomesa.service.constant.ExceptionEnum;
import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;
import com.lsy.myhadoop.geomesa.service.entity.GeomesaResult;
import com.lsy.myhadoop.geomesa.service.entity.PageInfo;
import com.lsy.myhadoop.geomesa.service.utils.BusiVerify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class GeomesaFilterQueryExecutorImpl extends AbstractGeomesaExecutor<GeomesaContext, GeomesaResult> {

    @Override
    public GeomesaResult queryResult(DataStore dataStore, GeomesaContext geomesaContext) throws Exception {
        log.info("geo sql = {}", geomesaContext.getCql());
        Query query = new Query(geomesaContext.getFeature(), ECQL.toFilter(geomesaContext.getCql()));

        FeatureReader<SimpleFeatureType, SimpleFeature> reader = getFeatureReader(dataStore, query);
        try {
            List<String> columnNames = getAttributeNames(reader.getFeatureType());

            log.info("query.....");
            List<Map<String, Object>> resultList = getResultList(reader, columnNames, geomesaContext.getPageInfo());

            return new GeomesaResult(columnNames, resultList.size(), resultList);
        } finally {
            log.info("close.....");
            close(reader);
        }
    }

    private List<String> getAttributeNames(SimpleFeatureType simpleFeatureType) {
        String geom = simpleFeatureType.getGeometryDescriptor().getName().toString();
        List<String> attributeNames = Arrays.stream(DataUtilities.attributeNames(simpleFeatureType)).filter(x -> !x.equals(geom)).collect(Collectors.toList());
        return BusiVerify.verifyNotNull(attributeNames, ExceptionEnum.DATA_MANAGER_HIVE_SUG.getCode(), "获取attributeNames失败");
    }

    private FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(DataStore dataStore, Query query) throws IOException {
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

        return BusiVerify.verifyNotNull(reader, ExceptionEnum.DATA_MANAGER_HIVE_SUG.getCode(), String.format("query=%s 查询失败", query.toString()));
    }

    private List<Map<String, Object>> getResultList(FeatureReader<SimpleFeatureType, SimpleFeature> reader, List<String> attributeNames, PageInfo pageInfo) throws IOException {
        List<Map<String, Object>> resultList = new LinkedList<>();

        int size = pageInfo.getPageSize();
        int offset = pageInfo.getPageNum() * size;

        // pageSize为-1时没有offset
        while (offset > 0 && reader.hasNext()) {
            offset -= 1;
            reader.next();
        }

        // pageSize大于0时返回size个结果，pageSize为-1时返回全部结果
        while (size != 0 && reader.hasNext()) {
            size -= 1;
            SimpleFeature simpleFeature = reader.next();
            Map<String, Object> values = new HashMap<>();
            for (String name : attributeNames) {
                Object attribute = simpleFeature.getAttribute(name);
                values.put(name, attribute instanceof Date ? ((Date) attribute).getTime() / 1000 : attribute);
//                values.put(name, attribute);
            }
            resultList.add(values);
        }

        return resultList;
    }

    private void close(FeatureReader reader) throws Exception {
        if (reader != null) {
            while (reader.hasNext()) {
                reader.next();
            }
            IOUtils.closeQuietly(reader);
        }
    }
}

