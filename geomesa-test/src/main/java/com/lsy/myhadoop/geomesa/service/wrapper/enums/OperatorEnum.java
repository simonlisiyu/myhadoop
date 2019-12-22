package com.lsy.myhadoop.geomesa.service.wrapper.enums;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lsy.myhadoop.geomesa.service.constant.ExceptionEnum;
import com.lsy.myhadoop.geomesa.service.constant.WrapperConstant;
import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;
import com.lsy.myhadoop.geomesa.service.entity.OperationInfo;
import com.lsy.myhadoop.geomesa.service.entity.ParamOperationInfo;
import com.lsy.myhadoop.geomesa.service.utils.BusiVerify;
import com.lsy.myhadoop.geomesa.service.utils.DateTimeUtil;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.QueryBuilder;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.impl.AttrQueryBuilder;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.impl.GeoQueryBuilder;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.impl.TimeQueryBuilder;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public enum OperatorEnum {

    within("within") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            String operator = WrapperConstant.CIRCLE.equals(geometry.type) ? WrapperConstant.DWIHIN :
                    (WrapperConstant.POINTS2.equals(geometry.type) ? WrapperConstant.BBOX : WrapperConstant.WITHIN);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, operator);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    beyond("beyond") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String value = getGeoValue(paramOperationInfo.getParamValue());

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.BEYOND);

            return new GeoQueryBuilder(operationInfo);
        }
    },

    bbox("bbox") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String value = getGeoValue(paramOperationInfo.getParamValue());

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.BBOX);

            return new GeoQueryBuilder(operationInfo);
        }
    },

    intersects("intersects") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String value = getGeoValue(paramOperationInfo.getParamValue());

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.INTERSECTS);

            return new GeoQueryBuilder(operationInfo);
        }
    },

    disjoint("disjoint") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String value = getGeoValue(paramOperationInfo.getParamValue());

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.DISJOINT);

            return new GeoQueryBuilder(operationInfo);
        }
    },

    contains("contains") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.CONTAINS);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    touches("touches") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.TOUCHES);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    crosses("crosses") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.CROSSES);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    overlaps("overlaps") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.OVERLAPS);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    equals("equals") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            GeometryType geometry = getGeometryType(paramOperationInfo.getParamValue());

            String value = GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.EQUALS);
            return new GeoQueryBuilder(operationInfo);
        }
    },

    during("during") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String[] timestamps = paramOperationInfo.getParamValue().split("/");
            Date begin = new Date(Long.parseLong(timestamps[0]) * 1000);
            Date end = new Date(Long.parseLong(timestamps[1]) * 1000);

            String value = DateTimeUtil.DateToString(begin, "yyyy-MM-dd'T'HH:mm:ss") + "/" + DateTimeUtil.DateToString(end, "yyyy-MM-dd'T'HH:mm:ss");
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.DURING);
            return new TimeQueryBuilder(operationInfo);
        }

        @Override
        public boolean check(ParamOperationInfo paramOperationInfo, GeomesaContext context) {
            String[] timestamps = paramOperationInfo.getParamValue().split("/");

            return Long.parseLong(timestamps[1]) - Long.parseLong(timestamps[0]) <= context.getTimeRange();
        }
    },

    range("range") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            String[] range = paramOperationInfo.getParamName().split(",");

            String operator = WrapperConstant.BETWEEN + range[0] + WrapperConstant.AND + range[1];

            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), "", operator);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    eq("=") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.EQ);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    neq("!=") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.NEQ);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    lt("<") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.LT);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    lte("<=") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.LTE);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    gt(">") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.GT);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    gte(">=") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.GTE);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    like("like") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), paramOperationInfo.getParamValue(), WrapperConstant.LIKE);
            return new AttrQueryBuilder(operationInfo);
        }
    },

    in("in") {
        @Override
        public QueryBuilder operate(ParamOperationInfo paramOperationInfo) {
            List<String> valueList = Arrays.stream(paramOperationInfo.getParamValue().split(WrapperConstant.COMMA_SPLITTER)).map(x -> "'" + x.trim() + "'").collect(Collectors.toList());
            String value = "(" + String.join(WrapperConstant.COMMA_JOINER, valueList) + ")";
            OperationInfo operationInfo = new OperationInfo(paramOperationInfo.getParamName(), value, WrapperConstant.IN);
            return new AttrQueryBuilder(operationInfo);
        }
    };

    private String operator;

    OperatorEnum(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }

    public boolean check(ParamOperationInfo paramOperationInfo, GeomesaContext context) {
        return true;
    }

    public abstract QueryBuilder operate(ParamOperationInfo paramOperationInfo);

    private static final Gson GSON = new GsonBuilder().create();

    String getGeoValue(String paramValue) {
        GeometryType geometry = getGeometryType(paramValue);
        BusiVerify.verifyNotNull(GeometryEnum.valueOf(geometry.type), ExceptionEnum.PARAM_INVALID.getCode(), String.format("geometry类型 type = %s 不存在", geometry.type));

        return GeometryEnum.valueOf(geometry.type).asGeometryString(geometry.content);
    }

    GeometryType getGeometryType(String value) {
        return GSON.fromJson(value, GeometryType.class);
    }
}
