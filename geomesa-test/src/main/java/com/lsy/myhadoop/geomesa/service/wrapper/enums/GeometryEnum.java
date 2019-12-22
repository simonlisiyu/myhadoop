package com.lsy.myhadoop.geomesa.service.wrapper.enums;


import com.lsy.myhadoop.geomesa.service.constant.ExceptionEnum;
import com.lsy.myhadoop.geomesa.service.utils.BusiVerify;

public enum GeometryEnum {

    /**
     * 圆
     * 输入示例: 1.0 1.0, 2.0
     * 返回示例: POINT(1.0 1.0), 2.0, kilometers
     */
    circle {
        @Override
        public String asGeometryString(String str) {
            String[] ps = str.split(",");
            BusiVerify.verify(ps.length == 2 && ps[0].split(" ").length == 2, ExceptionEnum.PARAM_INVALID.getCode(), String.format("circle参数%s格式错误", str));
            return "POINT(" + ps[0].trim() + "), " + ps[1].trim() + ", kilometers";
        }
    },

    /**
     * 多边形
     * 输入示例: 0 0, 4 0, 4 4, 0 4, 0 0; 1 1, 2 1, 2 2, 1 2,1 1
     * 返回示例: POLYGON((0 0, 4 0, 4 4, 0 4, 0 0),(1 1, 2 1, 2 2, 1 2,1 1))
     */
    polygon {
        @Override
        public String asGeometryString(String str) {
            String[] ls = str.split(";");
            BusiVerify.verify(ls.length == 1 || ls.length == 2, ExceptionEnum.PARAM_INVALID.getCode(), String.format("polygon参数%s错误", str));
            return ls.length == 1 ? "POLYGON ((" + ls[0].trim() + "))" : "POLYGON ((" + ls[0].trim() + "), (" + ls[1].trim() + "))";
        }
    },

    /**
     * 线
     * 输入示例: 0 0, 1 1, 1 2
     * 输出示例: LINESTRING(0 0, 1 1, 1 2)
     */
    line {
        @Override
        public String asGeometryString(String str) {
            return "LINESTRING(" + str + ")";
        }
    },

    /**
     * 两点 (bounding box时使用)
     * 输入示例: -90, 40, -60, 45
     * 输出示例: -90, 40, -60, 45
     */
    points2 {
        @Override
        public String asGeometryString(String str) {
            return str;
        }
    };

    public abstract String asGeometryString(String str);
}
