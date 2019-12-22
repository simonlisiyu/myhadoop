package com.lsy.myhadoop.es.commons;


/**
 * Created by lisiyu on 2019/6/3.
 * ES 常用常量类
 */
public class ElasticsearchHttpConst {

    // 比较符号
    public enum COMPARE_SYMBOL_ENUM {
        GREATER_THAN_OR_EQUAL("gte"),
        LESS_THAN_OR_EQUAL("lte"),
        GREATER_THAN("gt"),
        LESS_THAN("lt");

        private String symbol;

        COMPARE_SYMBOL_ENUM(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return this.symbol;
        }
    }

    public final static String ES_QUERY = "query";

    public final static String ES_SEARCH = "_search";

    public final static String ES_SOURCE = "_source";

    public final static String ES_TYPE = "_type";

    public final static String ES_HITS = "hits";

    public final static String ES_TOTAL = "total";

    public final static String SYMBOL_COMMA = ",";

    // 排序符号
    public enum ORDER_BY_SYMBOL_ENUM {
        DESC("desc"),
        ASC("asc");

        private String symbol;

        ORDER_BY_SYMBOL_ENUM(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return this.symbol;
        }
    }
}
