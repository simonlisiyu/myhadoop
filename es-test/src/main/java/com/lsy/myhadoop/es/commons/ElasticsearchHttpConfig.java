package com.lsy.myhadoop.es.commons;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by lisiyu on 2019/6/3.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElasticsearchHttpConfig {
    private String ip;

    private String port;


}
