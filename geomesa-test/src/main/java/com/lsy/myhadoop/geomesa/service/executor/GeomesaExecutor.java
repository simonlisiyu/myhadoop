package com.lsy.myhadoop.geomesa.service.executor;


public interface GeomesaExecutor<K, V> {
    V execute(K k);
}
