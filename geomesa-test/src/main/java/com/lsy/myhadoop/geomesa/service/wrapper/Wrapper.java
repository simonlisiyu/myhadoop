package com.lsy.myhadoop.geomesa.service.wrapper;


import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;

public interface Wrapper<T extends GeomesaContext> {

    void wrap(T t);
}
