package com.lsy.myhadoop.flink.tools;

import java.util.HashMap;
import java.util.Map;

public class MyHashMap extends HashMap<Integer,Integer> {
    @Override
    public Integer put(Integer key, Integer value) {
        Integer nv = value;
        if (containsKey(key)){
            Integer old = get(key);
            nv=old+nv;
        }
        return super.put(key, nv);
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> m) {

        super.putAll(m);
    }
    public void putAtoB(MyHashMap mm){
        for (Entry<Integer, Integer> entry : mm.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();
            put(key, value);
        }
    }

}
