package com.lsy.myhadoop.geomesa.conf;


//import com.google.common.base.Preconditions;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lisiyu on 2017/1/7.
 */
public class Context {
    private Map<String, String> parameters;

    public Context() {
        parameters = Collections.synchronizedMap(new HashMap<String, String>());
    }

    public Context(Map<String, String> paramters) {
        this();
        this.putAll(paramters);
    }

    public void putAll(Map<String, String> map) {
        parameters.putAll(map);
    }

    /**
     * Gets a copy of the backing map structure.
     * @return immutable copy of backing map structure
     */
//    public ImmutableMap<String, String> getParameters() {
//        synchronized (parameters) {
//            return ImmutableMap.copyOf(parameters);
//        }
//    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        parameters.clear();
    }

    /**
     * Get properties which start with a prefix. When a property is returned,
     * the prefix is removed the from name. For example, if this method is
     * called with a parameter &quot;hdfs.&quot; and the context contains:
     * <code>
     * { hdfs.key = value, otherKey = otherValue }
     * </code>
     * this method will return a map containing:
     * <code>
     * { key = value}
     * </code>
     *
     * <b>Note:</b> The <tt>prefix</tt> must end with a period character. If not
     * this method will raise an IllegalArgumentException.
     *
     * @param prefix key prefix to find and remove from keys in resulting map
     * @return map with keys which matched prefix with prefix removed from
     *   keys in resulting map. If no keys are matched, the returned map is
     *   empty
    //     * @throws IllegalArguemntException if the given prefix does not end with
     *   a period character.
     */
//    public ImmutableMap<String, String> getSubProperties(String prefix) {
//        Preconditions.checkArgument(prefix.endsWith("."),
//                "The given prefix does not end with a period (" + prefix + ")");
//        Map<String, String> result = Maps.newHashMap();
//        synchronized (parameters) {
//            for (String key : parameters.keySet()) {
//                if (key.startsWith(prefix)) {
//                    String name = key.substring(prefix.length());
//                    result.put(name, parameters.get(key));
//                }
//            }
//        }
//        return ImmutableMap.copyOf(result);
//    }

    /**
     * Associates the specified value with the specified key in this context.
     * If the context previously contained a mapping for the key, the old value
     * is replaced by the specified value.
     * @param key key with which the specified value is to be associated
     * @param value to be associated with the specified key
     */
    public void put(String key, String value) {
        parameters.put(key, value);
    }

    /**
     * Returns true if this Context contains a mapping for key.
     * Otherwise, returns false.
     */
    public boolean containsKey(String key) {
        return parameters.containsKey(key);
    }

    public <T extends Object> T get(String key, Class<T> clazz, T defaultValue) {
        Object o = parameters.get(key);
        if (o == null) {
            return defaultValue;
        } else {
            try {
                return (T)o;
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }

//    public <T extends Object> T forEach(String key, Class<T> clazz, T defaultValue) {
//        for(Map.Entry<String, String> entry : parameters.entrySet()){
//
//        }
//        Object o = parameters.get(key);
//        if (o == null) {
//            return defaultValue;
//        } else {
//            try {
//                return (T)o;
//            } catch (Exception e) {
//                return defaultValue;
//            }
//        }
//    }

    public <T extends Object> T get(String key, Class<T> clazz) {
        return get(key, clazz, null);
    }

    @Override
    public String toString() {
        return "{ parameters:" + parameters + " }";
    }
}
