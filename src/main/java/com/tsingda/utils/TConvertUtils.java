package com.tsingda.utils;

import java.lang.reflect.Field;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TConvertUtils {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TConvertUtils.class);
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends TBase, F extends TFieldIdEnum> void convertToTBase(Object obj, T t, F[] fields) {
        if (obj == null) {
            return;
        }
        Class<?> clazz = obj.getClass();
        for (F f : fields) {
            String fieldName = f.getFieldName();
            try {
                Field objField = clazz.getDeclaredField(fieldName);
                objField.setAccessible(true);
                Object value = objField.get(obj);
                t.setFieldValue(f, value);
            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                LOGGER.error("转换异常：", e);
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends TBase, F extends TFieldIdEnum, M> M convertToCommonModel(T t, F[] fields, Class<M> clazz) {
        if (t == null)
            return null;
        M obj = null;
        try {
            obj = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("实例化异常:", e);
        }
        for (F f : fields) {
            try {
                String name = f.getFieldName();
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                Object value = t.getFieldValue(f);
                field.set(obj, value);
            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                LOGGER.error("转换异常:", e);
                continue;
            }
        }
        return obj;
    }
}
