package com.quan.serialize;

import com.quan.extension.SPI;

/**
 * Description: 序列化接口，所有序列化都需要实现这个接口
 * date: 2023/02/17 上午 11:27
 *
 * @author Quan
 */

@SPI
public interface Serializer {

    // 序列化对象
    byte[] serialize(Object obj);

    // 反序列化
    /*
        bytes: 序列化后的字节数组
        clazz: 目标类
        T: 类的类型，以及返回的反序列化对象
     */
    <T> T deserialize(byte[] bytes, Class<T> clazz);

}
