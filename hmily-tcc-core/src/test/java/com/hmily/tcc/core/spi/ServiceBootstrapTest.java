package com.hmily.tcc.core.spi;


import com.hmily.tcc.common.enums.SerializeEnum;
import com.hmily.tcc.common.serializer.ObjectSerializer;
import com.hmily.tcc.common.utils.ServiceBootstrap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;


public class ServiceBootstrapTest {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBootstrapTest.class);


    @Test
    public void loadFirst() throws Exception {
        final ObjectSerializer objectSerializer = ServiceBootstrap.loadFirst(ObjectSerializer.class);
        LOGGER.info("加载的序列化名称为：{}", objectSerializer.getClass().getName());

    }


    @Test
    public void loadAll() {
        //spi  serialize
        final SerializeEnum serializeEnum = SerializeEnum.HESSIAN;
        final ServiceLoader<ObjectSerializer> objectSerializers = ServiceBootstrap.loadAll(ObjectSerializer.class);
        for(Iterator<ObjectSerializer> it = objectSerializers.iterator(); it.hasNext();) {
            ObjectSerializer objectSerializer = it.next();
            if(Objects.equals(objectSerializer.getScheme(), serializeEnum.getSerialize())) {
                LOGGER.info("加载的序列化名称为：{}", objectSerializer.getClass().getName());
                break;
            }
        }
    }

}