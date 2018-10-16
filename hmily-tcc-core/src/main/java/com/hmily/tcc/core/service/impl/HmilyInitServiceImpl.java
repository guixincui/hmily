/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmily.tcc.core.service.impl;

import com.hmily.tcc.common.config.TccConfig;
import com.hmily.tcc.common.enums.RepositorySupportEnum;
import com.hmily.tcc.common.enums.SerializeEnum;
import com.hmily.tcc.common.serializer.KryoSerializer;
import com.hmily.tcc.common.serializer.ObjectSerializer;
import com.hmily.tcc.common.utils.ServiceBootstrap;
import com.hmily.tcc.core.coordinator.CoordinatorService;
import com.hmily.tcc.core.disruptor.publisher.HmilyTransactionEventPublisher;
import com.hmily.tcc.core.helper.SpringBeanUtils;
import com.hmily.tcc.core.service.HmilyInitService;
import com.hmily.tcc.core.spi.CoordinatorRepository;
import com.hmily.tcc.core.spi.repository.JdbcCoordinatorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * hmily tcc init service.
 *
 * @author xiaoyu
 */
@Service("tccInitService")
public class HmilyInitServiceImpl implements HmilyInitService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HmilyInitServiceImpl.class);

    private final CoordinatorService coordinatorService;

    private final HmilyTransactionEventPublisher hmilyTransactionEventPublisher;

    @Autowired
    public HmilyInitServiceImpl(final CoordinatorService coordinatorService, final HmilyTransactionEventPublisher hmilyTransactionEventPublisher) {
        this.coordinatorService = coordinatorService;
        this.hmilyTransactionEventPublisher = hmilyTransactionEventPublisher;
    }

    /**
     * hmily initialization.
     *
     * @param tccConfig {@linkplain TccConfig}
     */
    @Override
    public void initialization(final TccConfig tccConfig) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("hmily shutdown now");
            }
        }));
        try {
            loadSpiSupport(tccConfig);
            hmilyTransactionEventPublisher.start(tccConfig.getBufferSize(), tccConfig.getConsumerThreads());
            coordinatorService.start(tccConfig);
        } catch (Exception ex) {
            LOGGER.error(" hmily init exception:{}", ex.getMessage());
            System.exit(1);
        }
        LOGGER.info("hmily init success!");
    }

    /**
     * load spi.
     *
     * @param tccConfig {@linkplain TccConfig}
     */
    private void loadSpiSupport(final TccConfig tccConfig) {
        //spi serialize
        final SerializeEnum serializeEnum = SerializeEnum.acquire(tccConfig.getSerializer());
        final ServiceLoader<ObjectSerializer> objectSerializers = ServiceBootstrap.loadAll(ObjectSerializer.class);
        ObjectSerializer serializer = null;
        for (Iterator<ObjectSerializer> it = objectSerializers.iterator(); it.hasNext(); ) {
            ObjectSerializer item = it.next();
            if (item.getScheme() != null && item.getScheme().equals(serializeEnum)) {
                serializer = item;
                break;
            }
        }
        if (serializer == null) {
            serializer = new KryoSerializer();
        }
        //spi repository
        final RepositorySupportEnum repositorySupportEnum = RepositorySupportEnum.acquire(tccConfig.getRepositorySupport());
        final ServiceLoader<CoordinatorRepository> recoverRepositories = ServiceBootstrap.loadAll(CoordinatorRepository.class);
        CoordinatorRepository repository = null;
        for (Iterator<CoordinatorRepository> it = recoverRepositories.iterator(); it.hasNext(); ) {
            CoordinatorRepository item = it.next();
            if (item.getScheme() != null && item.getScheme().equals(repositorySupportEnum.getSupport())) {
                repository = item;
                break;
            }
        }
        if (repository == null) {
            repository = new JdbcCoordinatorRepository();
        }
        repository.setSerializer(serializer);
        SpringBeanUtils.getInstance().registerBean(CoordinatorRepository.class.getName(), repository);
    }
}
