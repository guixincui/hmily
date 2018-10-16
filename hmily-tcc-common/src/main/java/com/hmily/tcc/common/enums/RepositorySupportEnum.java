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

package com.hmily.tcc.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;


/**
 * RepositorySupportEnum.
 * @author xiaoyu
 */
@RequiredArgsConstructor
@Getter
public enum RepositorySupportEnum {

    /**
     * Db compensate cache type enum.
     */
    DB("db"),

    /**
     * File compensate cache type enum.
     */
    FILE("file"),

    /**
     * Redis compensate cache type enum.
     */
    REDIS("redis"),

    /**
     * Mongodb compensate cache type enum.
     */
    MONGODB("mongodb"),

    /**
     * Zookeeper compensate cache type enum.
     */
    ZOOKEEPER("zookeeper");

    private final String support;

    /**
     * Acquire compensate cache type compensate cache type enum.
     *
     * @param support the compensate cache type
     * @return the compensate cache type enum
     */
    public static RepositorySupportEnum acquire(final String support) {
        RepositorySupportEnum[] values = RepositorySupportEnum.values();
        for(RepositorySupportEnum val : values) {
            if(Objects.equals(val.getSupport(), support)) {
                return val;
            }
        }
        return RepositorySupportEnum.DB;
    }
}
