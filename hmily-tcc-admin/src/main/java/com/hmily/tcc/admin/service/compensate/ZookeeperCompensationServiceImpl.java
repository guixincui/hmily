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

package com.hmily.tcc.admin.service.compensate;

import com.google.common.collect.Lists;
import com.hmily.tcc.admin.helper.ConvertHelper;
import com.hmily.tcc.admin.helper.PageHelper;
import com.hmily.tcc.admin.page.CommonPager;
import com.hmily.tcc.admin.query.CompensationQuery;
import com.hmily.tcc.admin.service.CompensationService;
import com.hmily.tcc.admin.vo.TccCompensationVO;
import com.hmily.tcc.common.bean.adapter.CoordinatorRepositoryAdapter;
import com.hmily.tcc.common.exception.TccException;
import com.hmily.tcc.common.serializer.ObjectSerializer;
import com.hmily.tcc.common.utils.DateUtils;
import com.hmily.tcc.common.utils.RepositoryPathUtils;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Objects;

/**
 * zookeeper impl.
 *
 * @author xiaoyu(Myth)
 */
@RequiredArgsConstructor
@SuppressWarnings("all")
public class ZookeeperCompensationServiceImpl implements CompensationService {

    private final ZooKeeper zooKeeper;

    private final ObjectSerializer objectSerializer;

    @Override
    public CommonPager<TccCompensationVO> listByPage(final CompensationQuery query) {
        CommonPager<TccCompensationVO> voCommonPager = new CommonPager<>();
        final int currentPage = query.getPageParameter().getCurrentPage();
        final int pageSize = query.getPageParameter().getPageSize();
        int start = (currentPage - 1) * pageSize;
        final String rootPath = RepositoryPathUtils.buildZookeeperPathPrefix(query.getApplicationName());
        List<String> zNodePaths;
        List<TccCompensationVO> voList;
        int totalCount;
        try {
            //如果只查 重试条件的
            if (StringUtils.isBlank(query.getTransId()) && null != (query.getRetry())) {
                zNodePaths = zooKeeper.getChildren(rootPath, false);
                final List<TccCompensationVO> all = findAll(zNodePaths, rootPath);
                final List<TccCompensationVO> collect = Lists.newArrayList();
                for(TccCompensationVO vo : all) {
                    if(vo != null && vo.getRetriedCount() < query.getRetry()) {
                        collect.add(vo);
                    }
                }
                totalCount = collect.size();
                voList = Lists.newArrayList();
                for (int i = start; i < totalCount && i < start + pageSize; i++) {
                    voList.add(collect.get(i));
                }
            } else if (StringUtils.isNoneBlank(query.getTransId()) && null == (query.getRetry())) {
                zNodePaths = Lists.newArrayList(query.getTransId());
                totalCount = zNodePaths.size();
                voList = findAll(zNodePaths, rootPath);
            } else if (StringUtils.isNoneBlank(query.getTransId()) && null != (query.getRetry())) {
                zNodePaths = Lists.newArrayList(query.getTransId());
                totalCount = zNodePaths.size();
                voList = Lists.newArrayList();
                for (TccCompensationVO vo : findAll(zNodePaths, rootPath)) {
                    if (vo != null && vo.getRetriedCount() < query.getRetry()) {
                        voList.add(vo);
                    }
                }
            } else {
                zNodePaths = zooKeeper.getChildren(rootPath, false);
                totalCount = zNodePaths.size();
                voList = findByPage(zNodePaths, rootPath, start, pageSize);
            }
            voCommonPager.setPage(PageHelper.buildPage(query.getPageParameter(), totalCount));
            voCommonPager.setDataList(voList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return voCommonPager;
    }

    @Override
    public Boolean batchRemove(final List<String> ids, final String appName) {
        if (CollectionUtils.isEmpty(ids) || StringUtils.isBlank(appName)) {
            return Boolean.FALSE;
        }
        final String rootPathPrefix = RepositoryPathUtils.buildZookeeperPathPrefix(appName);
        for(String id : ids) {
            try {
                final String path = RepositoryPathUtils.buildZookeeperRootPath(rootPathPrefix, id);
                byte[] content = zooKeeper.getData(path,
                        false, new Stat());
                final CoordinatorRepositoryAdapter adapter =
                        objectSerializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
                zooKeeper.delete(path, adapter.getVersion());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean updateRetry(final String id, final Integer retry, final String appName) {
        if (StringUtils.isBlank(id) || StringUtils.isBlank(appName) || null == (retry)) {
            return Boolean.FALSE;
        }
        final String rootPathPrefix = RepositoryPathUtils.buildZookeeperPathPrefix(appName);
        final String path = RepositoryPathUtils.buildZookeeperRootPath(rootPathPrefix, id);
        try {
            byte[] content = zooKeeper.getData(path,
                    false, new Stat());
            final CoordinatorRepositoryAdapter adapter =
                    objectSerializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
            adapter.setLastTime(DateUtils.getDateYYYY());
            adapter.setRetriedCount(retry);
            zooKeeper.create(path,
                    objectSerializer.serialize(adapter),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return Boolean.TRUE;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Boolean.FALSE;
    }

    private List<TccCompensationVO> findAll(final List<String> zNodePaths, final String rootPath) {
        List<TccCompensationVO> result = Lists.newArrayList();
        for(String zNodePath : zNodePaths) {
            if(StringUtils.isNoneBlank(zNodePath)) {
                result.add(buildByNodePath(rootPath, zNodePath));
            }
        }
        return result;
    }

    private List<TccCompensationVO> findByPage(final List<String> zNodePaths, final String rootPath,
                                               final int start, final int pageSize) {
        List<TccCompensationVO> result = Lists.newArrayList();
        for(int i = start;i<start + pageSize && i<zNodePaths.size();i++) {
            if(StringUtils.isNoneBlank(zNodePaths.get(i))) {
                result.add(buildByNodePath(rootPath, zNodePaths.get(i)));
            }
        }
        return result;
    }

    private TccCompensationVO buildByNodePath(final String rootPath, final String zNodePath) {
        try {
            byte[] content = zooKeeper.getData(RepositoryPathUtils.buildZookeeperRootPath(rootPath, zNodePath),
                    false, new Stat());
            final CoordinatorRepositoryAdapter adapter =
                    objectSerializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
            return ConvertHelper.buildVO(adapter);
        } catch (KeeperException | InterruptedException | TccException e) {
            e.printStackTrace();
        }
        return null;
    }

}
