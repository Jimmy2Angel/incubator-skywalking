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
 *
 */

package org.apache.skywalking.oap.server.library.module;

import java.util.*;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.slf4j.*;

/**
 * @author wu-sheng, peng-yongsheng
 */
class BootstrapFlow {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapFlow.class);

    private Map<String, ModuleDefine> loadedModules;

    /**
     * 保存 ModuleProvider 的启动顺序
     * {@link org.apache.skywalking.oap.server.cluster.plugin.standalone.ClusterModuleStandaloneProvider}
     * {@link org.apache.skywalking.oap.server.core.CoreModuleProvider}
     * {@link org.apache.skywalking.oap.query.graphql.GraphQLQueryProvider}
     * {@link org.apache.skywalking.aop.server.receiver.mesh.MeshReceiverProvider}
     * {@link org.apache.skywalking.oap.server.core.alarm.provider.AlarmModuleProvider}
     * {@link org.apache.skywalking.oap.server.storage.plugin.jdbc.h2.H2StorageProvider}
     * {@link org.apache.skywalking.oap.server.receiver.trace.provider.TraceModuleProvide}
     * {@link org.apache.skywalking.oap.server.receiver.istio.telemetry.provider.IstioTelemetryReceiverProvider}
     * {@link org.apache.skywalking.oap.server.receiver.jvm.provider.JVMModuleProvider}
     * {@link org.apache.skywalking.oap.server.receiver.register.provider.RegisterModuleProvider}
     * {@link org.apache.skywalking.oap.server.receiver.zipkin.ZipkinReceiverProvider}
     */
    private List<ModuleProvider> startupSequence;

    BootstrapFlow(Map<String, ModuleDefine> loadedModules) throws CycleDependencyException {
        this.loadedModules = loadedModules;
        startupSequence = new LinkedList<>();

        // 获得 ModuleProvider 启动顺序
        makeSequence();
    }

    @SuppressWarnings("unchecked")
    void start(
        ModuleManager moduleManager) throws ModuleNotFoundException, ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            // 校验依赖的 Module 是否都已经存在
            String[] requiredModules = provider.requiredModules();
            if (requiredModules != null) {
                for (String module : requiredModules) {
                    if (!moduleManager.has(module)) {
                        throw new ModuleNotFoundException(module + " is required by " + provider.getModuleName()
                            + "." + provider.name() + ", but not found.");
                    }
                }
            }
            logger.info("start the provider {} in {} module.", provider.name(), provider.getModuleName());
            // 校验 ModuleProvider 包含的 Service 们都创建成功。(Service 会在 moduleProvider.prepare 方法中创建)
            provider.requiredCheck(provider.getModule().services());

            // 执行 ModuleProvider 启动阶段逻辑
            provider.start();
        }
    }

    void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            provider.notifyAfterCompleted();
        }
    }

    private void makeSequence() throws CycleDependencyException {
        List<ModuleProvider> allProviders = new ArrayList<>();
        loadedModules.forEach((moduleName, module) -> allProviders.addAll(module.providers()));

        do {
            int numOfToBeSequenced = allProviders.size();
            for (int i = 0; i < allProviders.size(); i++) {
                ModuleProvider provider = allProviders.get(i);
                String[] requiredModules = provider.requiredModules();
                if (CollectionUtils.isNotEmpty(requiredModules)) {
                    boolean isAllRequiredModuleStarted = true;
                    for (String module : requiredModules) {
                        // find module in all ready existed startupSequence
                        boolean exist = false;
                        for (ModuleProvider moduleProvider : startupSequence) {
                            if (moduleProvider.getModuleName().equals(module)) {
                                exist = true;
                                break;
                            }
                        }
                        if (!exist) {
                            isAllRequiredModuleStarted = false;
                            break;
                        }
                    }

                    if (isAllRequiredModuleStarted) {
                        startupSequence.add(provider);
                        allProviders.remove(i);
                        i--;
                    }
                } else {
                    startupSequence.add(provider);
                    allProviders.remove(i);
                    i--;
                }
            }

            if (numOfToBeSequenced == allProviders.size()) {
                StringBuilder unSequencedProviders = new StringBuilder();
                allProviders.forEach(provider -> unSequencedProviders.append(provider.getModuleName()).append("[provider=").append(provider.getClass().getName()).append("]\n"));
                throw new CycleDependencyException("Exist cycle module dependencies in \n" + unSequencedProviders.substring(0, unSequencedProviders.length() - 1));
            }
        }
        while (allProviders.size() != 0);
    }
}
