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

package org.apache.skywalking.oap.server.starter;

import org.apache.skywalking.oap.server.library.module.*;
import org.apache.skywalking.oap.server.starter.config.*;
import org.slf4j.*;

/**
 * SkyWalking Collector 启动入口
 * Collector 使用组件管理器( ModuleManager )，管理多个组件( Module )。
 *     一个组件有多种组件服务提供者( ModuleProvider )，同时一个组件只允许使用一个组件服务提供者。
 * Collector 使用一个应用配置类( ApplicationConfiguration )。
 *     一个应用配置类包含多个组件配置类( ModuleConfiguration )。每个组件对应一个组件配置类。
 *     一个组件配置类包含多个组件服务提供者配置( ProviderConfiguration )。每个组件服务提供者对应一个组件配置类。
 *     注意：因为一个组件只允许同时使用一个组件服务提供者，所以一个组件配置类只设置一个组件服务提供者配置。
 *
 *
 * @author peng-yongsheng
 */
public class OAPServerStartUp {

    private static final Logger logger = LoggerFactory.getLogger(OAPServerStartUp.class);

    public static void main(String[] args) {
        // 创建配置加载器
        ApplicationConfigLoader configLoader = new ApplicationConfigLoader();
        // 创建模块管理器
        ModuleManager manager = new ModuleManager();
        try {
            // 加载配置
            ApplicationConfiguration applicationConfiguration = configLoader.load();
            // 初始化组件
            manager.init(applicationConfiguration);

            String mode = System.getProperty("mode");
            if ("init".equals(mode)) {
                logger.info("OAP starts up in init mode successfully, exit now...");
                System.exit(0);
            }
        } catch (ConfigFileNotFoundException | ModuleNotFoundException | ProviderNotFoundException | ServiceNotProvidedException | ModuleConfigException | ModuleStartException e) {
            logger.error(e.getMessage(), e);
            System.exit(1);
        }
    }
}
