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

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.agent.core.dictionary.NetworkAddressDictionary;
import org.apache.skywalking.apm.agent.core.dictionary.OperationNameDictionary;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.os.OSUtil;
import org.apache.skywalking.apm.network.language.agent.Application;
import org.apache.skywalking.apm.network.language.agent.ApplicationInstance;
import org.apache.skywalking.apm.network.language.agent.ApplicationInstanceHeartbeat;
import org.apache.skywalking.apm.network.language.agent.ApplicationInstanceMapping;
import org.apache.skywalking.apm.network.language.agent.ApplicationMapping;
import org.apache.skywalking.apm.network.language.agent.ApplicationRegisterServiceGrpc;
import org.apache.skywalking.apm.network.language.agent.InstanceDiscoveryServiceGrpc;
import org.apache.skywalking.apm.network.language.agent.NetworkAddressRegisterServiceGrpc;
import org.apache.skywalking.apm.network.language.agent.ServiceNameDiscoveryServiceGrpc;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

/**
 * 服务和服务实例注册客户端
 * @author wusheng
 */
@DefaultImplementor
public class AppAndServiceRegisterClient implements BootService, Runnable, GRPCChannelListener {
    private static final ILog logger = LogManager.getLogger(AppAndServiceRegisterClient.class);
    private static final String PROCESS_UUID = UUID.randomUUID().toString().replaceAll("-", "");

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
    private volatile ApplicationRegisterServiceGrpc.ApplicationRegisterServiceBlockingStub applicationRegisterServiceBlockingStub;
    private volatile InstanceDiscoveryServiceGrpc.InstanceDiscoveryServiceBlockingStub instanceDiscoveryServiceBlockingStub;
    private volatile ServiceNameDiscoveryServiceGrpc.ServiceNameDiscoveryServiceBlockingStub serviceNameDiscoveryServiceBlockingStub;
    private volatile NetworkAddressRegisterServiceGrpc.NetworkAddressRegisterServiceBlockingStub networkAddressRegisterServiceBlockingStub;
    private volatile ScheduledFuture<?> applicationRegisterFuture;

    /**
     * 从 GRPCChannelManager notify() 获取到连接状态
     * @param status
     */
    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            // 获取带 AuthenticationDecorator 装饰的 channel
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            applicationRegisterServiceBlockingStub = ApplicationRegisterServiceGrpc.newBlockingStub(channel);
            instanceDiscoveryServiceBlockingStub = InstanceDiscoveryServiceGrpc.newBlockingStub(channel);
            serviceNameDiscoveryServiceBlockingStub = ServiceNameDiscoveryServiceGrpc.newBlockingStub(channel);
            networkAddressRegisterServiceBlockingStub = NetworkAddressRegisterServiceGrpc.newBlockingStub(channel);
        } else {
            applicationRegisterServiceBlockingStub = null;
            instanceDiscoveryServiceBlockingStub = null;
            serviceNameDiscoveryServiceBlockingStub = null;
        }
        // 设置状态
        this.status = status;
    }

    /**
      将自己添加到 GRPCChannelManager 的监听器中
     */
    @Override
    public void prepare() throws Throwable {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    /**
     * 创建一个 ScheduledFuture
     * this 的 run 方法每 3s 执行一次
     * @throws Throwable
     */
    @Override
    public void boot() throws Throwable {
        applicationRegisterFuture = Executors
            .newSingleThreadScheduledExecutor(new DefaultNamedThreadFactory("AppAndServiceRegisterClient"))
            .scheduleAtFixedRate(new RunnableWithExceptionProtection(this, new RunnableWithExceptionProtection.CallbackWhenException() {
                @Override
                public void handle(Throwable t) {
                    logger.error("unexpected exception.", t);
                }
            }), 0, Config.Collector.APP_AND_SERVICE_REGISTER_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void onComplete() throws Throwable {
    }

    @Override
    public void shutdown() throws Throwable {
        applicationRegisterFuture.cancel(true);
    }

    @Override
    public void run() {
        logger.debug("AppAndServiceRegisterClient running, status:{}.", status);
        boolean shouldTry = true;
        // 在上面 statusChanged 方法改变状态为已连接后执行
        while (GRPCChannelStatus.CONNECTED.equals(status) && shouldTry) {
            shouldTry = false;
            try {
                // 服务注册
                // APPLICATION_ID 默认值就是 DictionaryUtil.nullValue()
                if (RemoteDownstreamConfig.Agent.APPLICATION_ID == DictionaryUtil.nullValue()) {
                    // 正常来说，states 为 CONNECTED 的话，applicationRegisterServiceBlockingStub 不会为 null
                    if (applicationRegisterServiceBlockingStub != null) {
                        // APPLICATION_CODE 在 skywalking-agent 的 agent.conf 文件中配置
                        Application request = Application.newBuilder().setApplicationCode(Config.Agent.APPLICATION_CODE).build();
                        logger.debug("AppAndServiceRegisterClient request:{}.", request);
                        ApplicationMapping applicationMapping = applicationRegisterServiceBlockingStub.applicationCodeRegister(
                            Application.newBuilder().setApplicationCode(Config.Agent.APPLICATION_CODE).build());
                        // 从 response 中获取 APPLICATION_ID 设置到 RemoteDownstreamConfig.Agent
                        if (applicationMapping != null) {
                            RemoteDownstreamConfig.Agent.APPLICATION_ID = applicationMapping.getApplication().getValue();
                            shouldTry = true;
                        }
                    }
                } else {
                    // 服务实例注册
                    if (instanceDiscoveryServiceBlockingStub != null) {
                        if (RemoteDownstreamConfig.Agent.APPLICATION_INSTANCE_ID == DictionaryUtil.nullValue()) {

                            ApplicationInstanceMapping instanceMapping = instanceDiscoveryServiceBlockingStub.registerInstance(ApplicationInstance.newBuilder()
                                .setApplicationId(RemoteDownstreamConfig.Agent.APPLICATION_ID)
                                .setAgentUUID(PROCESS_UUID)
                                .setRegisterTime(System.currentTimeMillis())
                                .setOsinfo(OSUtil.buildOSInfo())
                                .build());
                            if (instanceMapping.getApplicationInstanceId() != DictionaryUtil.nullValue()) {
                                RemoteDownstreamConfig.Agent.APPLICATION_INSTANCE_ID
                                    = instanceMapping.getApplicationInstanceId();
                            }
                        }
                        // 服务实例心跳检测
                        else {
                            instanceDiscoveryServiceBlockingStub.heartbeat(ApplicationInstanceHeartbeat.newBuilder()
                                .setApplicationInstanceId(RemoteDownstreamConfig.Agent.APPLICATION_INSTANCE_ID)
                                .setHeartbeatTime(System.currentTimeMillis())
                                .build());

                            NetworkAddressDictionary.INSTANCE.syncRemoteDictionary(networkAddressRegisterServiceBlockingStub);
                            OperationNameDictionary.INSTANCE.syncRemoteDictionary(serviceNameDiscoveryServiceBlockingStub);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error(t, "AppAndServiceRegisterClient execute fail.");
                ServiceManager.INSTANCE.findService(GRPCChannelManager.class).reportError(t);
            }
        }
    }
}
