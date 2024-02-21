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

package org.apache.shenyu.client.core.disruptor;

import org.apache.shenyu.client.core.disruptor.executor.RegisterClientConsumerExecutor.RegisterClientExecutorFactory;
import org.apache.shenyu.client.core.disruptor.subcriber.ShenyuClientApiDocExecutorSubscriber;
import org.apache.shenyu.client.core.disruptor.subcriber.ShenyuClientMetadataExecutorSubscriber;
import org.apache.shenyu.client.core.disruptor.subcriber.ShenyuClientURIExecutorSubscriber;
import org.apache.shenyu.disruptor.DisruptorProviderManage;
import org.apache.shenyu.disruptor.provider.DisruptorProvider;
import org.apache.shenyu.register.client.api.ShenyuClientRegisterRepository;
import org.apache.shenyu.register.common.type.DataTypeParent;

/**
 * The type shenyu client register event publisher.
 */
public class ShenyuClientRegisterEventPublisher {
    
    private static final ShenyuClientRegisterEventPublisher INSTANCE = new ShenyuClientRegisterEventPublisher();
    
    private DisruptorProviderManage<DataTypeParent> providerManage;
    
    /**
     * Get instance.
     *
     * @return ShenyuClientRegisterEventPublisher instance
     */
    public static ShenyuClientRegisterEventPublisher getInstance() {
        return INSTANCE;
    }
    
    /**
     * Start.
     *
     * @param shenyuClientRegisterRepository shenyuClientRegisterRepository
     */
    public void start(final ShenyuClientRegisterRepository shenyuClientRegisterRepository) {
        // 工厂用 Map （工厂的一个成员变量）来封装多个订阅者，其中订阅者用来向 Admin 发送数据，
        // key 为数据类型，value 为对应的订阅者
        RegisterClientExecutorFactory factory = new RegisterClientExecutorFactory();
        // 添加 3 个订阅者，分别负责向 Admin 元数据、URI、ApiDoc 的传输
        factory.addSubscribers(new ShenyuClientMetadataExecutorSubscriber(shenyuClientRegisterRepository));
        factory.addSubscribers(new ShenyuClientURIExecutorSubscriber(shenyuClientRegisterRepository));
        factory.addSubscribers(new ShenyuClientApiDocExecutorSubscriber(shenyuClientRegisterRepository));
        // 创建一个 DisruptorProviderManage，它负责提供一个 disruptor provider
        providerManage = new DisruptorProviderManage<>(factory);
        providerManage.startup();
    }
    
    /**
     * Publish event.
     *
     * @param data the data
     */
    public void publishEvent(final DataTypeParent data) {
        DisruptorProvider<DataTypeParent> provider = providerManage.getProvider();
        // data 传给 Disruptor provider
        provider.onData(data);
    }
}
