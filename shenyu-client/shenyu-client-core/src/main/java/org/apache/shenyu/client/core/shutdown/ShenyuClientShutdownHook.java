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

package org.apache.shenyu.client.core.shutdown;

import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.shenyu.register.client.api.ShenyuClientRegisterRepository;
import org.apache.shenyu.register.common.config.ShenyuRegisterCenterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shenyu client shutdown hook.
 */
public class ShenyuClientShutdownHook {

    private static final Logger LOG = LoggerFactory.getLogger(ShenyuClientShutdownHook.class);

    private static final AtomicBoolean DELAY = new AtomicBoolean(false);

    private static String hookNamePrefix = "ShenyuClientShutdownHook";

    private static AtomicInteger hookId = new AtomicInteger(0);

    private static Properties props;

    private static IdentityHashMap<Thread, Thread> delayHooks = new IdentityHashMap<>();

    private static IdentityHashMap<Thread, Thread> delayedHooks = new IdentityHashMap<>();

    public ShenyuClientShutdownHook() {
    }

    public ShenyuClientShutdownHook(final ShenyuClientRegisterRepository repository, final ShenyuRegisterCenterConfig config) {
        String name = String.join("-", hookNamePrefix, String.valueOf(hookId.incrementAndGet()));
        ShutdownHookManager.get().addShutdownHook(new Thread(repository::closeRepository, name), 1);
        LOG.info("Add hook {}", name);
        ShenyuClientShutdownHook.props = config.getProps();
    }

    /**
     * Add shenyu client shutdown hook.
     *
     * @param repository ShenyuClientRegisterRepository
     * @param props  Properties
     */
    public static void set(final ShenyuClientRegisterRepository repository, final Properties props) {
        String name = String.join("-", hookNamePrefix, String.valueOf(hookId.incrementAndGet()));
        ShutdownHookManager.get().addShutdownHook(new Thread(repository::closeRepository, name), 1);
        LOG.info("Add hook {}", name);
        ShenyuClientShutdownHook.props = props;
    }

    /**
     * Delay other shutdown hooks.
     */
    public static void delayOtherHooks() {
        // 利用 CAS 不加锁地确保并发时 TakeoverOtherHooksThread 线程只被运行一次
        if (!DELAY.compareAndSet(false, true)) {
            return;
        }
        // 接管其他钩子的线程
        TakeoverOtherHooksThread thread = new TakeoverOtherHooksThread();
        thread.start();
    }

    /**
     * Delay other shutdown hooks thread.
     */
    private static class TakeoverOtherHooksThread extends Thread {
        @Override
        // 1. 该线程用于生成钩子，这些钩子用来延迟执行已经添加的钩子，为的是处理一些资源的关闭，和注册信息的注销
        public void run() {
            int shutdownWaitTime = Integer.parseInt(props.getProperty("shutdownWaitTime", "3000"));
            int delayOtherHooksExecTime = Integer.parseInt(props.getProperty("delayOtherHooksExecTime", "2000"));
            IdentityHashMap<Thread, Thread> hooks = null;
            try {
                // 2. 通过反射拿到应用关闭时的所有钩子
                Class<?> clazz = Class.forName(props.getProperty("applicationShutdownHooksClassName", "java.lang.ApplicationShutdownHooks"));
                Field field = clazz.getDeclaredField(props.getProperty("applicationShutdownHooksFieldName", "hooks"));
                field.setAccessible(true);
                hooks = (IdentityHashMap<Thread, Thread>) field.get(clazz);
            } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException ex) {
                LOG.error(ex.getMessage(), ex);
            }
            long s = System.currentTimeMillis();
            // 3. 限制处理钩子的时间在 delayOtherHooksExecTime 之内，为什么要控制时间，难道不会遗漏一些钩子无法延迟吗？
            // GPT:
            // 答：1. 避免死锁或长时间阻塞
            //    2. 可以确保这个延迟逻辑不会过度拖延应用的关闭过程
            //    3. 实用性考虑： 在大多数情况下，如果在给定的时间内无法连接到或修改某些钩子，可能是因为存在一些异常或特殊情况。
            //       在这种情况下，继续等待可能不会带来太多好处，而是增加了关闭过程的复杂性和不确定性。
            //    确实，这种方法可能会遗漏一些在延迟期间新注册的钩子，但这通常是一个权衡的结果，设计者可能认为这种情况很少发生，或者遗漏的风险相对较小。
            while (System.currentTimeMillis() - s < delayOtherHooksExecTime) {
                for (Iterator<Thread> iterator = Objects.requireNonNull(hooks).keySet().iterator(); iterator.hasNext();) {
                    Thread hook = iterator.next();
                    // 4. 用于延迟执行原本钩子的钩子不必再延迟，所以跳过
                    if (hook.getName().startsWith(hookNamePrefix)) {
                        continue;
                    }
                    // 5. 正在处理的延迟的钩子和处理过的延迟的钩子不必再延迟，所以跳过
                    if (delayHooks.containsKey(hook) || delayedHooks.containsKey(hook)) {
                        continue;
                    }
                    Thread delayHook = new Thread(() -> {
                        LOG.info("sleep {}ms", shutdownWaitTime);
                        try {
                            // 6. 先睡眠 shutdownWaitTime，然后再执行原本的在应用关闭时的钩子
                            TimeUnit.MILLISECONDS.sleep(shutdownWaitTime);
                        } catch (InterruptedException ex) {
                            LOG.error(ex.getMessage(), ex);
                        }
                        hook.run();
                    }, hook.getName());
                    delayHooks.put(delayHook, delayHook);
                    // 7. 从原本的钩子 map 中移除这个原本要执行的钩子，即 delayHook
                    iterator.remove();
                }

                for (Iterator<Thread> iterator = delayHooks.keySet().iterator(); iterator.hasNext();) {
                    Thread delayHook = iterator.next();
                    // 8. 向运行时加入用来延迟执行原本钩子的钩子，即 delayedHooks
                    Runtime.getRuntime().addShutdownHook(delayHook);
                    // 9. 加入已处理过的钩子 map
                    delayedHooks.put(delayHook, delayHook);
                    iterator.remove();
                    LOG.info("hook {} will sleep {}ms when it start", delayHook.getName(), shutdownWaitTime);
                }
                try {
                    // 10. 睡眠 100ms，目的是？
                    // GPT:
                    // 答：1. 减少CPU使用率
                    //    2. 给其他操作留出处理时间，通过在每次循环后短暂休眠，可以给其他线程运行的机会
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
            // 帮助 GC
            hookNamePrefix = null;
            hookId = new AtomicInteger(0);
            props = null;
            delayHooks = null;
            delayedHooks = null;
        }
    }
}
