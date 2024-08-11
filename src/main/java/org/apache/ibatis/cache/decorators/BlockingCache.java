/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.cache.decorators;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.cache.impl.PerpetualCache;

/**
 * <p>
 * Simple blocking decorator
 * <p>
 * Simple and inefficient version of EhCache's BlockingCache decorator. It sets a lock over a cache key when the element
 * is not found in cache. This way, other threads will wait until this element is filled instead of hitting the
 * database.
 * <p>
 * By its nature, this implementation can cause deadlock when used incorrectly.
 *
 * @author Eduardo Macarron
 */
public class BlockingCache implements Cache {

  private long timeout;
  private final Cache delegate;
  /**
   * 为啥要使用 CountDownLatch来替换掉 ReentrantLock
   * 1. 可重入锁 无法被 remove掉，会造成oom, 一旦创建就无法被remove,因为unlock和remove两个操作都会造成释放的临界区锁
   * 2. 这里使用countDownLatch 结合 putIfAbsent语义 来保障，谁初始化了 key的 countdownlatch谁就进入的临界区，
   * 3. 没有初始化成功的线程，拿着被初始化的countdownLatch等待折被唤醒
   */
  private final ConcurrentHashMap<Object, CountDownLatch> locks;

  public BlockingCache(Cache delegate) {
    this.delegate = delegate;
    this.locks = new ConcurrentHashMap<>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public void putObject(Object key, Object value) {
    try {
      delegate.putObject(key, value);
    } finally {
      releaseLock(key);
    }
  }

  /**
   * 为啥会有不符合常理的 releaseLock
   * 一般 lock / unlock 都是成对出现的，为啥这里的unlock操作会有判断条件
   * 1. 结合cache在mybatis中的整体作用，或者引入的背景来看，主要是防止多个会话有相同的sql同时向数据库发出请求
   * 2. 保障只有一个会话能请求数据库，其他会话等待结果，不管结果是啥，
   * 3. 不再缓存，第一个真正执行sql的会话 也是当前只有锁的线程（countdownLatch的创建者），在缓存中没有获得结果，那么他就有责任去执行下面的逻辑
   * 去拿到真正的结果，不管结果是空值还是啥，拿到结果以后，将结果放入缓存中，也就是上面的putObject方法，所以在putObject方法中，才调用了真正的释放锁操作
   * @param key
   *          The key
   *
   * @return
   */
  @Override
  public Object getObject(Object key) {
    acquireLock(key);
    Object value = delegate.getObject(key);
    if (value != null) {
      releaseLock(key);
    }
    return value;
  }

  @Override
  public Object removeObject(Object key) {
    // despite its name, this method is called only to release locks
    releaseLock(key);
    return null;
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  private void acquireLock(Object key) {
    CountDownLatch newLatch = new CountDownLatch(1);
    while (true) {
      CountDownLatch latch = locks.putIfAbsent(key, newLatch);
      if (latch == null) {
        break;
      }
      try {
        if (timeout > 0) {
          boolean acquired = latch.await(timeout, TimeUnit.MILLISECONDS);
          if (!acquired) {
            throw new CacheException(
                "Couldn't get a lock in " + timeout + " for the key " + key + " at the cache " + delegate.getId());
          }
        } else {
          latch.await();
        }
      } catch (InterruptedException e) {
        throw new CacheException("Got interrupted while trying to acquire lock for key " + key, e);
      }
    }
  }

  private void releaseLock(Object key) {
    CountDownLatch latch = locks.remove(key);
    if (latch == null) {
      throw new IllegalStateException("Detected an attempt at releasing unacquired lock. This should never happen.");
    }
    latch.countDown();
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }


  public static void main(String[] args) {
    Cache cache = new BlockingCache(new PerpetualCache("zone"));

    CountDownLatch latch = new CountDownLatch(3);
    for (int i = 0; i < 3; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
           cache.getObject("test");
           latch.countDown();
        }
      }).start();
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }



}
