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
package org.apache.ibatis.executor;

import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.TransactionalCacheManager;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 *
 * 缓存执行器 也是装饰者模式
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class CachingExecutor implements Executor {

  private final Executor delegate;

  /**
   * 事务管理器
   */
  private final TransactionalCacheManager tcm = new TransactionalCacheManager();

  public CachingExecutor(Executor delegate) {
    this.delegate = delegate;
    delegate.setExecutorWrapper(this);
  }

  @Override
  public Transaction getTransaction() {
    return delegate.getTransaction();
  }

  @Override
  public void close(boolean forceRollback) {
    try {
      // issues #499, #524 and #573
      if (forceRollback) {
        tcm.rollback();
      } else {
        tcm.commit();
      }
    } finally {
      delegate.close(forceRollback);
    }
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public int update(MappedStatement ms, Object parameterObject) throws SQLException {
    flushCacheIfRequired(ms);
    return delegate.update(ms, parameterObject);
  }

  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    flushCacheIfRequired(ms);
    return delegate.queryCursor(ms, parameter, rowBounds);
  }

  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler)
      throws SQLException {
    BoundSql boundSql = ms.getBoundSql(parameterObject);
    CacheKey key = createCacheKey(ms, parameterObject, rowBounds, boundSql);
    return query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  /**
   * 查询数据库中的数据
   * @param ms  映射语句
   * @param parameterObject
   * @param rowBounds  翻页限制条件
   * @param resultHandler 结果处理器
   * @param key  缓存键
   * @param boundSql 查询语句（sql语句）
   * @return
   * @param <E>
   * @throws SQLException
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler,
      CacheKey key, BoundSql boundSql) throws SQLException {
    Cache cache = ms.getCache();
    if (cache != null) {
      // 根据要求判断语句执行前 是否要清楚二级缓存，如果有需要，清楚二级缓存
      flushCacheIfRequired(ms);
      if (ms.isUseCache() && resultHandler == null) {
        ensureNoOutParams(ms, boundSql);
        // 从缓存中读取数据
        @SuppressWarnings("unchecked")
        List<E> list = (List<E>) tcm.getObject(cache, key);
        if (list == null) {
          // 真正的执行器 执行sql语句
          list = delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
          // 缓存 结果
          tcm.putObject(cache, key, list); // issue #578 and #116
        }
        return list;
      }
    }
    // 如果没有缓存，直接由后面的执行器执行
    return delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return delegate.flushStatements();
  }

  @Override
  public void commit(boolean required) throws SQLException {
    delegate.commit(required);
    tcm.commit();
  }

  @Override
  public void rollback(boolean required) throws SQLException {
    try {
      delegate.rollback(required);
    } finally {
      if (required) {
        tcm.rollback();
      }
    }
  }

  private void ensureNoOutParams(MappedStatement ms, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
        if (parameterMapping.getMode() != ParameterMode.IN) {
          throw new ExecutorException(
              "Caching stored procedures with OUT params is not supported.  Please configure useCache=false in "
                  + ms.getId() + " statement.");
        }
      }
    }
  }

  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    return delegate.createCacheKey(ms, parameterObject, rowBounds, boundSql);
  }

  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    return delegate.isCached(ms, key);
  }

  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key,
      Class<?> targetType) {
    delegate.deferLoad(ms, resultObject, property, key, targetType);
  }

  @Override
  public void clearLocalCache() {
    delegate.clearLocalCache();
  }


  /**
   * 根据要求判断语句执行前是否要清楚二级缓存，如果需要，则清楚二级缓存
   * 非select语句，isFlushCacheRequired方法返回true
   * @param ms
   */
  private void flushCacheIfRequired(MappedStatement ms) {
    Cache cache = ms.getCache();
    if (cache != null && ms.isFlushCacheRequired()) {
      tcm.clear(cache);
    }
  }

  @Override
  public void setExecutorWrapper(Executor executor) {
    throw new UnsupportedOperationException("This method should not be called");
  }

}
