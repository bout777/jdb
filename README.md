# JDB

一个 Java 实现的简单数据库，部分原理参照自 MySQL、PostgreSQL、RookieDB(berkeley cs186)。实现了以下功能:
- 时间戳的多版本乐观并发控制, 实现了两种事务隔离级别 (读已提交, 快照隔离)
- B+树簇集索引
- 基于火山模型的sql查询处理
- 日志系统保证原子性与持久性, ARIES算法崩溃恢复



TODO

- lru缓冲池替换
- 表连接
- 多客户端连接
- ...

水平有限，时间仓促，仍有很多不足之处，敬请指正，备战秋招ing，只能后续有时间再完善了 TnT

