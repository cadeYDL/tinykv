# 目标
使用badger的api实现支持列族的读写操作，注意读写均需要事务
# 解决思路
1. 学习badger的api与原理
2. 了解engine_util相关的列组的实现
3. 组合badger+engine_util实现kv/storage/standalone_storage/standalone_storage.go中的Reader和Write以及相关初始化
4. 实现kv/server/raw_api.go中接口
   - 参数校验
   - 调用server内部的storage组装入参
   - 处理返回值
# 学习总结
1. badger的[学习文档](../study/badger.md)
2. server侧提供统一的外部接口storage的实现目前有三个，也可以自行替换
3. [列族的含义](../../kv/util/engine_util/Column-Families.md)就是用前置的方式将key隔离开，当前的实现存储是没有隔离的
4. engine_util中包含了列组迭代器(以cf作为前置去过滤)+对批量写的相关封装