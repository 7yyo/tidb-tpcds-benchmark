# tidb-tpcds-benchmark

这是为了方便使用 tidb 执行 tpc-ds 而做的一个小工具。集成了一些功能：

1. 运行 tpc-ds SQL
2. 查询 tpc-ds 所有表的数据量
3. analyze 所有表
4. explain analyze 所有 SQL 并输出文件
5. 将 tpc-ds 生成数据的文件名修改为 tidb-lightning 支持格式，方便做数据导入

## 参数
```properties
host=172.16.5.133:4000 
user=root              
password=              
db=test               
variables=\            
  set @@tidb_isolation_read_engines='tiflash';\
  set @@tidb_allow_mpp=1;\
  set @@tidb_mem_quota_query=12884901888;

# tpc-ds query 文件路径
file=/Users/yuyang/Desktop/sql 
# job 类型，以下参数对应上面提到的六个功能:
# tidb
# row
# analyze
# explain_analyze
# replace
job=explain_analyze 
```