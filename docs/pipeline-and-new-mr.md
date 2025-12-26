# 完整数据管道 + 新增 MapReduce 任务

## 一、数据管道执行流程

### Step 1: 启动虚拟机服务（按顺序）
```bash
# 1. Zookeeper (所有节点)
zkServer.sh start

# 2. HDFS
start-dfs.sh

# 3. YARN  
start-yarn.sh

# 4. Kafka (所有节点)
cd /home/hadoop/soft/kafka_2.13-3.0.0
bin/kafka-server-start.sh -daemon config/server.properties
```

### Step 2: Windows - 发送数据到 Kafka
```powershell
cd d:\NIIT
& d:/NIIT/niit/Scripts/python.exe d:/NIIT/kafka_jobs_producer.py
```
预期：7,000+ 条数据成功发送

### Step 3: 虚拟机 - 启动 Flume (Kafka → HDFS)
```bash
cd /home/hadoop/soft/apache-flume-1.10.0-bin

# 使用全新的 consumer group 确保从头消费
vi conf/kafka-to-hdfs.conf
# 修改: a1.sources.kafka-source.kafka.consumer.group.id = flume-hdfs-new-20251225

bin/flume-ng agent --name a1 --conf conf --conf-file conf/kafka-to-hdfs.conf -Dflume.root.logger=INFO,console
```

### Step 4: 验证 HDFS 数据
```bash
hdfs dfs -ls -R /user/hadoop/nowcoder_jobs/
hdfs dfs -cat /user/hadoop/nowcoder_jobs/dt=*/jobs*.json 2>/dev/null | head -3
```

---

## 二、新增字段说明

| 字段 | 类型 | 说明 | 用途 |
|------|------|------|------|
| `is_valid_job` | boolean | 是否有效岗位 | 数据过滤 |
| `parsed_salary.min` | int | 最低薪资(K) | 薪资分析 |
| `parsed_salary.max` | int | 最高薪资(K) | 薪资分析 |
| `parsed_salary.months` | int | 薪资月数 | 年薪计算 |
| `parsed_salary.negotiable` | boolean | 是否面议 | 统计面议比例 |
| `collection_count` | int | 收藏数 | 热门岗位排名 |
| `active_status` | string | 活跃状态 | 活跃岗位筛选 |
| `value_tags` | array | 标签["高收藏","活跃"] | 分类统计 |

---

## 三、新增 MapReduce 任务建议

### MR11: 高收藏岗位分析
**目的**: 统计高收藏岗位的城市/技能/薪资分布
```
Mapper: 过滤 collection_count >= 50 的岗位
        输出 <城市, (jobName, collection_count, avgSalary)>
Reducer: 汇总各城市高收藏岗位数量和平均薪资
MySQL表: high_collection_jobs
```

### MR12: 活跃岗位分析
**目的**: 分析"刚刚有人投递过"等活跃岗位特征
```
Mapper: 过滤 active_status 非空的岗位
        或 value_tags 包含 "活跃" 的岗位
        输出 <城市, (count, avgSalary)>
Reducer: 统计各城市活跃岗位数量
MySQL表: active_jobs_stats
```

### MR13: 薪资面议比例分析
**目的**: 统计各城市/公司类型的薪资面议比例
```
Mapper: 输出 <城市, (negotiable ? 1 : 0, 1)>
Reducer: 计算 negotiable_ratio = sum(negotiable) / sum(total)
MySQL表: negotiable_ratio_stats
```

### MR14: 高收藏岗位技能热度
**目的**: 分析高收藏岗位中最受欢迎的技能
```
Mapper: 过滤 collection_count >= 50
        拆分技能标签, 输出 <skill, collection_count>
Reducer: 聚合每个技能的总收藏数
MySQL表: skill_collection_rank
```

### MR15: 活跃度-薪资关联
**目的**: 分析活跃岗位与非活跃岗位的薪资差异
```
Mapper: 输出 <active_flag, (salarySum, count)>
Reducer: 计算活跃/非活跃岗位的平均薪资对比
MySQL表: activity_salary_comparison
```

---

## 四、衍生统计建议

1. **热门岗位榜单**: 根据 collection_count 排名 Top 100
2. **活跃公司榜**: 统计哪些公司的岗位最活跃
3. **高薪高收藏岗位**: 同时满足薪资高和收藏高的岗位
4. **城市竞争度**: 活跃岗位占比高的城市竞争更激烈
5. **薪资面议分析**: 哪些行业/城市面议比例高

---

## 五、JobRecord.java 更新

需要更新数据模型以支持新字段:
- `isValidJob` (boolean)
- `parsedSalaryMin/Max/Months` (int)  
- `negotiable` (boolean)
- `collectionCount` (int)
- `activeStatus` (String)
- `valueTags` (List<String>)
