# 大数据环境启动指南

## 启动顺序（重要！按顺序执行）

### 1️⃣ 启动 Zookeeper（所有节点）
```bash
# 在 hadoop101, hadoop102, hadoop103 分别执行
zkServer.sh start

# 验证状态
zkServer.sh status
```

### 2️⃣ 启动 HDFS（在 hadoop101）
```bash
start-dfs.sh

# 验证 HA 状态
hdfs haadmin -getServiceState nn1
hdfs haadmin -getServiceState nn2
```

### 3️⃣ 启动 YARN（在 hadoop101）
```bash
start-yarn.sh

# 验证
yarn node -list
```

### 4️⃣ 启动 Kafka（所有节点）
```bash
# 在 hadoop101, hadoop102, hadoop103 分别执行
cd /home/hadoop/soft/kafka_2.13-3.0.0
bin/kafka-server-start.sh -daemon config/server.properties

# 验证
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 5️⃣ 启动 Flume（在 hadoop101）
```bash
cd /home/hadoop/soft/apache-flume-1.10.0-bin
bin/flume-ng agent --name a1 --conf conf --conf-file conf/kafka-to-hdfs.conf -Dflume.root.logger=INFO,console
```

---

## 关闭顺序（反向执行）

### 停止 Flume
```bash
# Ctrl+C 或
kill $(jps | grep Application | awk '{print $1}')
```

### 停止 Kafka（所有节点）
```bash
cd /home/hadoop/soft/kafka_2.13-3.0.0
bin/kafka-server-stop.sh
```

### 停止 YARN
```bash
stop-yarn.sh
```

### 停止 HDFS
```bash
stop-dfs.sh
```

### 停止 Zookeeper（所有节点）
```bash
zkServer.sh stop
```

---

## 验证命令

| 服务 | 验证命令 |
|------|----------|
| Zookeeper | `zkServer.sh status` |
| HDFS | `hdfs dfs -ls /` |
| HDFS HA | `hdfs haadmin -getServiceState nn1` |
| YARN | `yarn node -list` |
| Kafka | `bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| 进程检查 | `jps` |

---

## 发送数据到 Kafka（Windows）
```powershell
& d:/NIIT/niit/Scripts/python.exe d:/NIIT/kafka_jobs_producer.py
```
