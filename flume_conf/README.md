# Flume Kafka -> HDFS 部署指南

## 配置文件说明

### kafka-to-hdfs.conf
- **Kafka Source**: 从 `nowcoder_jobs` topic 消费数据
- **Memory Channel**: 内存缓冲，容量 10000 events
- **HDFS Sink**: 写入 `/user/hadoop/nowcoder_jobs/` 目录

## 部署步骤

### 1. 复制配置文件到虚拟机
在 Windows PowerShell 中执行:
```powershell
# 使用 scp 复制配置文件
scp d:\NIIT\flume_conf\kafka-to-hdfs.conf hadoop@192.168.120.101:/home/hadoop/soft/apache-flume-1.10.0-bin/conf/
scp d:\NIIT\flume_conf\start-flume.sh hadoop@192.168.120.101:/home/hadoop/soft/apache-flume-1.10.0-bin/
```

### 2. 在虚拟机上执行
SSH 登录到 hadoop101:
```bash
ssh hadoop@192.168.120.101
# 密码: 111
```

### 3. 确保 Hadoop HDFS 已启动
```bash
# 检查 HDFS 状态
hdfs dfs -ls /

# 如果 HDFS 未启动，启动 Hadoop
start-dfs.sh
start-yarn.sh
```

### 4. 启动 Flume
```bash
cd /home/hadoop/soft/apache-flume-1.10.0-bin
chmod +x start-flume.sh
./start-flume.sh
```

或者直接运行:
```bash
cd /home/hadoop/soft/apache-flume-1.10.0-bin
bin/flume-ng agent \
    --name a1 \
    --conf conf \
    --conf-file conf/kafka-to-hdfs.conf \
    -Dflume.root.logger=INFO,console
```

### 5. 验证数据写入
```bash
# 查看 HDFS 目录
hdfs dfs -ls /user/hadoop/nowcoder_jobs/

# 查看文件内容
hdfs dfs -cat /user/hadoop/nowcoder_jobs/dt=*/jobs*.json | head -20
```

## 注意事项
1. 确保 Kafka 服务正在运行
2. 确保 HDFS 目录有写入权限
3. 如果 HDFS 使用 HA 模式，需要修改 `hdfs.path` 中的 namenode 地址
