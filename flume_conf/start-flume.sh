#!/bin/bash
# Flume 启动脚本: Kafka -> HDFS
# 将此脚本放到虚拟机后执行

FLUME_HOME=/home/hadoop/soft/apache-flume-1.10.0-bin
CONF_FILE=$FLUME_HOME/conf/kafka-to-hdfs.conf

echo "=========================================="
echo "  Flume: Kafka -> HDFS 数据传输"
echo "=========================================="
echo "Flume Home: $FLUME_HOME"
echo "Config File: $CONF_FILE"
echo ""

# 检查配置文件是否存在
if [ ! -f "$CONF_FILE" ]; then
    echo "错误: 配置文件不存在: $CONF_FILE"
    echo "请先将 kafka-to-hdfs.conf 复制到 $FLUME_HOME/conf/ 目录"
    exit 1
fi

# 检查 HDFS 是否可用
echo "检查 HDFS 连接..."
hdfs dfs -ls / > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "警告: HDFS 可能未启动或无法连接"
    echo "请确保 Hadoop 集群正常运行"
fi

# 创建 HDFS 目录
echo "创建 HDFS 目标目录..."
hdfs dfs -mkdir -p /user/hadoop/nowcoder_jobs

# 启动 Flume Agent
echo ""
echo "启动 Flume Agent..."
echo "使用 Ctrl+C 停止"
echo ""

$FLUME_HOME/bin/flume-ng agent \
    --name a1 \
    --conf $FLUME_HOME/conf \
    --conf-file $CONF_FILE \
    -Dflume.root.logger=INFO,console
