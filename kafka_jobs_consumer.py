"""
Kafka Consumer - 用于验证 nowcoder_jobs 数据导入
功能：
1. 消费并显示消息
2. 统计消息数量
3. 支持从头开始消费或从最新开始
"""

import json
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ==================== 配置参数 ====================
KAFKA_SERVERS = ['192.168.120.101:9092', '192.168.120.102:9092', '192.168.120.103:9092']
TOPIC_NAME = 'nowcoder_jobs'
GROUP_ID = 'nowcoder_jobs_consumer_group'

# ==================== 创建 Consumer ====================
def create_consumer(from_beginning=True):
    """创建 Kafka Consumer"""
    auto_offset_reset = 'earliest' if from_beginning else 'latest'
    
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        max_poll_records=500,
    )

# ==================== 主函数 ====================
def main():
    """主函数"""
    print("\n" + "="*60)
    print("  Kafka Jobs Data Consumer")
    print("  牛客网招聘数据消费验证工具")
    print("="*60)
    print(f"Kafka 集群: {', '.join(KAFKA_SERVERS)}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Consumer Group: {GROUP_ID}")
    
    # 解析命令行参数
    from_beginning = '--from-beginning' in sys.argv or '-b' in sys.argv
    show_detail = '--detail' in sys.argv or '-d' in sys.argv
    max_messages = None
    
    for arg in sys.argv[1:]:
        if arg.startswith('--max='):
            try:
                max_messages = int(arg.split('=')[1])
            except ValueError:
                pass
    
    print(f"从头消费: {'是' if from_beginning else '否'}")
    print(f"显示详情: {'是' if show_detail else '否'}")
    print(f"最大消息数: {max_messages if max_messages else '无限制'}")
    print(f"\n{'='*60}")
    print("开始消费消息 (Ctrl+C 退出)...")
    print(f"{'='*60}\n")
    
    consumer = create_consumer(from_beginning)
    
    count = 0
    start_time = time.time()
    partition_counts = {}
    
    try:
        for message in consumer:
            count += 1
            
            # 统计每个分区的消息数
            partition = message.partition
            partition_counts[partition] = partition_counts.get(partition, 0) + 1
            
            if show_detail:
                job = message.value
                print(f"[P{partition}|O{message.offset}] {job.get('岗位名称', 'N/A')} - {job.get('公司名称', 'N/A')} - {job.get('薪资', 'N/A')}")
            else:
                # 每100条消息打印一次进度
                if count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"已消费: {count:,} 条 | 速率: {rate:.0f} msg/s")
            
            # 检查是否达到最大消息数
            if max_messages and count >= max_messages:
                print(f"\n已达到最大消息数 {max_messages}，停止消费")
                break
                
    except KeyboardInterrupt:
        print("\n\n用户中断，正在退出...")
    except Exception as e:
        print(f"\n✗ 消费过程中发生错误: {e}")
    finally:
        consumer.close()
    
    # 打印统计信息
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n{'='*60}")
    print("  消费完成统计")
    print(f"{'='*60}")
    print(f"总耗时: {total_time:.2f} 秒")
    print(f"消费消息数: {count:,} 条")
    print(f"消费速率: {count / total_time:.0f} 消息/秒" if total_time > 0 else "")
    print(f"\n分区消息分布:")
    for p, c in sorted(partition_counts.items()):
        print(f"  Partition {p}: {c:,} 条")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    print("\n用法: python kafka_jobs_consumer.py [选项]")
    print("选项:")
    print("  -b, --from-beginning  从头开始消费")
    print("  -d, --detail          显示消息详情")
    print("  --max=N               最多消费N条消息")
    print("")
    main()
