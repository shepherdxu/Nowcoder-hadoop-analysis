"""
优化的 Kafka Producer - 用于导入 nowcoder_jobs_edge.json 数据到 Kafka
特性：
1. 批量处理数据
2. 多线程发送
3. 异步回调处理
4. 进度显示
5. 错误处理和重试机制
"""

import json
import time
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import threading

# ==================== 配置参数 ====================
# 使用 IP 地址确保连接稳定
KAFKA_SERVERS = ['192.168.120.101:9092', '192.168.120.102:9092', '192.168.120.103:9092']
TOPIC_NAME = 'nowcoder_jobs'
NUM_PARTITIONS = 3  # 分区数
REPLICATION_FACTOR = 2  # 副本数

# 数据文件路径
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nowcoder_jobs_edge.json')

# 性能优化参数
BATCH_SIZE = 32768  # 批次大小（字节）
LINGER_MS = 5  # 延迟发送时间（毫秒），用于批量发送
BUFFER_MEMORY = 67108864  # 缓冲区大小（64MB）
MAX_REQUEST_SIZE = 1048576  # 最大请求大小（1MB）
COMPRESSION_TYPE = 'gzip'  # 压缩类型
NUM_THREADS = 4  # 发送线程数
CHUNK_SIZE = 500  # 每个chunk的记录数

# ==================== 统计计数器 ====================
class Counter:
    def __init__(self):
        self.success = 0
        self.failed = 0
        self.lock = threading.Lock()
    
    def increment_success(self):
        with self.lock:
            self.success += 1
    
    def increment_failed(self):
        with self.lock:
            self.failed += 1
    
    def get_counts(self):
        with self.lock:
            return self.success, self.failed

counter = Counter()

# ==================== 创建 Topic ====================
def create_topic_if_not_exists():
    """创建 Kafka Topic（如果不存在）"""
    print(f"\n{'='*60}")
    print(f"检查/创建 Topic: {TOPIC_NAME}")
    print(f"{'='*60}")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVERS,
            client_id='admin_client',
            request_timeout_ms=10000,
        )
        
        # 获取现有topic列表
        existing_topics = admin_client.list_topics()
        
        if TOPIC_NAME in existing_topics:
            print(f"✓ Topic '{TOPIC_NAME}' 已存在")
            admin_client.close()
            return True
        
        # 创建新topic
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        
        admin_client.create_topics([topic], validate_only=False)
        print(f"✓ Topic '{TOPIC_NAME}' 创建成功")
        print(f"  - 分区数: {NUM_PARTITIONS}")
        print(f"  - 副本数: {REPLICATION_FACTOR}")
        admin_client.close()
        return True
        
    except TopicAlreadyExistsError:
        print(f"✓ Topic '{TOPIC_NAME}' 已存在")
        return True
    except Exception as e:
        # 如果 AdminClient 出错，尝试直接发送（topic可能已存在）
        print(f"⚠ Topic 检查出错: {e}")
        print(f"  尝试继续发送数据（假设 topic 已存在）...")
        return True  # 继续执行，让 producer 尝试发送

# ==================== 创建 Producer ====================
def create_producer():
    """创建优化的 Kafka Producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # 确保所有副本确认
        retries=5,  # 重试次数
        retry_backoff_ms=100,  # 重试间隔
        batch_size=BATCH_SIZE,
        linger_ms=LINGER_MS,
        buffer_memory=BUFFER_MEMORY,
        max_request_size=MAX_REQUEST_SIZE,
        compression_type=COMPRESSION_TYPE,
        max_in_flight_requests_per_connection=5,
    )

# ==================== 回调函数 ====================
def on_success(record_metadata):
    """成功发送回调"""
    counter.increment_success()

def on_error(excp):
    """发送失败回调"""
    counter.increment_failed()
    print(f"✗ 发送失败: {excp}")

# ==================== 发送数据块 ====================
def send_chunk(producer, chunk, chunk_id):
    """发送一个数据块"""
    futures = []
    for job in chunk:
        # 使用 job_id 作为 key 进行分区
        key = str(job.get('job_id', ''))
        future = producer.send(TOPIC_NAME, value=job, key=key)
        future.add_callback(on_success)
        future.add_errback(on_error)
        futures.append(future)
    return futures

# ==================== 加载数据 ====================
def load_data():
    """加载 JSON 数据文件"""
    print(f"\n{'='*60}")
    print(f"加载数据文件: {DATA_FILE}")
    print(f"{'='*60}")
    
    if not os.path.exists(DATA_FILE):
        print(f"✗ 数据文件不存在: {DATA_FILE}")
        return None
    
    file_size = os.path.getsize(DATA_FILE)
    print(f"文件大小: {file_size / 1024 / 1024:.2f} MB")
    
    start_time = time.time()
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    load_time = time.time() - start_time
    
    print(f"✓ 加载完成，共 {len(data):,} 条记录")
    print(f"  加载耗时: {load_time:.2f} 秒")
    
    return data

# ==================== 主函数 ====================
def main():
    """主函数"""
    print("\n" + "="*60)
    print("  Kafka Jobs Data Producer")
    print("  牛客网招聘数据导入工具")
    print("="*60)
    print(f"Kafka 集群: {', '.join(KAFKA_SERVERS)}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"配置: batch_size={BATCH_SIZE}, linger_ms={LINGER_MS}")
    print(f"压缩: {COMPRESSION_TYPE}")
    
    # 1. 创建 Topic
    if not create_topic_if_not_exists():
        print("\n无法创建 Topic，退出程序")
        return
    
    # 2. 加载数据
    data = load_data()
    if data is None:
        return
    
    # 3. 创建 Producer
    print(f"\n{'='*60}")
    print("开始发送数据到 Kafka...")
    print(f"{'='*60}")
    
    producer = create_producer()
    
    # 分割数据为 chunks
    chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    total_chunks = len(chunks)
    
    print(f"总记录数: {len(data):,}")
    print(f"分块数: {total_chunks} (每块 {CHUNK_SIZE} 条)")
    
    start_time = time.time()
    
    try:
        # 发送所有 chunks
        for idx, chunk in enumerate(chunks):
            send_chunk(producer, chunk, idx)
            
            # 打印进度
            if (idx + 1) % 10 == 0 or idx == total_chunks - 1:
                success, failed = counter.get_counts()
                progress = (idx + 1) / total_chunks * 100
                elapsed = time.time() - start_time
                rate = success / elapsed if elapsed > 0 else 0
                print(f"  进度: {progress:5.1f}% | 成功: {success:,} | 失败: {failed} | 速率: {rate:.0f} msg/s")
        
        # 等待所有消息发送完成
        print("\n等待所有消息发送完成...")
        producer.flush()
        
    except KeyboardInterrupt:
        print("\n\n用户中断，正在清理...")
    except Exception as e:
        print(f"\n✗ 发送过程中发生错误: {e}")
    finally:
        producer.close()
    
    # 4. 打印统计信息
    end_time = time.time()
    total_time = end_time - start_time
    success, failed = counter.get_counts()
    
    print(f"\n{'='*60}")
    print("  发送完成统计")
    print(f"{'='*60}")
    print(f"总耗时: {total_time:.2f} 秒")
    print(f"成功: {success:,} 条")
    print(f"失败: {failed} 条")
    print(f"发送速率: {success / total_time:.0f} 消息/秒")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()
