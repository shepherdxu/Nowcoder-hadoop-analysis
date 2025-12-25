import json
import re
import os
from collections import Counter

# ==========================================
# 配置常量
# ==========================================
INPUT_FILE = 'nowcoder_jobs_edge.json'
OUTPUT_FILE = 'nowcoder_jobs_cleaned.json'
HIGH_VALUE_FILE = 'nowcoder_jobs_high_value.json'
REPORT_FILE = 'cleaning_report.json'

# 公司名称中常见的非真实名称标签
COMPANY_TAGS = {
    "体验很好", "独角兽企业", "股权激励", "待遇好", "工资高", 
    "互联网综合", "通信电子", "信息安全", "平台大", "数据服务",
    "计算机软件", "游戏", "文娱内容", "硬件", "企业服务", 
    "移动互联网", "电商", "教育", "金融", "医疗健康", "生活服务",
    "房产家居", "旅游", "社交网络", "分类信息", "物流运输", 
    "其它", "高新技术", "上市公司", "外企", "国企", "事业单位",
    "创业公司", "融资", "天使轮", "A轮", "B轮", "C轮", "D轮及以上",
    "不需要融资", "少于15人", "15-50人", "50-150人", "150-500人",
    "500-2000人", "2000人以上", "交通便利", "同事nice",
    "年底双薪", "股票期权", "带薪年假", "绩效奖金", "定期体检"
}

# 启发式规则：已知的真实大厂名称（用于从描述中匹配）
KNOWN_COMPANIES = [
    "字节跳动", "腾讯", "阿里", "百度", "美团", "京东", "快手", "网易", 
    "华为", "小米", "滴滴", "拼多多", "携程", "小红书", "bilibili", "哔哩哔哩",
    "得物", "去哪儿", "搜狐", "新浪", "360", "金山", "联想", "vivo", "oppo",
    "荣耀", "大疆", "海康威视", "科大讯飞", "商汤", "旷视", "蔚来", "理想",
    "小鹏", "特斯拉", "宁德时代", "比亚迪", "顺丰", "中国移动", "中国联通",
    "中国电信", "银行", "招商", "平安", "微众", "蚂蚁", "米哈游", "莉莉丝",
    "叠纸", "鹰角", "完美世界", "巨人网络", "吉比特", "游族", "数字马力",
    "神州泰岳", "海能达", "精智达", "群核科技", "同花顺", "深信服", "用友",
    "金蝶", "亚信", "中兴", "中科院", "研究所"
]

# ==========================================
# 工具函数
# ==========================================

def load_data(filepath):
    print(f"正在读取文件: {filepath} ...")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到文件 {filepath}")
        return []
    except json.JSONDecodeError as e:
        print(f"错误: JSON解析失败 - {e}")
        return []

def save_data(data, filepath):
    print(f"正在保存文件 (共 {len(data)} 条): {filepath} ...")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_text(text):
    if not text:
        return ""
    return text.strip()

def extract_salary(salary_str):
    """
    解析薪资字符串，返回结构化数据
    格式示例: "15-25K·14薪", "20-30K", "面议"
    """
    if not salary_str:
        return None
    
    salary_str = salary_str.replace("k", "K").replace(" ", "")
    
    if "面议" in salary_str:
        return {"min": None, "max": None, "months": None, "negotiable": True, "raw": salary_str}
    
    # 匹配 15-25K·14薪
    match_full = re.search(r'(\d+)-(\d+)K[·.](?:(\d+)薪)?', salary_str)
    if match_full:
        return {
            "min": int(match_full.group(1)), 
            "max": int(match_full.group(2)), 
            "months": int(match_full.group(3)) if match_full.group(3) else 12,
            "negotiable": False,
            "raw": salary_str
        }
        
    # 匹配简单格式 15-25K without month
    match_simple = re.search(r'(\d+)-(\d+)K', salary_str)
    if match_simple:
        return {
            "min": int(match_simple.group(1)), 
            "max": int(match_simple.group(2)), 
            "months": 12, # default
            "negotiable": False,
            "raw": salary_str
        }

    return {"min": None, "max": None, "months": None, "negotiable": False, "raw": salary_str}

def extract_from_description(desc):
    """从职位描述中提取信息"""
    if not desc:
        return {}
    
    extracted = {}
    
    # 提取收藏数
    match_coll = re.search(r'(\d+)位牛友收藏', desc)
    if match_coll:
        extracted['collection_count'] = int(match_coll.group(1))
    else:
        extracted['collection_count'] = 0
        
    # 提取活跃状态
    active_tags = []
    if "刚刚有人投递过" in desc:
        active_tags.append("刚刚有人投递过")
    if "HR近期来过" in desc:
        active_tags.append("HR近期来过")
    if "HR刚刚处理简历" in desc:
        active_tags.append("HR刚刚处理简历")
    if active_tags:
        extracted['active_status'] = "|".join(active_tags)
        
    # 提取城市 (扩展城市列表)
    common_cities = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "南京", "西安", "郑州", "长沙", "苏州", "天津", "重庆", "合肥", "厦门", "珠海", "大连", "青岛", "石家庄", "宁波", "无锡", "福州", "济南", "沈阳", "哈尔滨", "长春", "昆明", "南宁", "贵阳", "南昌", "扬州", "东莞", "佛山"]
    for city in common_cities:
        if city in desc:
            extracted['city_candidate'] = city
            break
            
    # 提取学历 (按优先级)
    degrees = ["博士", "硕士", "本科", "专科"]
    for deg in degrees:
        if deg in desc:
            extracted['degree_candidate'] = deg
            break
            
    # 提取职位类型 (关键词匹配)
    job_type_keywords = {
        "AI": ["AI", "人工智能", "算法", "深度学习", "机器学习", "NLP", "CV", "机器视觉", "大模型", "SLAM", "机载"],
        "后端开发": ["Java", "C++", "Python", "Go", "Golang", "PHP", "Node", "后端", "服务器", "高性能计算"],
        "前端开发": ["前端", "JavaScript", "TypeScript", "Vue", "React", "Web"],
        "测试": ["测试", "QA"],
        "运维": ["运维", "SRE", "DevOps"],
        "数据": ["数据分析", "数据挖掘", "数据开发", "大数据", "ETL"],
        "硬件": ["硬件", "嵌入式", "芯片", "集成电路", "FPGA"]
    }
    for type_name, keywords in job_type_keywords.items():
        for kw in keywords:
            if kw.upper() in desc.upper():
                extracted['job_type_candidate'] = type_name
                # 找到一个高优先级的关键词就停止，这里简单取第一个匹配到的类型
                break
        if 'job_type_candidate' in extracted:
            break
            
    # 尝试提取真实公司名
    for company in KNOWN_COMPANIES:
        if company in desc:
            extracted['company_candidate'] = company
            break
    
    return extracted

def normalize_job_title(title):
    """清洗岗位名称"""
    if not title:
        title = "未知"
        
    title = clean_text(title)
    
    # 处理 "为你推荐" -> "未知"
    if "为你推荐" in title:
        return "未知", {}
        
    extracted = {}
    
    # 提取城市 (合肥)
    match_city = re.search(r'[（\(](.+?)[）\)]', title)
    if match_city:
        potential_city = match_city.group(1)
        if len(potential_city) >= 2 and len(potential_city) <= 5 and not re.match(r'[a-zA-Z0-9]+', potential_city):
             extracted['city_from_title'] = potential_city
    
    # 提取批次
    match_batch = re.search(r'(?:\[|【)?(20\d\d|[23]\d)届(?:\]|】|校招|秋招)?', title)
    if match_batch:
        extracted['batch'] = match_batch.group(1) + "届"
        
    return title, extracted

# ==========================================
# 主清洗逻辑
# ==========================================

def process_data(data):
    cleaned_rows = []
    stats = Counter()
    seen_ids = set()
    seen_content = set()
    
    print("开始清洗数据...")
    
    for row in data:
        stats['total_processed'] += 1
        
        job_id = str(row.get('job_id', '')).strip()
        
        # 去重
        if job_id and job_id in seen_ids:
            stats['duplicate_id'] += 1
            continue
        
        # 1. 字段提取
        raw_company = clean_text(row.get('公司名称', ''))
        raw_title = clean_text(row.get('岗位名称', ''))
        raw_desc = clean_text(row.get('职位描述', ''))
        raw_city = clean_text(row.get('城市', ''))
        raw_salary = clean_text(row.get('薪资', ''))
        raw_degree = clean_text(row.get('学历要求', ''))
        raw_type = clean_text(row.get('职位类型', ''))
        
        clean_title, title_extracted = normalize_job_title(raw_title)
        desc_extracted = extract_from_description(raw_desc)
        
        # 2. 字段修复逻辑
        
        # 修复公司名称
        final_company = raw_company
        if raw_company in COMPANY_TAGS or len(raw_company) < 2:
            if 'company_candidate' in desc_extracted:
                final_company = desc_extracted['company_candidate']
                stats['company_restored_from_desc'] += 1
            else:
                final_company = "未知"
                stats['company_unknown'] += 1
        
        # 修复城市 (优先级: 原城市 > 标题提取 > 描述提取)
        # 但如果原城市字段实际上是无效的（空或显然错误），则覆盖
        # 此处简化策略：如果为空，尝试填充
        final_city = raw_city
        if not final_city:
            if 'city_from_title' in title_extracted:
                final_city = title_extracted['city_from_title']
                stats['city_restored_from_title'] += 1
            elif 'city_candidate' in desc_extracted:
                final_city = desc_extracted['city_candidate']
                stats['city_restored_from_desc'] += 1
            
        # 修复学历 (策略: 如果原值为"不限"或为空，且描述里有明确学历，则覆盖)
        final_degree = raw_degree
        if (not final_degree or final_degree == "不限") and 'degree_candidate' in desc_extracted:
            final_degree = desc_extracted['degree_candidate']
            stats['degree_restored_from_desc'] += 1
            
        # 修复职位类型
        final_type = raw_type
        if not final_type and 'job_type_candidate' in desc_extracted:
            final_type = desc_extracted['job_type_candidate']
            stats['type_restored_from_desc'] += 1
             
        final_salary_struct = extract_salary(raw_salary)
             
        # 3. 构建
        new_row = row.copy()
        new_row['岗位名称'] = clean_title
        new_row['公司名称'] = final_company
        new_row['城市'] = final_city
        new_row['学历要求'] = final_degree
        new_row['职位类型'] = final_type
        
        new_row['is_valid_job'] = clean_title != "未知"
        new_row['parsed_salary'] = final_salary_struct
        new_row['collection_count'] = desc_extracted.get('collection_count', 0)
        new_row['active_status'] = desc_extracted.get('active_status', '')
        new_row['batch'] = title_extracted.get('batch', row.get('毕业年份', ''))
        
        # 价值标签 (调整阈值 >= 10)
        value_tags = []
        if new_row['collection_count'] >= 10:
            value_tags.append("高收藏")
        if new_row['active_status']:
            value_tags.append("活跃")
        new_row['value_tags'] = value_tags
        
        content_hash = f"{final_company}_{clean_title}_{final_city}"
        if content_hash in seen_content:
             stats['duplicate_content_warning'] += 1
        
        cleaned_rows.append(new_row)
        if job_id:
            seen_ids.add(job_id)
        seen_content.add(content_hash)
        
    print("清洗完成。")
    return cleaned_rows, stats

# ==========================================
# 执行入口
# ==========================================

if __name__ == "__main__":
    try:
        if not os.path.exists(INPUT_FILE):
            print(f"找不到输入文件: {INPUT_FILE}")
            exit(1)
            
        raw_data = load_data(INPUT_FILE)
        if not raw_data:
            print("数据为空，退出。")
            exit(1)
            
        cleaned_data, stats = process_data(raw_data)
        
        # 筛选高价值岗位
        high_value_jobs = [row for row in cleaned_data if row.get('value_tags')]
        
        # 保存结果
        save_data(cleaned_data, OUTPUT_FILE)
        save_data(high_value_jobs, HIGH_VALUE_FILE)
        
        # 保存报告
        report = {
            "input_count": len(raw_data),
            "output_count": len(cleaned_data),
            "high_value_count": len(high_value_jobs),
            "stats": dict(stats)
        }
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            
        print("="*30)
        print("清洗报告 Summary:")
        print(f"原数据: {len(raw_data)}")
        print(f"处理后: {len(cleaned_data)}")
        print(f"高价值: {len(high_value_jobs)}")
        print(f"公司名修复: {stats['company_restored_from_desc']}")
        print(f"公司名未知: {stats['company_unknown']}")
        print(f"城市修复: {stats['city_restored_from_desc'] + stats['city_restored_from_title']}")
        print(f"ID去重: {stats['duplicate_id']}")
        print("="*30)
    except Exception as e:
        import traceback
        with open('error.log', 'w', encoding='utf-8') as f:
            f.write(traceback.format_exc())
        print("程序发生错误，详情请查看 error.log")
        exit(1)
