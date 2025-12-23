
import os
import time
import json
import csv
import random
import logging
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

import config
from database import DatabaseManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawl_edge.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EdgeSpider:
    """使用Edge浏览器的爬虫"""
    
    def __init__(self, edgedriver_path: str = None, use_database: bool = True):
        """
        初始化爬虫
        
        Args:
            edgedriver_path: EdgeDriver路径（可选，如果为None则使用系统PATH）
            use_database: 是否使用数据库
        """
        self.base_url = "https://www.nowcoder.com"
        self.job_center_url = "https://www.nowcoder.com/jobs/school/jobs"
        self.driver = None
        self.data_list = []
        self.use_database = use_database
        self.db_manager = None
        self.edgedriver_path = edgedriver_path
        self.seen_job_ids = set()  # 用于去重，记录已爬取的job_id
        
        # 初始化数据库
        if self.use_database:
            self._init_database()
        
        # 初始化Selenium
        self._init_selenium()
    
    def _init_database(self):
        """初始化数据库"""
        try:
            self.db_manager = DatabaseManager(**config.DATABASE_CONFIG)
            self.db_manager.create_database_if_not_exists()
            if self.db_manager.connect():
                self.db_manager.create_tables()
                logger.info("数据库初始化成功")
            else:
                logger.warning("数据库连接失败，将仅保存到文件")
                self.use_database = False
        except Exception as e:
            logger.warning(f"数据库初始化失败: {str(e)}")
            self.use_database = False
    
    def _init_selenium(self):
        """初始化Selenium - 使用Edge浏览器"""
        try:
            edge_options = Options()
            # 指定Edge浏览器路径（如果找不到）
            edge_binary_paths = [
                r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            ]
            for path in edge_binary_paths:
                if os.path.exists(path):
                    edge_options.binary_location = path
                    logger.info(f"指定Edge浏览器路径: {path}")
                    break
            
            # 基本选项（使用最简单的配置，确保能启动）
            edge_options.add_argument('--disable-gpu')
            edge_options.add_argument('--no-sandbox')
            
            driver_initialized = False
            
            # 方法1：如果指定了路径，使用指定路径
            if self.edgedriver_path and os.path.exists(self.edgedriver_path):
                try:
                    logger.info(f"使用指定的EdgeDriver: {self.edgedriver_path}")
                    service = Service(self.edgedriver_path)
                    # 不添加service_args，使用默认配置
                    self.driver = webdriver.Edge(service=service, options=edge_options)
                    driver_initialized = True
                    logger.info("成功使用指定的EdgeDriver")
                    
                    # 启动成功后再添加反检测脚本
                    try:
                        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                            'source': '''
                                Object.defineProperty(navigator, 'webdriver', {
                                    get: () => undefined
                                })
                            '''
                        })
                    except:
                        pass  # 如果失败也不影响使用
                except Exception as e:
                    logger.warning(f"使用指定路径失败: {str(e)}")
                    import traceback
                    logger.debug(traceback.format_exc())
            
            # 方法2：尝试使用系统PATH中的EdgeDriver
            if not driver_initialized:
                try:
                    logger.info("尝试使用系统PATH中的EdgeDriver...")
                    self.driver = webdriver.Edge(options=edge_options)
                    driver_initialized = True
                    logger.info("成功使用系统PATH中的EdgeDriver")
                except Exception as e:
                    logger.debug(f"系统PATH EdgeDriver失败: {str(e)}")
            
            # 方法3：尝试使用webdriver-manager（需要网络）
            if not driver_initialized:
                try:
                    from webdriver_manager.microsoft import EdgeChromiumDriverManager
                    logger.info("尝试使用webdriver-manager下载EdgeDriver...")
                    service = Service(EdgeChromiumDriverManager().install())
                    self.driver = webdriver.Edge(service=service, options=edge_options)
                    driver_initialized = True
                    logger.info("成功使用webdriver-manager下载的EdgeDriver")
                except Exception as e1:
                    logger.warning(f"webdriver-manager失败: {str(e1)}")
                    logger.warning("可能是网络问题，无法下载EdgeDriver")
            
            # 如果都失败了
            if not driver_initialized:
                raise Exception("无法初始化EdgeDriver")
            
            logger.info("Edge WebDriver 初始化成功")
            
        except Exception as e:
            logger.error(f"Edge WebDriver初始化失败: {str(e)}")
            raise
    
    def _wait_for_page_load(self, timeout: int = 15):
        """等待页面加载"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(3)  # 额外等待JavaScript执行
        except:
            logger.warning("页面加载超时，继续尝试...")
            time.sleep(5)
    
    def _extract_jobs_from_page(self) -> list:
        """从当前页面提取职位信息"""
        jobs = []
        
        try:
            # 等待页面加载
            self._wait_for_page_load()
            
            # 等待职位元素实际出现（SPA页面需要等待JS渲染）
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/jobs/detail/"]'))
                )
                logger.info("职位链接元素已加载")
            except:
                logger.warning("等待职位链接元素超时，尝试继续...")
            
            # 滚动页面以触发懒加载
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # 获取页面源码
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 尝试多种方法提取数据
            job_elements = []
            
            # 方法1：直接找包含职位链接的元素（最可靠）
            job_links = soup.find_all('a', href=re.compile(r'/jobs/detail/\d+'))
            if job_links:
                logger.info(f"找到 {len(job_links)} 个职位链接")
                # 获取每个链接的父容器（通常是职位卡片）
                for link in job_links:
                    # 向上找到一个合理大小的父容器
                    parent = link.find_parent(['div', 'li', 'article', 'section'])
                    if parent and parent not in job_elements:
                        # 验证父容器包含足够的内容
                        text = parent.get_text(strip=True)
                        if 30 < len(text) < 1000:
                            job_elements.append(parent)
            
            # 方法2：如果方法1没找到，使用class选择器
            if not job_elements:
                selectors = [
                    {'class': re.compile(r'job.*item|item.*job', re.I)},
                    {'class': re.compile(r'position|job.*list', re.I)},
                    {'data-job-id': True},
                ]
                
                for selector in selectors:
                    elements = soup.find_all(['div', 'li', 'article', 'section'], selector)
                    if elements:
                        job_elements = elements
                        logger.info(f"通过选择器找到 {len(elements)} 个职位元素")
                        break
            
            # 方法3：如果都没找到，尝试查找包含关键词的元素
            if not job_elements:
                logger.info("使用关键词搜索元素...")
                all_elements = soup.find_all(['div', 'li', 'article'])
                for elem in all_elements:
                    text = elem.get_text()
                    if any(kw in text for kw in ['招聘', '职位', '岗位', '薪资', '公司']) and 50 < len(text) < 500:
                        job_elements.append(elem)
                        if len(job_elements) >= 20:
                            break
            
            logger.info(f"共找到 {len(job_elements)} 个待解析的职位元素")
            
            # 解析每个职位
            for elem in job_elements:
                job = self._parse_job_element(elem)
                # 验证数据有效性
                if job:
                    is_valid, reason = self._is_valid_job(job)
                    if is_valid:
                        jobs.append(job)
                    else:
                        logger.debug(f"过滤数据: {job.get('岗位名称', 'N/A')[:20]} - 原因: {reason}")
            
            logger.info(f"从页面提取到 {len(jobs)} 条职位信息")
            
        except Exception as e:
            logger.error(f"提取职位信息时出错: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return jobs
    
    def _is_valid_job(self, job: dict) -> tuple:
        """
        验证职位数据是否有效
        
        Args:
            job: 职位数据字典
            
        Returns:
            (是否有效, 过滤原因)
        """
        job_name = job.get('岗位名称', '').strip()
        company_name = job.get('公司名称', '').strip()
        job_link = job.get('职位链接', '').strip()
        
        # 1. 必须有职位名称且长度合理
        if not job_name or len(job_name) < 3:
            return (False, f"职位名称太短或为空: '{job_name}'")
        
        # 2. 必须有职位链接（放宽条件：只要包含 nowcoder.com 即可）
        if not job_link or 'nowcoder.com' not in job_link:
            return (False, f"链接无效: '{job_link[:50] if job_link else 'empty'}'")
        
        # 3. 过滤明显的无效数据（精简关键词列表）
        invalid_keywords = [
            '发布职位', '邀约', '助力', '简历加分', 
            '直达官网', '投后必反馈',
            '高薪榜', '必争榜'
        ]
        
        # 检查岗位名称是否包含无效关键词
        for keyword in invalid_keywords:
            if keyword in job_name:
                return (False, f"包含无效关键词: '{keyword}'")
        
        # 4. 过滤单独的城市名、学历等
        if job_name in ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', 
                        '武汉', '西安', '苏州', '天津', '重庆', '石家庄', '本科', 
                        '硕士', '博士', '专科', '不限']:
            return (False, f"职位名称是城市/学历: '{job_name}'")
        
        # 5. 过滤明显不是职位的短文本
        if len(job_name) < 4 and job_name not in ['UI', 'UX', 'AI', 'IT']:
            return (False, f"职位名称太短: '{job_name}'")
        
        # 6. 必须有公司名称（至少2个字符）
        if not company_name or len(company_name) < 2:
            return (False, f"公司名称无效: '{company_name}'")
        
        # 7. 公司名称不能是薪资、城市等
        if company_name in ['薪资面议', '200-300元/天', '120-400元/天']:
            return (False, f"公司名称是薪资: '{company_name}'")
        
        return (True, None)

    def _parse_job_element(self, elem) -> dict:
        """解析单个职位元素"""
        try:
            job = {
                '岗位名称': '',
                '公司名称': '',
                '薪资': '',
                '学历要求': '',
                '城市': '',
                '职位类型': '',
                '招聘人数': '',
                '公司类型': '',
                '公司性质': '',
                '毕业年份': '',
                '每周工作天数': '',
                '实习时长': '',
                '是否有转正': '',
                '职位描述': '',
                '职位链接': '',
                'job_id': '',
            }
            
            text = elem.get_text(separator='\n', strip=True)
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            used_lines = set()  # 记录已经被用来填字段的行索引
            
            # 提取职位名称（优先查找包含职位关键词的行）
            job_keywords = ['工程师', '开发', '算法', '产品', '运营', '设计', '经理', 
                          '专员', '助理', '分析师', '架构师', '测试', '运维', '前端', 
                          '后端', '全栈', 'Java', 'Python', 'C++', 'Go', 'PHP', '实习']
            
            for i, line in enumerate(lines[:10]):  # 扩大搜索范围
                line_clean = line.strip()
                # 检查是否包含职位关键词
                if any(kw in line_clean for kw in job_keywords):
                    # 确保不是无效数据
                    if len(line_clean) >= 4 and '发布' not in line_clean and 'HR' not in line_clean:
                        job['岗位名称'] = line_clean
                        used_lines.add(i)
                        break
            
            # 如果没找到，使用第一行（但会在验证时过滤）
            if not job['岗位名称'] and lines:
                first_line = lines[0].strip()
                if len(first_line) >= 4:
                    job['岗位名称'] = first_line
                    used_lines.add(0)
            
            # 提取公司名称（更精确：从底部向上找真正的公司名）
            company_keywords = ['公司', '科技', '有限', '股份', '集团', '企业', '网络', '信息']
            cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉',
                      '西安', '苏州', '天津', '重庆', '广东']
            invalid_company_lines = [
                '助力简历加分', '校招高薪榜', '高校必争榜', '必争榜',
                'HR刚处理简历', 'HR今日在线', '投后必反馈', '直达官网',
            ]
            industry_keywords = [
                '互联网', '电商', '企业服务', '游戏', '硬件', '教育',
                '金融', '芯片', '人工智能', '大数据', '高新技术', '音乐'
            ]

            def is_company_candidate(line_clean: str) -> bool:
                # 明显无效：标签/城市/纯薪资/工作时间/人数
                if not line_clean:
                    return False
                if line_clean in invalid_company_lines:
                    return False
                if line_clean in cities:
                    return False
                if any(kw in line_clean for kw in [
                    '薪资', '元/天', '天/周', '个月', '简历', '榜', 'HR'
                ]):
                    return False
                # 人数规模，如 100-499人 / 1000-9999人 / 10000人以上
                if '人' in line_clean and any(x in line_clean for x in ['-', '以上']):
                    return False
                # 纯行业标签（游戏 / 硬件 / 电商 等）
                if line_clean in industry_keywords:
                    return False
                # 不能等于岗位名称
                if line_clean == job['岗位名称']:
                    return False
                # 合理长度
                if not (2 <= len(line_clean) <= 40):
                    return False
                return True

            # 从底部向上找最可能的公司名（通常公司在职位描述靠后的位置）
            for idx in range(len(lines) - 1, -1, -1):
                line = lines[idx]
                line_clean = line.strip()
                if not is_company_candidate(line_clean):
                    continue
                # 优先包含“公司 / 科技 / 有限 / 股份 / 集团”等关键字
                if any(kw in line_clean for kw in company_keywords):
                    job['公司名称'] = line_clean.replace('公司', '').strip()
                    used_lines.add(idx)
                    break
                # 否则作为备选（例如：优选智行、阿里巴巴、韶音科技、炎魂网络）
                if not job['公司名称']:
                    job['公司名称'] = line_clean.strip()
                    used_lines.add(idx)
            
            # 提取薪资（覆盖 元/天 / k / 万 / 面议 等形式）
            for idx, line in enumerate(lines):
                line_clean = line.replace(' ', '')
                if any(kw in line_clean for kw in ['薪资', '元/天', 'K', 'k', '万', '面议']):
                    job['薪资'] = line.strip()
                    used_lines.add(idx)
                    break
            
            # 提取城市
            cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '苏州', '天津', '重庆']
            for idx, line in enumerate(lines):
                for city in cities:
                    if city in line:
                        job['城市'] = city
                        used_lines.add(idx)
                        break
                if job['城市']:
                    break
            
            # 提取学历（优先从HTML元素中提取）
            education_keywords = ['本科', '硕士', '博士', '专科', '不限']
            # 方法1：尝试从class包含edu-level的元素中提取
            edu_elem = elem.find(class_=re.compile(r'edu.*level|education|degree', re.I))
            if edu_elem:
                edu_text = edu_elem.get_text(strip=True)
                for edu in education_keywords:
                    if edu in edu_text:
                        job['学历要求'] = edu
                        break
            # 方法2：如果方法1没找到，从文本行中查找
            if not job['学历要求']:
                for idx, line in enumerate(lines):
                    for edu in education_keywords:
                        if edu in line:
                            job['学历要求'] = edu
                            used_lines.add(idx)
                            break
                    if job['学历要求']:
                        break
            
            # 提取职位类型（例如：产品经理 / 后端开发 / 算法工程师 等）
            job_type_keywords = [
                '产品经理', '后端开发', '前端开发', '客户端开发', '算法工程师', '数据开发',
                '测试工程师', '运维工程师', '运营', '视觉设计', '交互设计', 'UI设计',
                '机器学习工程师', '深度学习工程师', 'NLP工程师', '大数据工程师'
            ]
            for kw in job_type_keywords:
                if kw in job['岗位名称']:
                    job['职位类型'] = kw
                    break
            if not job['职位类型']:
                for idx, line in enumerate(lines):
                    for kw in job_type_keywords:
                        if kw in line:
                            job['职位类型'] = kw
                            used_lines.add(idx)
                            break
                    if job['职位类型']:
                        break

            # 提取公司类型 / 行业（例如：企业服务、游戏、硬件、互联网等）
            industry_candidates = [
                '企业服务', '游戏', '硬件', '互联网', '电商', '教育',
                '金融', '芯片', '人工智能', '音乐', '高新技术'
            ]
            if not job['公司类型']:
                for idx, line in enumerate(lines):
                    for ind in industry_candidates:
                        if ind in line:
                            job['公司类型'] = ind
                            used_lines.add(idx)
                            break
                    if job['公司类型']:
                        break

            # 提取招聘人数（原公司规模字段，如 100-499人 / 1000-9999人 / 10000人以上）
            if not job['招聘人数']:
                for idx, line in enumerate(lines):
                    if '人' in line and any(x in line for x in ['-', '以上']):
                        job['招聘人数'] = line.strip()
                        used_lines.add(idx)
                        break
            
            
            # 提取公司性质（国企、私企、外企、合资、上市公司等）
            company_nature_keywords = ['国企', '央企', '私企', '民企', '外企', '合资', '上市公司', 
                                       '国有企业', '事业单位', '政府机关', '创业公司', '独角兽']
            for idx, line in enumerate(lines):
                for nature in company_nature_keywords:
                    if nature in line:
                        job['公司性质'] = nature
                        used_lines.add(idx)
                        break
                if job['公司性质']:
                    break
            # 也从岗位名称中提取（如"国企-校招java开发"）
            if not job['公司性质']:
                for nature in company_nature_keywords:
                    if nature in job.get('岗位名称', ''):
                        job['公司性质'] = nature
                        break
            
            # 提取毕业年份（如 毕业不限、2025届、2026届 等）
            graduation_year_keywords = ['毕业不限', '不限年份', '应届生', '往届生']
            for idx, line in enumerate(lines):
                # 检查特定关键词
                for kw in graduation_year_keywords:
                    if kw in line:
                        job['毕业年份'] = kw
                        used_lines.add(idx)
                        break
                if job['毕业年份']:
                    break
                # 检查届份模式（如2025届、2026届）
                year_match = re.search(r'(20\d{2})届', line)
                if year_match:
                    job['毕业年份'] = year_match.group(0)
                    used_lines.add(idx)
                    break
            
            # 提取每周工作天数（如 5天/周、4天/周、3天/周）
            work_days_pattern = re.compile(r'(\d+)\s*天\s*/\s*周')
            for idx, line in enumerate(lines):
                match = work_days_pattern.search(line)
                if match:
                    job['每周工作天数'] = match.group(1)  # 只保存数字，便于统计
                    used_lines.add(idx)
                    break
            
            # 提取实习时长（如 最少3个月、3个月以上、实习6个月）
            duration_pattern = re.compile(r'(最少|最短|至少)?(\d+)\s*个月(以上)?')
            for idx, line in enumerate(lines):
                match = duration_pattern.search(line)
                if match:
                    months = match.group(2)
                    prefix = match.group(1) or ''
                    suffix = match.group(3) or ''
                    if prefix or suffix:
                        job['实习时长'] = f"≥{months}个月"
                    else:
                        job['实习时长'] = f"{months}个月"
                    used_lines.add(idx)
                    break
            
            # 提取是否有转正（有转正、可转正、转正机会）
            for idx, line in enumerate(lines):
                if any(kw in line for kw in ['有转正', '可转正', '转正机会', '优秀转正']):
                    job['是否有转正'] = '是'
                    used_lines.add(idx)
                    break
                elif any(kw in line for kw in ['无转正', '不转正']):
                    job['是否有转正'] = '否'
                    used_lines.add(idx)
                    break
            
            # 提取福利标签（五险一金、带薪年假、餐补、交通补贴等）
            benefit_keywords = ['五险一金', '六险一金', '带薪年假', '年终奖', '餐补', '餐饮补贴',
                               '交通补贴', '住房补贴', '加班补贴', '节日福利', '定期体检',
                               '弹性工作', '免费班车', '股票期权', '员工旅游', '培训机会',
                               '补充医疗', '商业保险', '零食下午茶', '健身房', '团建活动']
            found_benefits = []
            for idx, line in enumerate(lines):
                for benefit in benefit_keywords:
                    if benefit in line and benefit not in found_benefits:
                        found_benefits.append(benefit)
                        used_lines.add(idx)
            if found_benefits:
                job['福利标签'] = ','.join(found_benefits)
            
            # 提取技能要求标签（编程语言、框架、工具等）
            skill_keywords = ['Java', 'Python', 'C++', 'C#', 'Go', 'Golang', 'PHP', 'JavaScript', 'TypeScript',
                             'React', 'Vue', 'Angular', 'Node.js', 'Spring', 'SpringBoot', 'Django', 'Flask',
                             'MySQL', 'PostgreSQL', 'MongoDB', 'Redis', 'Kafka', 'RabbitMQ',
                             'Docker', 'Kubernetes', 'K8s', 'Linux', 'Git', 'AWS', 'Azure',
                             'TensorFlow', 'PyTorch', '机器学习', '深度学习', 'NLP', 'CV',
                             'HTML', 'CSS', 'SQL', 'Spark', 'Hadoop', 'Flink', 'Elasticsearch']
            found_skills = []
            for idx, line in enumerate(lines):
                for skill in skill_keywords:
                    # 使用大小写不敏感匹配
                    if skill.lower() in line.lower() and skill not in found_skills:
                        found_skills.append(skill)
                        used_lines.add(idx)
            if found_skills:
                job['技能要求标签'] = ','.join(found_skills)
            
            # 提取链接
            link_elem = elem.find('a', href=True)
            if link_elem:
                href = link_elem['href']
                if href.startswith('/'):
                    job['职位链接'] = self.base_url + href
                elif href.startswith('http'):
                    job['职位链接'] = href
                else:
                    job['职位链接'] = self.base_url + '/' + href
                
                # 提取job_id
                job_id_match = re.search(r'/(\d+)/?$', job['职位链接'])
                if job_id_match:
                    job['job_id'] = job_id_match.group(1)
            
            # 职位描述
            if len(text) > 0:
                # 去掉已经用于其他字段的行，以及明显是标签/提示信息的行
                desc_lines = []
                for idx, line in enumerate(lines):
                    if idx in used_lines:
                        continue
                    if any(kw in line for kw in [
                        'HR刚处理简历', 'HR今日在线', '直达官网', '投后必反馈',
                        '校招高薪榜', '高校必争榜'
                    ]):
                        continue
                    desc_lines.append(line)
                desc_text = '\n'.join(desc_lines).strip()
                if desc_text:
                    job['职位描述'] = desc_text  # 获取完整职位描述，不再截断
            
            return job
            
        except Exception as e:
            logger.error(f"解析职位元素失败: {str(e)}")
            return {}

    def crawl(self, max_pages: int = 5, keyword: str = "软件开发", target_size_mb: float = 5.0):
        """
        爬取数据 - 使用输入框搜索和点击分页
        
        注意: 牛客网的校招页面是SPA，URL参数不支持分页，需要：
        1. 在搜索框输入关键词
        2. 点击分页器的页码按钮切换页面
        """
        logger.info(f"开始爬取，关键词: {keyword}，最大页数: {max_pages}，目标大小: {target_size_mb}MB")
        
        all_jobs = []
        
        try:
            # 步骤1：访问校招职位页面（只访问一次）
            logger.info(f"访问页面: {self.job_center_url}")
            self.driver.get(self.job_center_url)
            self._wait_for_page_load()
            time.sleep(2)
            
            # 步骤2：在搜索框中输入关键词
            try:
                logger.info(f"搜索关键词: {keyword}")
                # 查找搜索输入框
                search_input = None
                input_selectors = [
                    'input.el-input__inner[placeholder*="搜索"]',
                    'input.el-input__inner[placeholder*="公司名或职位名"]',
                    'input.el-input__inner',
                    'input[type="text"][placeholder]',
                ]
                
                for selector in input_selectors:
                    try:
                        inputs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for inp in inputs:
                            if inp.is_displayed():
                                search_input = inp
                                break
                        if search_input:
                            break
                    except:
                        continue
                
                if search_input:
                    search_input.clear()
                    search_input.send_keys(keyword)
                    time.sleep(0.5)
                    search_input.send_keys(Keys.ENTER)
                    logger.info("已输入关键词并按回车搜索")
                    time.sleep(3)  # 等待搜索结果加载
                else:
                    logger.warning("未找到搜索框，将直接爬取当前页面")
            except Exception as e:
                logger.warning(f"搜索失败: {str(e)}，将直接爬取当前页面")
            
            # 步骤3：分页循环
            current_page = 1
            consecutive_empty_pages = 0
            
            while current_page <= max_pages:
                try:
                    logger.info(f"正在爬取第 {current_page} 页...")
                    
                    # 提取当前页职位
                    jobs = self._extract_jobs_from_page()
                    
                    if not jobs:
                        logger.warning(f"第 {current_page} 页未找到职位数据")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 2:
                            logger.info("连续多页无数据，停止爬取")
                            break
                    else:
                        consecutive_empty_pages = 0
                        logger.info(f"第 {current_page} 页获取到 {len(jobs)} 条职位信息")
                        
                        # 保存数据（去重处理）
                        for job in jobs:
                            # 使用job_id去重
                            job_id = job.get('job_id', '')
                            if not job_id:
                                job_link = job.get('职位链接', '')
                                job_id_match = re.search(r'/jobs/detail/(\d+)', job_link)
                                if job_id_match:
                                    job_id = job_id_match.group(1)
                                    job['job_id'] = job_id
                            
                            if job_id and job_id in self.seen_job_ids:
                                logger.debug(f"跳过重复职位: {job.get('岗位名称', 'N/A')}")
                                continue
                            
                            if job_id:
                                self.seen_job_ids.add(job_id)
                            
                            all_jobs.append(job)
                            self.data_list.append(job)
                            
                            if self.use_database and self.db_manager:
                                self.db_manager.insert_job(job)
                    
                    # 检查数据大小
                    current_json_str = json.dumps(all_jobs, ensure_ascii=False, indent=2)
                    current_size_mb = len(current_json_str.encode('utf-8')) / (1024 * 1024)
                    logger.info(f"当前收集数据: {len(all_jobs)} 条, 大小: {current_size_mb:.2f} MB")
                    
                    # 实时保存到JSON文件
                    try:
                        with open('nowcoder_jobs_edge.json', 'w', encoding='utf-8') as f:
                            json.dump(self.data_list, f, ensure_ascii=False, indent=2)
                        logger.info(f"实时保存: {len(self.data_list)} 条数据已写入 nowcoder_jobs_edge.json")
                    except Exception as e:
                        logger.warning(f"实时保存失败: {str(e)}")
                    
                    if current_size_mb >= target_size_mb:
                        logger.info(f"已达到目标大小 {target_size_mb} MB，停止爬取")
                        break
                    
                    # 步骤4：点击下一页
                    if current_page >= max_pages:
                        break
                    
                    next_page = current_page + 1
                    clicked = self._click_page_number(next_page)
                    
                    if not clicked:
                        logger.info(f"无法点击第 {next_page} 页，可能已到最后一页")
                        break
                    
                    current_page = next_page
                    
                    # 等待页面更新
                    delay = random.uniform(2, 4)
                    logger.info(f"等待 {delay:.1f} 秒...")
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"爬取第 {current_page} 页时出错: {str(e)}")
                    break
            
        except Exception as e:
            logger.error(f"爬取过程出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        logger.info(f"爬取完成，共获取 {len(all_jobs)} 条职位信息")
        return all_jobs
    
    def crawl_by_category(self, career_job_ids: list = None, base_urls: list = None, 
                           max_pages_per_category: int = 20, target_size_mb: float = 5.0):
        """
        按职位类别爬取数据 - 支持多个URL基础和careerJob参数
        
        Args:
            career_job_ids: 职位类别ID列表
            base_urls: URL基础列表，如校招/实习/社招
            max_pages_per_category: 每个类别最多爬取的页数
            target_size_mb: 目标数据大小 (MB)
        """
        # 默认爬取后端开发的所有类别
        if career_job_ids is None:
            career_job_ids = [11002, 11003, 11004, 11005, 11006, 11007]
        
        # 默认爬取三种招聘类型
        if base_urls is None:
            base_urls = [
                'https://www.nowcoder.com/jobs/school/jobs',      # 校招
                'https://www.nowcoder.com/jobs/intern/center',    # 实习
                'https://www.nowcoder.com/jobs/fulltime/center',  # 社招
            ]
        
        total_categories = len(base_urls) * len(career_job_ids)
        logger.info(f"开始多URL爬取: {len(base_urls)}个URL × {len(career_job_ids)}个类别 = {total_categories}个组合")
        logger.info(f"每类最多: {max_pages_per_category}页，目标大小: {target_size_mb}MB")
        
        all_jobs = []
        
        try:
            for base_url in base_urls:
                # 确定URL类型
                if 'school' in base_url:
                    url_type = '校招'
                elif 'intern' in base_url:
                    url_type = '实习'
                elif 'fulltime' in base_url:
                    url_type = '社招'
                else:
                    url_type = '未知'
                
                logger.info(f"\n{'#'*60}")
                logger.info(f"开始爬取【{url_type}】: {base_url}")
                logger.info(f"{'#'*60}")
                
                for career_id in career_job_ids:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"[{url_type}] 爬取类别: careerJob={career_id}")
                    logger.info(f"{'='*50}")
                    
                    current_page = 1
                    consecutive_empty_pages = 0
                    
                    while current_page <= max_pages_per_category:
                        try:
                            # 构建URL - 处理不同的URL格式
                            if 'recruitType=' in base_url:
                                url = f"{base_url}&careerJob={career_id}"
                            else:
                                url = f"{base_url}?careerJob={career_id}"
                            
                            if current_page == 1:
                                logger.info(f"访问: {url}")
                                self.driver.get(url)
                                self._wait_for_page_load()
                                time.sleep(1)  # 加速：1秒
                            
                            logger.info(f"[{url_type}] 类别 {career_id} 第 {current_page} 页...")
                            
                            jobs = self._extract_jobs_from_page()
                            
                            if not jobs:
                                logger.warning(f"[{url_type}] 类别 {career_id} 第 {current_page} 页无数据")
                                consecutive_empty_pages += 1
                                if consecutive_empty_pages >= 2:
                                    logger.info(f"连续多页无数据，切换下一类别")
                                    break
                            else:
                                consecutive_empty_pages = 0
                                logger.info(f"获取到 {len(jobs)} 条职位")
                                
                                new_jobs_count = 0
                                for job in jobs:
                                    job_id = job.get('job_id', '')
                                    if not job_id:
                                        job_link = job.get('职位链接', '')
                                        job_id_match = re.search(r'/jobs/detail/(\d+)', job_link)
                                        if job_id_match:
                                            job_id = job_id_match.group(1)
                                            job['job_id'] = job_id
                                    
                                    if job_id and job_id in self.seen_job_ids:
                                        continue
                                    
                                    if job_id:
                                        self.seen_job_ids.add(job_id)
                                    
                                    job['careerJob'] = career_id
                                    job['招聘类型'] = url_type
                                    
                                    all_jobs.append(job)
                                    self.data_list.append(job)
                                    new_jobs_count += 1
                                    
                                    if self.use_database and self.db_manager:
                                        self.db_manager.insert_job(job)
                                
                                logger.info(f"新增 {new_jobs_count} 条（去重后）")
                            
                            # 记录数据量
                            logger.info(f"总数据: {len(all_jobs)} 条")
                            
                            # 实时保存
                            try:
                                with open('nowcoder_jobs_edge.json', 'w', encoding='utf-8') as f:
                                    json.dump(self.data_list, f, ensure_ascii=False, indent=2)
                            except Exception as e:
                                logger.warning(f"保存失败: {str(e)}")
                            
                            if current_page >= max_pages_per_category:
                                break
                            
                            next_page = current_page + 1
                            clicked = self._click_page_number(next_page)
                            
                            if not clicked:
                                logger.info(f"无法翻页，切换下一类别")
                                break
                            
                            current_page = next_page
                            
                            # 加速：1-2秒延迟
                            delay = random.uniform(1, 2)
                            time.sleep(delay)
                            
                        except Exception as e:
                            logger.error(f"爬取出错: {str(e)}")
                            break
                    
                    # 加速：类别间1秒
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"爬取过程出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"全部爬取完成，共获取 {len(all_jobs)} 条职位信息")
        logger.info(f"{'#'*60}")
        return all_jobs

    
    def _click_page_number(self, page_num: int) -> bool:
        """
        点击指定页码
        
        Args:
            page_num: 目标页码
            
        Returns:
            是否成功点击
        """
        try:
            logger.info(f"尝试点击第 {page_num} 页...")
            
            # 滚动到页面底部以确保分页器可见
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # 保存当前页面用于调试
            try:
                with open(f'debug_before_click_page_{page_num}.html', 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                logger.info(f"已保存调试页面: debug_before_click_page_{page_num}.html")
            except:
                pass
            
            # 查找分页器中的页码按钮 - 使用多种选择器
            # Element UI 分页器结构: ul.el-pager > li.number
            pager_selectors = [
                'ul.el-pager li.number',  # Element UI 标准
                '.el-pager li.number',
                '.el-pagination li.number',
                '.el-pager li',
                '.pagination li.page-item a',
            ]
            
            for selector in pager_selectors:
                try:
                    page_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    logger.info(f"选择器 '{selector}' 找到 {len(page_items)} 个元素")
                    
                    for item in page_items:
                        item_text = item.text.strip()
                        logger.debug(f"  页码元素文本: '{item_text}'")
                        if item_text == str(page_num):
                            # 确保元素可见
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)
                            time.sleep(0.5)
                            
                            # 点击
                            self.driver.execute_script("arguments[0].click();", item)
                            logger.info(f"成功点击第 {page_num} 页")
                            time.sleep(2)  # 等待内容加载
                            return True
                except Exception as e:
                    logger.debug(f"选择器 {selector} 失败: {str(e)}")
                    continue
            
            # 如果没找到具体页码，尝试点击"下一页"按钮
            logger.info("未找到页码按钮，尝试点击'下一页'按钮...")
            next_selectors = [
                'button.btn-next:not([disabled])',
                '.el-pagination .btn-next:not([disabled])',
                '.el-pagination button:last-child:not([disabled])',
                'li.btn-next:not(.disabled)',
                '.el-icon-arrow-right',
            ]
            
            for selector in next_selectors:
                try:
                    next_btns = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    logger.info(f"'下一页'选择器 '{selector}' 找到 {len(next_btns)} 个元素")
                    for next_btn in next_btns:
                        if next_btn.is_displayed():
                            self.driver.execute_script("arguments[0].click();", next_btn)
                            logger.info(f"通过'下一页'按钮跳转")
                            time.sleep(2)
                            return True
                except Exception as e:
                    logger.debug(f"下一页选择器 {selector} 失败: {str(e)}")
                    continue
            
            logger.warning(f"未找到第 {page_num} 页的按钮，请检查 debug_before_click_page_{page_num}.html")
            return False
            
        except Exception as e:
            logger.error(f"点击页码失败: {str(e)}")
            return False


    def save_to_csv(self, filename: str = 'nowcoder_jobs_edge.csv'):
        """保存到CSV"""
        if not self.data_list:
            logger.warning("没有数据可保存")
            return
        
        fieldnames = [
            '岗位名称', '公司名称', '薪资', '学历要求',
            '城市', '职位类型', '招聘人数', '公司类型', '公司性质',
            '毕业年份', '每周工作天数', '实习时长', '是否有转正',
            '职位描述', '职位链接', 
            'job_id', 'careerJob', '招聘类型'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                # extrasaction='ignore' 忽略旧数据中的多余字段
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.data_list)
            logger.info(f"数据已保存到 {filename}，共 {len(self.data_list)} 条")
        except Exception as e:
            logger.error(f"保存CSV失败: {str(e)}")
    
    def save_to_json(self, filename: str = 'nowcoder_jobs_edge.json'):
        """保存到JSON"""
        if not self.data_list:
            return
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.data_list, f, ensure_ascii=False, indent=2)
            logger.info(f"数据已保存到 {filename}，共 {len(self.data_list)} 条")
        except Exception as e:
            logger.error(f"保存JSON失败: {str(e)}")
    
    def close(self):
        """关闭资源"""
        if self.driver:
            self.driver.quit()
            logger.info("浏览器已关闭")
        if self.db_manager:
            self.db_manager.close()


def main():
    """主函数"""
    import sys
    
    # 默认尝试使用用户提供的路径
    default_paths = [
        r"D:\Desktop\APP\msedgedriver.exe",  # 用户提供的路径
        "msedgedriver.exe",  # 项目目录
    ]
    
    edgedriver_path = None
    
    # 检查默认路径
    for path in default_paths:
        if os.path.exists(path):
            edgedriver_path = os.path.abspath(path)
            logger.info(f"自动找到EdgeDriver: {edgedriver_path}")
            break
    
    # 如果命令行指定了路径，优先使用命令行参数
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith('--edgedriver-path='):
                edgedriver_path = arg.split('=', 1)[1]
                if not os.path.exists(edgedriver_path):
                    print(f"错误: 指定的路径不存在: {edgedriver_path}")
                    sys.exit(1)
    
    print("=" * 70)
    print("牛客网招聘信息爬虫 - Edge浏览器版本")
    print("=" * 70)
    print("\n此版本使用Edge浏览器，适合没有Chrome的用户")
    print("EdgeDriver会自动下载（如果网络可用）\n")
    
    spider = None
    try:
        spider = EdgeSpider(edgedriver_path=edgedriver_path, use_database=True)
        
        # 使用按类别爬取（扩展的职位类别）
        # 后端: 11002-11007
        # 其他软件开发相关类别
        career_job_ids = [
            # 后端开发 (Java, C++, C, C#, Python, Go)
            11002, 11003, 11004, 11005, 11006, 11007,
            # 更多开发类别
            11022,  # 其他开发
            11235, 11236,  # 开发相关
            143742, 143751,  # 新类别
            11023, 11024,  # 更多开发
            142692, 143753,
            11025, 11026,
            11238,
            143761, 143762,
            11027, 11028, 11029,
            143756, 142195,
            # 新增类别
            11092, 11093, 11095,
        ]
        # 三种招聘类型URL
        base_urls = [
            'https://www.nowcoder.com/jobs/school/jobs',              # 校招
            'https://www.nowcoder.com/jobs/intern/center?recruitType=2',   # 实习
            'https://www.nowcoder.com/jobs/fulltime/center?recruitType=3', # 社招
        ]
        
        print(f"爬取 {len(base_urls)} 种招聘类型 × {len(career_job_ids)} 个职位类别")
        print(f"URL类型: 校招、实习、社招")
        print(f"每类最多: 20 页")
        print(f"目标数据量: 5 MB")
        print(f"加速模式：页间延迟1-2秒")
        print("\n开始爬取...\n")
        
        jobs = spider.crawl_by_category(
            career_job_ids=career_job_ids,
            base_urls=base_urls,
            max_pages_per_category=20, 
            target_size_mb=5.0
        )
        
        if jobs:
            spider.save_to_csv()
            spider.save_to_json()
            
            print("\n" + "=" * 70)
            print("爬取成功！")
            print("=" * 70)
            print(f"共获取 {len(jobs)} 条职位信息")
            print("\n数据已保存到:")
            print("  - nowcoder_jobs_edge.csv")
            print("  - nowcoder_jobs_edge.json")
            if spider.use_database:
                print(f"  - 数据库: {config.DATABASE_CONFIG['database']}")
            print("=" * 70)
        else:
            print("\n未获取到数据")
            print("请查看 debug_edge_page_*.html 文件了解页面结构")
            print("或查看日志文件 crawl_edge.log")
    
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if spider:
            spider.close()


if __name__ == "__main__":
    main()
