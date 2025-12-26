-- 牛客招聘数据分析 MySQL 表结构
-- 数据库: nowcoder_analysis

-- 创建数据库
CREATE DATABASE IF NOT EXISTS nowcoder_analysis 
    DEFAULT CHARACTER SET utf8mb4 
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE nowcoder_analysis;

-- ========================================
-- MR1: 城市岗位统计
-- ========================================
DROP TABLE IF EXISTS city_job_count;
CREATE TABLE city_job_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    job_count INT NOT NULL DEFAULT 0,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_job_count (job_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市岗位数量统计';

-- ========================================
-- MR2: 城市薪资统计
-- ========================================
DROP TABLE IF EXISTS city_salary_stats;
CREATE TABLE city_salary_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    avg_salary DECIMAL(10,2) COMMENT '平均薪资',
    min_salary INT COMMENT '最低薪资',
    max_salary INT COMMENT '最高薪资',
    job_count INT COMMENT '有效薪资岗位数',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_avg_salary (avg_salary DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市薪资统计';

-- ========================================
-- MR3: 技能热度统计
-- ========================================
DROP TABLE IF EXISTS skill_count;
CREATE TABLE skill_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    skill VARCHAR(100) NOT NULL UNIQUE,
    count INT NOT NULL DEFAULT 0,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_count (count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='技能标签热度';

-- ========================================
-- MR4: 技能薪资关联
-- ========================================
DROP TABLE IF EXISTS skill_salary_stats;
CREATE TABLE skill_salary_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    skill VARCHAR(100) NOT NULL UNIQUE,
    avg_salary DECIMAL(10,2),
    min_salary INT,
    max_salary INT,
    job_count INT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_avg_salary (avg_salary DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='技能薪资统计';

-- ========================================
-- MR5: 学历分布统计
-- ========================================
DROP TABLE IF EXISTS education_count;
CREATE TABLE education_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    education VARCHAR(50) NOT NULL UNIQUE,
    count INT NOT NULL DEFAULT 0,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='学历分布';

-- ========================================
-- MR6: 学历薪资关联
-- ========================================
DROP TABLE IF EXISTS education_salary_stats;
CREATE TABLE education_salary_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    education VARCHAR(50) NOT NULL UNIQUE,
    avg_salary DECIMAL(10,2),
    min_salary INT,
    max_salary INT,
    job_count INT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='学历薪资统计';

-- ========================================
-- MR7: 公司类型统计
-- ========================================
DROP TABLE IF EXISTS company_type_count;
CREATE TABLE company_type_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_type VARCHAR(100) NOT NULL UNIQUE,
    count INT NOT NULL DEFAULT 0,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_count (count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公司类型分布';

-- ========================================
-- MR8: 招聘类型统计
-- ========================================
DROP TABLE IF EXISTS recruit_type_count;
CREATE TABLE recruit_type_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    recruit_type VARCHAR(50) NOT NULL UNIQUE,
    count INT NOT NULL DEFAULT 0,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='招聘类型分布';

-- ========================================
-- MR9: 实习岗位统计
-- ========================================
DROP TABLE IF EXISTS internship_stats;
CREATE TABLE internship_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    internship_count INT DEFAULT 0,
    avg_duration VARCHAR(50) COMMENT '平均实习时长',
    conversion_rate DECIMAL(5,2) COMMENT '转正率',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实习岗位统计';

-- ========================================
-- MR10: 总览仪表盘
-- ========================================
DROP TABLE IF EXISTS dashboard_summary;
CREATE TABLE dashboard_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL UNIQUE,
    metric_value VARCHAR(255),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仪表盘总览数据';

-- ========================================
-- MR11: 高收藏岗位分析
-- ========================================
DROP TABLE IF EXISTS high_collection_jobs;
CREATE TABLE high_collection_jobs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    high_collection_count INT DEFAULT 0 COMMENT '高收藏岗位数(>=50)',
    avg_salary DECIMAL(10,2) COMMENT '平均薪资',
    total_collection INT DEFAULT 0 COMMENT '总收藏数',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_count (high_collection_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='高收藏岗位统计';

-- ========================================
-- MR12: 活跃岗位分析
-- ========================================
DROP TABLE IF EXISTS active_jobs_stats;
CREATE TABLE active_jobs_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    active_count INT DEFAULT 0 COMMENT '活跃岗位数',
    avg_salary DECIMAL(10,2) COMMENT '平均薪资',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_active (active_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活跃岗位统计';

-- ========================================
-- MR13: 薪资面议比例分析
-- ========================================
DROP TABLE IF EXISTS negotiable_ratio_stats;
CREATE TABLE negotiable_ratio_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL UNIQUE,
    total_count INT DEFAULT 0 COMMENT '总岗位数',
    negotiable_count INT DEFAULT 0 COMMENT '面议岗位数',
    negotiable_ratio DECIMAL(5,2) COMMENT '面议比例%',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='薪资面议比例';

-- ========================================
-- MR14: 高收藏岗位技能热度
-- ========================================
DROP TABLE IF EXISTS skill_collection_rank;
CREATE TABLE skill_collection_rank (
    id INT AUTO_INCREMENT PRIMARY KEY,
    skill VARCHAR(100) NOT NULL UNIQUE,
    total_collection INT DEFAULT 0 COMMENT '该技能总收藏数',
    job_count INT DEFAULT 0 COMMENT '岗位数',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_collection (total_collection DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='技能收藏热度排名';

-- ========================================
-- MR15: 活跃度-薪资关联
-- ========================================
DROP TABLE IF EXISTS activity_salary_comparison;
CREATE TABLE activity_salary_comparison (
    id INT AUTO_INCREMENT PRIMARY KEY,
    activity_type VARCHAR(20) NOT NULL UNIQUE COMMENT '活跃/非活跃',
    job_count INT DEFAULT 0,
    avg_salary DECIMAL(10,2),
    min_salary INT,
    max_salary INT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活跃度薪资对比';
