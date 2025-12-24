package nowcoder.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 牛客网招聘数据模型
 * 解析 JSON 记录并提供数据访问方法
 */
public class JobRecord {
    private String jobName; // 岗位名称
    private String companyName; // 公司名称
    private String salary; // 薪资（原始字符串）
    private String education; // 学历要求
    private String city; // 城市
    private String jobType; // 职位类型
    private String recruitCount; // 招聘人数
    private String companyType; // 公司类型
    private String companyNature; // 公司性质
    private String graduateYear; // 毕业年份
    private String workDaysPerWeek; // 每周工作天数
    private String internDuration; // 实习时长
    private String hasConversion; // 是否有转正
    private String jobDesc; // 职位描述
    private String jobLink; // 职位链接
    private String jobId; // job_id
    private String skillTags; // 技能要求标签
    private int careerJob; // careerJob
    private String recruitType; // 招聘类型

    // 薪资解析结果
    private Integer minSalary; // 最低薪资（月薪，单位：元）
    private Integer maxSalary; // 最高薪资（月薪，单位：元）
    private Integer salaryMonths; // 薪资月数

    // 薪资解析正则
    private static final Pattern SALARY_PATTERN = Pattern.compile("(\\d+)-(\\d+)[Kk]");
    private static final Pattern MONTHS_PATTERN = Pattern.compile("(\\d+)薪");

    /**
     * 从 JSON 字符串解析 JobRecord
     * 简单的手动解析，避免引入额外依赖
     */
    public static JobRecord fromJson(String json) {
        JobRecord record = new JobRecord();

        record.jobName = extractValue(json, "岗位名称");
        record.companyName = extractValue(json, "公司名称");
        record.salary = extractValue(json, "薪资");
        record.education = extractValue(json, "学历要求");
        record.city = extractValue(json, "城市");
        record.jobType = extractValue(json, "职位类型");
        record.recruitCount = extractValue(json, "招聘人数");
        record.companyType = extractValue(json, "公司类型");
        record.companyNature = extractValue(json, "公司性质");
        record.graduateYear = extractValue(json, "毕业年份");
        record.workDaysPerWeek = extractValue(json, "每周工作天数");
        record.internDuration = extractValue(json, "实习时长");
        record.hasConversion = extractValue(json, "是否有转正");
        record.jobDesc = extractValue(json, "职位描述");
        record.jobLink = extractValue(json, "职位链接");
        record.jobId = extractValue(json, "job_id");
        record.skillTags = extractValue(json, "技能要求标签");
        record.recruitType = extractValue(json, "招聘类型");

        // 解析薪资
        record.parseSalary();

        return record;
    }

    /**
     * 从 JSON 中提取字段值
     */
    private static String extractValue(String json, String key) {
        String pattern = "\"" + key + "\"\\s*:\\s*\"([^\"]*)\"";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    /**
     * 解析薪资字符串
     * 格式示例: "15-25K·14薪" -> minSalary=15000, maxSalary=25000, months=14
     */
    private void parseSalary() {
        if (salary == null || salary.isEmpty() || salary.contains("面议")) {
            return;
        }

        Matcher salaryMatcher = SALARY_PATTERN.matcher(salary);
        if (salaryMatcher.find()) {
            this.minSalary = Integer.parseInt(salaryMatcher.group(1)) * 1000;
            this.maxSalary = Integer.parseInt(salaryMatcher.group(2)) * 1000;
        }

        Matcher monthsMatcher = MONTHS_PATTERN.matcher(salary);
        if (monthsMatcher.find()) {
            this.salaryMonths = Integer.parseInt(monthsMatcher.group(1));
        } else {
            this.salaryMonths = 12; // 默认12薪
        }
    }

    /**
     * 获取有效城市（如果城市为空，尝试从岗位名称或职位描述中提取）
     */
    public String getEffectiveCity() {
        if (city != null && !city.isEmpty()) {
            return city;
        }

        // 尝试从岗位名称中提取城市（格式如: "Java工程师（合肥）"）
        Pattern cityPattern = Pattern.compile("[（(]([\\u4e00-\\u9fa5]{2,4})[）)]");
        Matcher m = cityPattern.matcher(jobName != null ? jobName : "");
        if (m.find()) {
            String extracted = m.group(1);
            // 验证是否是有效城市名（简单判断）
            if (isValidCity(extracted)) {
                return extracted;
            }
        }

        return "未知";
    }

    /**
     * 简单验证是否是有效城市名
     */
    private boolean isValidCity(String name) {
        // 常见城市列表
        String[] cities = { "北京", "上海", "广州", "深圳", "杭州", "南京", "武汉",
                "成都", "西安", "重庆", "苏州", "天津", "合肥", "郑州",
                "长沙", "青岛", "大连", "厦门", "珠海", "东莞", "佛山" };
        for (String city : cities) {
            if (city.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 计算平均月薪
     */
    public Integer getAvgMonthlySalary() {
        if (minSalary != null && maxSalary != null) {
            return (minSalary + maxSalary) / 2;
        }
        return null;
    }

    /**
     * 计算年薪（基于月薪和月数）
     */
    public Integer getAnnualSalary() {
        Integer avgMonthly = getAvgMonthlySalary();
        if (avgMonthly != null && salaryMonths != null) {
            return avgMonthly * salaryMonths;
        }
        return null;
    }

    /**
     * 获取技能标签数组
     */
    public String[] getSkillArray() {
        if (skillTags == null || skillTags.isEmpty()) {
            return new String[0];
        }
        return skillTags.split(",");
    }

    /**
     * 是否是实习岗位
     */
    public boolean isInternship() {
        if ("实习".equals(recruitType)) {
            return true;
        }
        if (jobName != null && jobName.contains("实习")) {
            return true;
        }
        return false;
    }

    // Getters
    public String getJobName() {
        return jobName;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getSalary() {
        return salary;
    }

    public String getEducation() {
        return education;
    }

    public String getCity() {
        return city;
    }

    public String getJobType() {
        return jobType;
    }

    public String getRecruitCount() {
        return recruitCount;
    }

    public String getCompanyType() {
        return companyType;
    }

    public String getCompanyNature() {
        return companyNature;
    }

    public String getGraduateYear() {
        return graduateYear;
    }

    public String getWorkDaysPerWeek() {
        return workDaysPerWeek;
    }

    public String getInternDuration() {
        return internDuration;
    }

    public String getHasConversion() {
        return hasConversion;
    }

    public String getJobDesc() {
        return jobDesc;
    }

    public String getJobLink() {
        return jobLink;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSkillTags() {
        return skillTags;
    }

    public int getCareerJob() {
        return careerJob;
    }

    public String getRecruitType() {
        return recruitType;
    }

    public Integer getMinSalary() {
        return minSalary;
    }

    public Integer getMaxSalary() {
        return maxSalary;
    }

    public Integer getSalaryMonths() {
        return salaryMonths;
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobName='" + jobName + '\'' +
                ", city='" + getEffectiveCity() + '\'' +
                ", salary='" + salary + '\'' +
                ", minSalary=" + minSalary +
                ", maxSalary=" + maxSalary +
                '}';
    }
}
