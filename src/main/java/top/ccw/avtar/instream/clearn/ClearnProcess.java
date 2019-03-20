package top.ccw.avtar.instream.clearn;

import com.google.common.base.CharMatcher;
import top.ccw.avtar.db.dao.stream.JobDataTwoDao;
import top.ccw.avtar.entity.JobDataOne;
import top.ccw.avtar.entity.JobDataTwo;
import top.ccw.avtar.instream.ElasticsearchUtils;
import top.ccw.avtar.utils.DateHelper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClearnProcess {

    public static void start(JobDataOne j1) {
        JobDataTwo j2 = new JobDataTwo();
        process(j1, j2);

        //add j2 to database
        JobDataTwoDao.getInstance().insert(j2);

        //add j2 to elasticsearch
        ElasticsearchUtils.saveTo(j2);

    }

    private static void process(JobDataOne j1, JobDataTwo j2) {
        init(j1, j2);

        firstModifyJobSite(j1, j2);
        secondModifySalary(j1, j2);
        thirdModifyRelaseDate(j1,j2);
        fourthModifyJobInfo(j1, j2);
        fiveModifyCompanyPeopleNum(j1, j2);
        sixModifyCompanyTypeFormat(j1, j2);
        sevenModifyCompanyBusiness(j1, j2);
        eightModifyJobRequire(j1, j2);
    }

    private static void init(JobDataOne j1, JobDataTwo j2) {
        j2.setDirection(j1.getDirection());//设置方向
        j2.setJobName(j1.getJobName());//职位名
        j2.setCompanyName(j1.getCompanyName());//公司名
        j2.setCompanyWelfare(j1.getCompanyWelfare());//公司福利
    }

    //清洗程序1
    private static void firstModifyJobSite(JobDataOne j1, JobDataTwo j2) {

        if(j1.getJobSite() == null || j1.getJobSite().equals("") || j1.getJobSite().equals("None")) {
            j2.setJobSite("");
            j2.setJobSiteProvinces("");
            return ;
        }

        /**第一、
         * 1、如果j1.getJobSite()数据中中有包含‘-’ 这个字符，split("-")后，j2.setJobSite取[0]下标数据
         * 2、如果j1.getJobSite()没有包含‘-’ 这个字符，那j2.setJobSite直接取该值
         */
        if(j1.getJobSite().contains("-")) {
            String[] splits = j1.getJobSite().split("-");
            j2.setJobSite(splits[0]);
        } else {
            //如果j1.getJobSite() 包含异地 那j2.setJobSite()值为“异地招聘”;
            //如果不是“异地招聘字段”,那么如果长度大于2则取前2位，否则直接填值
            if(j1.getJobSite().contains("异地")) {
                j2.setJobSite("异地招聘");
            } else {
                j2.setJobSite(j1.getJobSite());
            }
        }

        /***
         * 第二、
         * A.如果当前数据是 异地招聘 那么做 异地招聘 处理
         * B.如果当前不是异地招聘，那么
         * 	 B-0.先对当前地区去tb_provinces 表中查询，如果查询不到 执行 B-1
         *   B-1.先对当前地区到tb_cities(市表)表中查询，如果查询又返回数据那么直接根据查到的 provinceid 拿到省。并去掉省字。 存入到 tb_job_info_new 的 job_site_provinces 字段中
         *	 B-2.如果当前地区在市数据库中查询不到，那么就去 tb_areas(县表) 表中查询。如果有结果则返回cityid,然后根据cityid查到provinceid 拿到省；如果查不到做异地招聘处理
         */
        String site = j2.getJobSite();
        String province = "";

        province = SiteTransferTools.getInstance().
                selectProvinceFromProvince(site);

        //如果在 tb_provinces 中查不到
        if(province == null || province.length() == 0) {
            province = goOtherDeal(site);
        }


        //插入province结果到 j2. 插入之前进行省格式转换
        j2.setJobSiteProvinces(modifyProvinceFormat(province));
    }

    private static String goOtherDeal(String site) {
        String province = "";
        //如果当前城市是异地招聘 那么直接填充 job_site_provinces字段 为异地
        if(site.contains("异地招聘")) {
            province = "异地";
        } else {
            //1.去tb_cities 查询
            province = SiteTransferTools.getInstance().
                    selectProvinceFromCity(site);

            //2.如果tb_cities查询不到，则去tb_areas查询city 然后根据拿到的 city字段去tb_cities表中查询
            if(province == null || province.length() == 0 ) {//如果返回为空 或者 长度为0
                String city = SiteTransferTools.getInstance().
                        selectCityFromAreas(site);

                //如果查到的为空那么做异地处理 如果不为空返回市
                if (city==null || city.length() == 0) {
                    province = "异地";
                } else {
                    province = SiteTransferTools.getInstance().
                            selectProvinceFromCity(city);
                }
            }
        }

        return province;
    }

    private final static String [] OhterProvinceFormat = {"内蒙古","黑龙江"};

    private static String modifyProvinceFormat(String province) {

        if(province!=null && !province.equals("")) {
            for(int i=0; i<OhterProvinceFormat.length; i++) {
                if(province.contains(OhterProvinceFormat[i])) {
                    return OhterProvinceFormat[i];
                }
            }

            return province.substring(0, 2);
        }

        return "";

    }

    //清洗程序2
    private static void secondModifySalary(JobDataOne j1, JobDataTwo j2) {

        /**
         * 考虑3中情况:
         * 1 => 为空。那么两个字段也为空
         * 2 => 固定如(9千/月)。那么两个字段也为9
         * 3 => 正常操作
         * @param lists
         * @return
         */
        String j1Salary = modifySalaryFormat(j1.getJobSalary());

        String salary_min = "";
        String salary_max = "";

        if(j1Salary!=null && !j1Salary.equals("")) {
            if(j1Salary.contains("-")){//第三种情况
                String[] split1 = j1Salary.split("千");

                String[] split2 = split1[0].split("-");
                salary_min = split2[0];
                salary_max = split2[1];
            } else {//第二种情况
                String[] split = j1Salary.split("千");
                salary_min = split[0];
                salary_max = split[0];
            }
        }

        //设置j2 salary
        j2.setJobSalaryMin(salary_min);
        j2.setJobSalaryMax(salary_max);

    }

    private static String modifySalaryFormat(String j1Salary) {

        if(j1Salary==null || j1Salary.equals("") || j1Salary.equals("None")) {
            return "";
        }

        //1.0 add
        if (j1Salary.contains("千/月")) {
            double firstnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[0]);
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[1]);
            j1Salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
        }
        if (j1Salary.contains("万/月")) {
            double firstnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[0])
                    * 10;
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[1])
                    * 10;
            j1Salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
        }
        if (j1Salary.contains("万/年")) {
            double firstnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[0])
                    / 1.2;
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3).split("-")[1])
                    / 1.2;
            j1Salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
        }
        if (j1Salary.contains("元/天")) {
            double firstnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 3)) * 0.03;
            j1Salary = String.format("%.0f", firstnum) + "千/月";
        }

        //2.0 add
        if (j1Salary.contains("万以上/月")) {
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 5)) * 10;
            j1Salary = String.format("%.0f", secondnum);
        }
        if (j1Salary.contains("万以下/年") || j1Salary.contains("万以上/年")) {
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 5)) * 5 /
                    6.0;
            j1Salary = String.format("%.0f", secondnum);
        }
        if (j1Salary.contains("元/小时")) {
            double secondnum = Double.valueOf(j1Salary.substring(0, j1Salary.length() - 4)) * 0.72;
            j1Salary = String.format("%.0f", secondnum);
        }

        //3.0 add

        return j1Salary;

    }

    //清洗程序3
    private static void thirdModifyRelaseDate(JobDataOne j1,JobDataTwo j2) {

        if(j1.getRelaseDate()!=null && !j1.getRelaseDate().equals("") || !j1.getJobSite().equals("None")) {
            String relaseDate = DateHelper.getYear() + "-" +j1.getRelaseDate();
            j2.setRelaseDate(relaseDate);
        } else {
            j2.setRelaseDate("");
        }

    }



    private static String [] EduLevel = {"高中","中专","大专","本科","研究生","硕士","博士"};
    /***
     * 判断是否包含 EduLevel中的学历
     * @param str
     * @return
     */
    private static boolean dealContainEduLevel(String str,JobDataTwo j2) {

        if(str!=null && !str.equals("")) {
            for( int i=0; i<EduLevel.length; i++ ) {
                if(str.contains(EduLevel[i])) {
                    j2.setEducationLevel(EduLevel[i]);
                    return true;
                }
            }
        }

        j2.setEducationLevel("");
        return false;
    }

    //清洗程序4
    private static void fourthModifyJobInfo(JobDataOne j1, JobDataTwo j2) {
        String jobInfo = j1.getJobInfo1();// 需要提取学历、工作经验的数据

        if (jobInfo!=null && !jobInfo.equals("") && !jobInfo.equals("None")) {

            //处理学历
            dealContainEduLevel(jobInfo, j2);

            //处理工作经验
            if(jobInfo.contains("经验")) {
                String [] splitStr = jobInfo.trim().split("\\,");
                for(int i=0; i<splitStr.length; i++) {
                    if(splitStr[i].contains("经验")) {
                        j2.setWorkExper(splitStr[i]);
                        break;
                    }
                }
            } else {
                j2.setWorkExper("");
            }

        } else {
            j2.setEducationLevel("");
            j2.setWorkExper("");
        }

    }

    //清洗程序5
    private static void fiveModifyCompanyPeopleNum(JobDataOne j1, JobDataTwo j2) {
        String str = j1.getCompanyPeopleNum();

        String personAve = "";

        /***
         * 第一
         * 	先判断是否包含 ‘-’ 字符，如果包含=> 先以‘人’ split 后
         * 							   => 再以‘-‘ split后 取 [0] 和[1]
         *
         * 第二、
         * 	如果不以’-‘情况, 直接取数字
         *
         */
        if(str!=null && !str.equals("") && !str.equals("None")) {

            if(str.contains("-")) {
                String[] split = str.split("人");

                String[] split2 = split[0].split("-");

                int parseInt = Integer.parseInt(split2[0]);
                int parseInt2 = Integer.parseInt(split2[1]);

                int ave = (parseInt+parseInt2)/2;

                personAve = new String(new Integer(ave).toString());

            } else {
                String regEx="[^0-9]";
                Pattern p = Pattern.compile(regEx);
                Matcher m = p.matcher(str);

                personAve = m.replaceAll("").trim();

            }

        }

        j2.setCompanyPeopleNum(personAve);
    }

    //清洗程序6
    private static void sixModifyCompanyTypeFormat(JobDataOne j1, JobDataTwo j2) {
        if(j1.getCompanyType() == null || j1.getCompanyType().equals("")
                || j1.getCompanyType().equals("None")) {
            j2.setCompanyType("");
        } else {
            j2.setCompanyType(j1.getCompanyType());
        }
    }

    //清洗程序7
    private static void sevenModifyCompanyBusiness(JobDataOne j1, JobDataTwo j2) {
        if(j1.getCompanyBusiness()==null || j1.getCompanyBusiness().equals("")
                || j1.getCompanyBusiness().equals("None")) {
            j2.setCompanyBusiness("");
        } else {
            j2.setCompanyBusiness(j1.getCompanyBusiness());
        }
    }

    //清洗程序8
    private static void eightModifyJobRequire(JobDataOne j1, JobDataTwo j2) {
        if(j1.getJobInfo2() == null || j1.getJobInfo2().equals("")
                || j1.getJobInfo2().equals("None")) {
            j2.setJobRequire("");
        } else {
            j2.setJobRequire(j1.getJobInfo2());
        }
    }

}
