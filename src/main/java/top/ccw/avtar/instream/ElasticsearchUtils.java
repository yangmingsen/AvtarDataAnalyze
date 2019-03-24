package top.ccw.avtar.instream;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import top.ccw.avtar.entity.JobDataTwo;

import java.net.InetAddress;

public class ElasticsearchUtils {

    private final static String HOST = "localhost";
    private final static int PORT = 9300;
    private static Client client = null;
    private final static String INDEX = "job_data_current";
    private final static String TYPE = "jdbc";

    static {
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static JSONObject parseToJSONObj(JobDataTwo j2) {
        JSONObject json = new JSONObject();
        json.put("id", j2.getId());
        json.put("job_name", j2.getJobName());
        json.put("direction", j2.getDirection());
        json.put("company_name", j2.getCompanyName());
        json.put("job_site_provinces", j2.getJobSiteProvinces());
        json.put("job_site", j2.getJobSite());
        json.put("job_salary_min", j2.getJobSalaryMin());
        json.put("job_salary_max", j2.getJobSalaryMax());
        json.put("relase_date", j2.getRelaseDate());
        json.put("education_level", j2.getEducationLevel());
        json.put("work_exper", j2.getWorkExper());
        json.put("company_welfare", j2.getCompanyWelfare());
        json.put("job_require", j2.getJobRequire());
        json.put("company_type", j2.getCompanyType());
        json.put("company_people_num", j2.getCompanyPeopleNum());
        json.put("company_business", j2.getCompanyBusiness());

        return json;
    }

    public static void saveTo(JobDataTwo j2) {
        JSONObject jsonObj = parseToJSONObj(j2);

        IndexResponse response = client.prepareIndex(INDEX, TYPE)
                .setSource(jsonObj)
                .get();

    }

}