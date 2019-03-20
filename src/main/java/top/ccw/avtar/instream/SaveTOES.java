package top.ccw.avtar.instream;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import top.ccw.avtar.db.dao.stream.JobDataTwoDao;
import top.ccw.avtar.entity.JobDataTwo;

import java.net.InetAddress;
import java.util.List;

public class SaveTOES {

    private static String host = "localhost";
    private static int port = 9300;

    public static void main(String[] args) throws Exception {
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        List<JobDataTwo> jobs = JobDataTwoDao.getInstance().selects();

        for (JobDataTwo j2 : jobs) {
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


            Thread.sleep(5 * 1000);

            IndexResponse response = client.prepareIndex("metawiki", "jdbc")
                    .setSource(json)
                    .get();

            String _index = response.getIndex();
            System.out.println("_index = " + _index);
            // Type name
            String _type = response.getType();
            System.out.println("_type = " + _type);
            // Document ID (generated or not)
            String _id = response.getId();
            System.out.println("_id = " + _id);
            // Version (if it's the first time you index this document, you will get: 1)
            long _version = response.getVersion();
            System.out.println("_version = " + _version);
            // isCreated() is true if the document is a new one, false if it has been updated
            boolean created = response.isCreated();
            System.out.println("created = " + created);

        }

        System.out.println(client);

    }
}
