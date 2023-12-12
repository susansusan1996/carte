package com.kettle.rest;

import com.kettle.service.dto.JobDTO;
import com.kettle.util.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

@RestController
@RequestMapping("")
public class KettleResource {

    private final Logger log = LoggerFactory.getLogger(KettleResource.class);

    /**
     * 啟動JOB
     */
    @PostMapping("/kettle/executeJob/")
    public void executeJob(@RequestBody JobDTO jobdto) {
        executeRestApi(jobdto);
    }


    //參考官方文件
    //https://help.hitachivantara.com/Documentation/Pentaho/Data_Integration_and_Analytics/9.1/Developer_center/REST_API_Reference/Carte/030#POSTexecuteJob
    public void executeRestApi(JobDTO jobdto) {
        String jobFilePath = jobdto.getJobFilePath();
//        String apiUrl = "http://localhost:8080/kettle/executeJob/?job=/pentaho/pdi-ce-9.4.0.0-343/data-integration/job/job_1.kjb";
        String baseUrl = "http://localhost:8080/kettle/executeJob/";
        String apiUrl = baseUrl + "?job=" + jobFilePath;
        String user = "cluster";
        String password = "cluster";
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(user, password);
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getInterceptors().add(new BasicAuthenticationInterceptor(user, password));
        ResponseEntity<String> response = restTemplate.getForEntity(apiUrl, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            String responseBody = response.getBody();
            log.info("呼叫CARTE API 成功，responseBody為:{}: ", responseBody);
            WebResult webResult = getXmlData(responseBody);
            String result = webResult.getResult();
            String message = webResult.getMessage();
            String id = webResult.getId();
            log.info("呼叫CARTE API 成功，Result: {}", result);
            log.info("呼叫CARTE API 成功，Message: {}", message);
            log.info("呼叫CARTE API 成功，Job ID: {}", id);
        } else {
            log.error("呼叫CARTE API 失敗，http code為: " + response.getStatusCodeValue());
        }
    }

    public WebResult getXmlData(String responseXML) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(WebResult.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return (WebResult) unmarshaller.unmarshal(new StringReader(responseXML));
        } catch (JAXBException e) {
            log.error("解析xml失敗:{}", e);
        }
        return null;
    }
}
