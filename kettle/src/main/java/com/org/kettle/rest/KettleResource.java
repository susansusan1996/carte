package com.org.kettle.rest;

import com.kettle.service.dto.JobDTO;
import com.kettle.util.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
//import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;

@RestController
@RequestMapping("/api")
public class KettleResource {

    private final Logger log = LoggerFactory.getLogger(KettleResource.class);


    /**
     * 測試api
     */
    @GetMapping("/kettle/hello/")
    public String hello() {
        return "hello!";
    }

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
//        String jobFilePath = jobdto.getJobFilePath();
////        String apiUrl = "http://localhost:8080/kettle/executeJob/?job=/pentaho/pdi-ce-9.4.0.0-343/data-integration/job/job_1.kjb";
//        String baseUrl = "http://localhost:8080/kettle/executeJob/";
//        String apiUrl = baseUrl + "?job=" + jobFilePath;
//        String user = "cluster";
//        String password = "cluster";
//        HttpHeaders headers = new HttpHeaders();
//        headers.setBasicAuth(user, password);
//        RestTemplate restTemplate = new RestTemplate();
//        restTemplate.getInterceptors().add(new BasicAuthenticationInterceptor(user, password));
//        ResponseEntity<String> response = restTemplate.getForEntity(apiUrl, String.class);
//        if (response.getStatusCode().is2xxSuccessful()) {
//            String responseBody = response.getBody();
//            log.info("呼叫CARTE API 成功，responseBody為:{}: ", responseBody);
//            WebResult webResult = getXmlData(responseBody);
//            String result = webResult.getResult();
//            String message = webResult.getMessage();
//            String id = webResult.getId();
//            log.info("呼叫CARTE API 成功，Result: {}", result);
//            log.info("呼叫CARTE API 成功，Message: {}", message);
//            log.info("呼叫CARTE API 成功，Job ID: {}", id);
//        } else {
//            log.error("呼叫CARTE API 失敗，http code為: " + response.getStatusCodeValue());
//        }
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


    public static void main(String[] args) {
        String baseUrl = "http://localhost:8888/api/kettle/test";
        String jwtToken = "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOiIxIiwiaWF0IjoxNzAzODQxNDY5fQ.a6ro9xuzWZDx4cOmK85PDagEpJ3pORoIrzNS4Sx-rfY";

        // Create headers with the JWT token
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + jwtToken);

        // Create an HttpEntity with the headers
        HttpEntity<String> entity = new HttpEntity<>(headers);

        // Create a RestTemplate
        RestTemplate restTemplate = new RestTemplate();

        // Perform a POST request
        ResponseEntity<String> response = restTemplate.exchange(baseUrl, HttpMethod.POST, entity, String.class);

        // Print the response
        System.out.println("Response: " + response.getBody());
    }

}
