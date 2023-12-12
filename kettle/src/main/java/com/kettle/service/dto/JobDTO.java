package com.kettle.service.dto;

public class JobDTO {

    private String jobFilePath;

    public String getJobFilePath() {
        return jobFilePath;
    }

    public void setJobFilePath(String jobFilePath) {
        this.jobFilePath = jobFilePath;
    }

    @Override
    public String toString() {
        return "JobDTO{" +
                "jobFilePath='" + jobFilePath + '\'' +
                '}';
    }
}
