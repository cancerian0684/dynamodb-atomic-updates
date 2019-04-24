package com.shunya.tutorial.dynamodbatomiccounter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.shunya.tutorial.dynamodbatomiccounter.atomic.Content;
import com.shunya.tutorial.dynamodbatomiccounter.atomic.ContentDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class StartupBean implements CommandLineRunner {

    @Autowired
    private ContentDao contentDao;

    @Autowired
    private AmazonDynamoDBClient amazonDynamoDBClient;

    @Autowired
    private DynamoDB dynamoDB;

    @Override
    public void run(String... args) {
        createTable();
        addRecord();
    }

    private void addRecord() {
        try {
            Content dto = new Content();
            dto.setId(UUID.randomUUID().toString());
            dto.setTitle("this is first content");
            dto.setDescription("some dummy description");
            dto.setViews(0);
            Content content = contentDao.create(dto);
            contentDao.incrementViews(content.getId(), 100);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable() {
        try {
            new TableCreator(amazonDynamoDBClient, dynamoDB).createTable();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
