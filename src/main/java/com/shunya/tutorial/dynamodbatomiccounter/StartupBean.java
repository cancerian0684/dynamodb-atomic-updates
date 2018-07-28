package com.shunya.tutorial.dynamodbatomiccounter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class StartupBean implements CommandLineRunner {

    @Autowired
    private ContentDao contentDao;

    @Override
    public void run(String... args) throws Exception {
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
}
