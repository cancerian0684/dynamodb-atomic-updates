package com.shunya.tutorial.dynamodbatomiccounter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
public class DynamoConfig {
    private static final Logger logger = LoggerFactory.getLogger(DynamoConfig.class);

    @Bean
    AmazonDynamoDBClient dbClient(@Value("${aws.dynamodb.endpoint}") String amazonDynamoDBEndpoint, AWSSettings awsSettings) {
        AmazonDynamoDBClient dbClient = new AmazonDynamoDBClient(
                new BasicAWSCredentials(awsSettings.getAccessKey(), awsSettings.getSecretKey()));
        dbClient.setRegion(Region.getRegion(Regions.fromName(awsSettings.getRegion())));
        logger.info("DB Endpoint is " + amazonDynamoDBEndpoint);
        if (!StringUtils.isEmpty(amazonDynamoDBEndpoint)) {
            dbClient.setEndpoint(amazonDynamoDBEndpoint);
        }
        if (awsSettings.getRegion().equalsIgnoreCase("local")) {
            dbClient.setEndpoint("http://localhost:8000");
        }
        return dbClient;
    }

    @Bean
    DynamoDB dynamoDB(AmazonDynamoDBClient client) {
        return new DynamoDB(client);
    }
}
