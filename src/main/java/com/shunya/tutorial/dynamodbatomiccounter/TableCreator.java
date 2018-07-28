package com.shunya.tutorial.dynamodbatomiccounter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.List;

public class TableCreator {

    final AmazonDynamoDBClient amazonDynamoDBClient;

    final DynamoDB dynamoDB;

    public TableCreator(AmazonDynamoDBClient amazonDynamoDBClient, DynamoDB dynamoDB) {
        this.amazonDynamoDBClient = amazonDynamoDBClient;
        this.dynamoDB = dynamoDB;
    }

    public void createTable() throws InterruptedException {
        List<KeySchemaElement> elements = new ArrayList<>();
        KeySchemaElement keySchemaElement = new KeySchemaElement()
                .withKeyType(KeyType.HASH)
                .withAttributeName("id");
        elements.add(keySchemaElement);

        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("id")
                .withAttributeType(ScalarAttributeType.S));

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("local-content")
                .withKeySchema(elements)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(1L))
                .withAttributeDefinitions(attributeDefinitions);

        Table dynamoDBTable = dynamoDB.createTable(createTableRequest);
        dynamoDBTable.waitForAllActiveOrDelete();
    }

    public void deleteTable() {
        deleteTable("local-content");
    }

    private void deleteTable(String tableName) {
        try {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
            deleteTableRequest.setTableName(tableName);
            dynamoDB.getTable(tableName).delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
