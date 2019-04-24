package com.shunya.tutorial.dynamodbatomiccounter.atomic;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class ContentDao {

    @Autowired
    private AmazonDynamoDBClient dynamoDBClient;

    @Autowired
    private DynamoDB dynamoDB;

    public Content create(Content dto)  {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        mapper.save(dto);
        return dto;
    }

    /**
     * This method atomically updates number of views without interfering with other update operations, use it with caution because method is not idempotent in nature.
     * @param contentId ID of the content
     * @param delta the number of views to increment
     */
    public void incrementViews(String contentId, long delta) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(contentId));

        UpdateItemRequest updateRequest =
                new UpdateItemRequest().withTableName("local-content")
                        .withKey(key)
                        .addAttributeUpdatesEntry("views", new AttributeValueUpdate()
                            .withValue(new AttributeValue().withN("" + delta))
                            .withAction(AttributeAction.ADD));
        dynamoDBClient.updateItem(updateRequest);
    }

    public void incrementViewsUsingExpression(String contentId, long delta) {
        Table table = dynamoDB.getTable("local-content");
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("id", contentId)
                .withUpdateExpression("SET #fn = #fn + :delta")
                .withNameMap(new NameMap()
                        .with("#fn", "views")
                )
                .withValueMap(new ValueMap()
                        .withNumber(":delta", delta)
                )
                .withReturnValues(ReturnValue.ALL_NEW);

        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println(outcome.getItem().toJSONPretty());
    }

    public void incrementViewsUsingConditionalExpression(String contentId, long newValue, long expected) {
        Table table = dynamoDB.getTable("local-content");
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("id", contentId)
                .withUpdateExpression("SET #fn = :newval")
                .withConditionExpression("#fn = :currval")
                .withNameMap(new NameMap()
                        .with("#fn", "views")
                )
                .withValueMap(new ValueMap()
                        .withNumber(":newval", newValue)
                        .withNumber(":currval", expected)
                )
                .withReturnValues(ReturnValue.ALL_NEW);

        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println(outcome.getItem().toJSONPretty());
    }

}
