package com.shunya.tutorial.dynamodbatomiccounter.conditional;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
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
public class ProductDao {

    @Autowired
    private AmazonDynamoDBClient dynamoDBClient;

    @Autowired
    private DynamoDB dynamoDB;

    public Product create(Product dto) {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        mapper.save(dto);
        return dto;
    }

    public Product get(String productId) {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        Product dbItem = mapper.load(Product.class, productId);
        return dbItem;
    }

    public Product update(String productId, long newPrice) {
        Product product = get(productId);
        product.setPrice(newPrice);
        final DynamoDBMapperConfig.Builder builder = new DynamoDBMapperConfig.Builder();
        builder.setConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL);
        builder.setSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES);
        DynamoDBMapperConfig mapperConfig = builder.build();
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient, mapperConfig);
        mapper.save(product);
        return product;
    }

    public Product updateWithoutOptimisticLocking(String productId, long newPrice) {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        // Load a catalog item.
        Product item = mapper.load(Product.class, productId);
        item.setPrice(newPrice);
        // Save the item.
        mapper.save(item, new DynamoDBMapperConfig(DynamoDBMapperConfig.SaveBehavior.CLOBBER));
        return item;
    }

    /**
     * This method atomically updates number of views without interfering with other update operations, use it with caution because method is not idempotent in nature.
     *
     * @param productId     ID of the Product
     * @param newPrice      New price to be set for the product
     * @param existingPrice Existing price to be set for the product
     */
    public void incrementPrice(String productId, long newPrice, long existingPrice) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(productId));
        UpdateItemRequest updateRequest =
                new UpdateItemRequest().withTableName("local-content")
                        .withKey(key)
                        .addAttributeUpdatesEntry("price", new AttributeValueUpdate()
                                .withValue(new AttributeValue().withN("" + newPrice))
                                .withAction(AttributeAction.ADD))
                        .addExpectedEntry("price", new ExpectedAttributeValue(new AttributeValue().withN("" + existingPrice)));
        dynamoDBClient.updateItem(updateRequest);
    }


    public void incrementUsingConditionalExpression(String contentId, long newValue, long expected) {
        Table table = dynamoDB.getTable("local-content");
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("id", contentId)
                .withUpdateExpression("SET #fn = :newval")
                .withConditionExpression("#fn = :currval")
                .withNameMap(new NameMap()
                        .with("#fn", "price")
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
