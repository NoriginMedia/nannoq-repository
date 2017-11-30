package com.nannoq.tools.repository.dynamodb.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nannoq.tools.repository.models.*;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.nannoq.tools.repository.dynamodb.model.TestModelConverter.fromJson;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@DynamoDBTable(tableName="testModels")
@DataObject(generateConverter = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TestModel implements DynamoDBModel, Model, ETagable, Cacheable {
    private String etag;
    private String someStringOne;
    private String someStringTwo;
    private String someStringThree;
    private String someStringFour;
    private Date someDate;
    private Date someDateTwo;
    private Long someLong;
    private Long someLongTwo;
    private Integer someInteger;
    private Integer someIntegerTwo;
    private Boolean someBoolean;
    private Boolean someBooleanTwo;
    private List<TestDocument> documents;
    private Date createdAt;
    private Date updatedAt;
    private Long version;

    public TestModel() {

    }

    public TestModel(JsonObject jsonObject) {
        fromJson(jsonObject, this);

        someDate = jsonObject.getLong("someDate") == null ? null : new Date(jsonObject.getLong("someDate"));
        someDateTwo = jsonObject.getLong("someDateTwo") == null ? null : new Date(jsonObject.getLong("someDateTwo"));
        createdAt = jsonObject.getLong("createdAt") == null ? null : new Date(jsonObject.getLong("createdAt"));
        updatedAt = jsonObject.getLong("updatedAt") == null ? null : new Date(jsonObject.getLong("updatedAt"));
    }

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public String getSomeStringOne() {
        return someStringOne;
    }

    @Fluent
    public TestModel setSomeStringOne(String someStringOne) {
        this.someStringOne = someStringOne;

        return this;
    }

    public String getSomeStringTwo() {
        return someStringTwo;
    }

    @Fluent
    public TestModel setSomeStringTwo(String someStringTwo) {
        this.someStringTwo = someStringTwo;

        return this;
    }

    public String getSomeStringThree() {
        return someStringThree;
    }

    @Fluent
    public TestModel setSomeStringThree(String someStringThree) {
        this.someStringThree = someStringThree;

        return this;
    }

    public String getSomeStringFour() {
        return someStringFour;
    }

    @Fluent
    public TestModel setSomeStringFour(String someStringFour) {
        this.someStringFour = someStringFour;

        return this;
    }

    public Date getSomeDate() {
        return someDate;
    }

    @Fluent
    public TestModel setSomeDate(Date someDate) {
        this.someDate = someDate;

        return this;
    }

    public Date getSomeDateTwo() {
        return someDateTwo;
    }

    @Fluent
    public TestModel setSomeDateTwo(Date someDateTwo) {
        this.someDateTwo = someDateTwo;

        return this;
    }

    public Long getSomeLong() {
        return someLong;
    }

    @Fluent
    public TestModel setSomeLong(Long someLong) {
        this.someLong = someLong;

        return this;
    }

    public Long getSomeLongTwo() {
        return someLongTwo;
    }

    @Fluent
    public TestModel setSomeLongTwo(Long someLongTwo) {
        this.someLongTwo = someLongTwo;

        return this;
    }

    public Integer getSomeInteger() {
        return someInteger;
    }

    @Fluent
    public TestModel setSomeInteger(Integer someInteger) {
        this.someInteger = someInteger;

        return this;
    }

    public Integer getSomeIntegerTwo() {
        return someIntegerTwo;
    }

    @Fluent
    public TestModel setSomeIntegerTwo(Integer someIntegerTwo) {
        this.someIntegerTwo = someIntegerTwo;

        return this;
    }

    public Boolean getSomeBoolean() {
        return someBoolean;
    }

    @Fluent
    public TestModel setSomeBoolean(Boolean someBoolean) {
        this.someBoolean = someBoolean;

        return this;
    }

    public Boolean getSomeBooleanTwo() {
        return someBooleanTwo;
    }

    @Fluent
    public TestModel setSomeBooleanTwo(Boolean someBooleanTwo) {
        this.someBooleanTwo = someBooleanTwo;

        return this;
    }

    public List<TestDocument> getDocuments() {
        return documents;
    }

    @Fluent
    public TestModel setDocuments(List<TestDocument> documents) {
        this.documents = documents;

        return this;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    @Fluent
    public TestModel setVersion(Long version) {
        this.version = version;

        return this;
    }

    @Override
    public String getHash() {
        return someStringOne;
    }

    @Override
    public String getRange() {
        return someStringTwo;
    }

    @Override
    @Fluent
    public DynamoDBModel setHash(String hash) {
        someStringTwo = hash;

        return this;
    }

    @Override
    @Fluent
    public DynamoDBModel setRange(String range) {
        someStringTwo = range;

        return this;
    }

    @Override
    public String getEtag() {
        return etag;
    }

    @Fluent
    @Override
    public TestModel setEtag(String etag) {
        this.etag = etag;

        return this;
    }

    @Override
    public String generateEtagKeyIdentifier() {
        return getSomeStringOne() != null ? "data_api_testModel_etag_" + getSomeStringOne() : "NoTestModelEtag";
    }

    @Override
    public Model setModifiables(Model newObject) {
        return this;
    }

    @Override
    public Model sanitize() {
        return this;
    }

    @Override
    public List<ValidationError> validateCreate() {
        return Collections.emptyList();
    }

    @Override
    public List<ValidationError> validateUpdate() {
        return Collections.emptyList();
    }

    @Override
    public Date getCreatedAt() {
        return createdAt != null ? createdAt : new Date();
    }

    @Override
    public Model setCreatedAt(Date date) {
        createdAt = date;

        return this;
    }

    @Override
    public Date getUpdatedAt() {
        return updatedAt != null ? updatedAt : new Date();
    }

    @Override
    public Model setUpdatedAt(Date date) {
        updatedAt = date;

        return this;
    }

    @Override
    public Model setInitialValues(Model record) {
        return this;
    }

    @Override
    public JsonObject toJsonFormat(@Nonnull String[] projections) {
        return new JsonObject(Json.encode(this));
    }
}
