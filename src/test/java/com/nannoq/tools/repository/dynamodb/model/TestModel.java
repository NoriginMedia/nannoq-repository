package com.nannoq.tools.repository.dynamodb.model;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nannoq.tools.repository.models.*;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
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

    @DynamoDBHashKey
    public String getSomeStringOne() {
        return someStringOne;
    }

    @Fluent
    public TestModel setSomeStringOne(String someStringOne) {
        this.someStringOne = someStringOne;

        return this;
    }

    @DynamoDBRangeKey
    public String getSomeStringTwo() {
        return someStringTwo;
    }

    @Fluent
    public TestModel setSomeStringTwo(String someStringTwo) {
        this.someStringTwo = someStringTwo;

        return this;
    }

    @DynamoDBIndexHashKey(globalSecondaryIndexName = "TEST_GSI")
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

    @DynamoDBIndexRangeKey(localSecondaryIndexName = PAGINATION_INDEX)
    public Date getSomeDate() {
        return someDate;
    }

    @Fluent
    public TestModel setSomeDate(Date someDate) {
        this.someDate = someDate;

        return this;
    }

    @DynamoDBIndexRangeKey(globalSecondaryIndexName = "TEST_GSI")
    public Date getSomeDateTwo() {
        return someDateTwo;
    }

    @Fluent
    public TestModel setSomeDateTwo(Date someDateTwo) {
        this.someDateTwo = someDateTwo;

        return this;
    }

    public Long getSomeLong() {
        return someLong != null ? someLong : 0L;
    }

    @Fluent
    public TestModel setSomeLong(Long someLong) {
        this.someLong = someLong;

        return this;
    }

    public Long getSomeLongTwo() {
        return someLongTwo != null ? someLongTwo : 0L;
    }

    @Fluent
    public TestModel setSomeLongTwo(Long someLongTwo) {
        this.someLongTwo = someLongTwo;

        return this;
    }

    public Integer getSomeInteger() {
        return someInteger != null ? someInteger : 0;
    }

    @Fluent
    public TestModel setSomeInteger(Integer someInteger) {
        this.someInteger = someInteger;

        return this;
    }

    public Integer getSomeIntegerTwo() {
        return someIntegerTwo != null ? someIntegerTwo : 0;
    }

    @Fluent
    public TestModel setSomeIntegerTwo(Integer someIntegerTwo) {
        this.someIntegerTwo = someIntegerTwo;

        return this;
    }

    public Boolean getSomeBoolean() {
        return someBoolean != null ? someBoolean : Boolean.FALSE;
    }

    @Fluent
    public TestModel setSomeBoolean(Boolean someBoolean) {
        this.someBoolean = someBoolean;

        return this;
    }

    public Boolean getSomeBooleanTwo() {
        return someBooleanTwo != null ? someBooleanTwo : Boolean.FALSE;
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
        someStringOne = hash;

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
        return getSomeStringOne() != null && getSomeStringTwo() != null ?
                "data_api_testModel_etag_" + getSomeStringOne() + "_" + getSomeStringTwo() :
                "NoTestModelEtag";
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestModel testModel = (TestModel) o;

        return Objects.equals(getSomeStringOne(), testModel.getSomeStringOne()) &&
                Objects.equals(getSomeStringTwo(), testModel.getSomeStringTwo()) &&
                Objects.equals(getSomeStringThree(), testModel.getSomeStringThree()) &&
                Objects.equals(getSomeStringFour(), testModel.getSomeStringFour()) &&
                Objects.equals(getSomeDate(), testModel.getSomeDate()) &&
                Objects.equals(getSomeDateTwo(), testModel.getSomeDateTwo()) &&
                Objects.equals(getSomeLong(), testModel.getSomeLong()) &&
                Objects.equals(getSomeLongTwo(), testModel.getSomeLongTwo()) &&
                Objects.equals(getSomeInteger(), testModel.getSomeInteger()) &&
                Objects.equals(getSomeIntegerTwo(), testModel.getSomeIntegerTwo()) &&
                Objects.equals(getSomeBoolean(), testModel.getSomeBoolean()) &&
                Objects.equals(getSomeBooleanTwo(), testModel.getSomeBooleanTwo()) &&
                Objects.equals(getDocuments(), testModel.getDocuments()) &&
                Objects.equals(getCreatedAt(), testModel.getCreatedAt()) &&
                Objects.equals(getUpdatedAt(), testModel.getUpdatedAt()) &&
                Objects.equals(getVersion(), testModel.getVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(someStringOne, someStringTwo, someStringThree, someStringFour, someDate, someDateTwo,
                someLong, someLongTwo, someInteger, someIntegerTwo, someBoolean, someBooleanTwo, documents, createdAt,
                updatedAt, version);
    }
}
