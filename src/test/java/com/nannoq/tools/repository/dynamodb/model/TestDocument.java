package com.nannoq.tools.repository.dynamodb.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nannoq.tools.repository.models.ETagable;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

import static com.nannoq.tools.repository.dynamodb.model.TestDocumentConverter.fromJson;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@DynamoDBDocument
@DataObject(generateConverter = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TestDocument implements ETagable {
    private String etag;
    private String someStringOne;
    private String someStringTwo;
    private String someStringThree;
    private String someStringFour;
    private Long version;

    public TestDocument() {

    }

    public TestDocument(JsonObject jsonObject) {
        fromJson(jsonObject, this);
    }

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public String getSomeStringOne() {
        return someStringOne;
    }

    public void setSomeStringOne(String someStringOne) {
        this.someStringOne = someStringOne;
    }

    public String getSomeStringTwo() {
        return someStringTwo;
    }

    public void setSomeStringTwo(String someStringTwo) {
        this.someStringTwo = someStringTwo;
    }

    public String getSomeStringThree() {
        return someStringThree;
    }

    public void setSomeStringThree(String someStringThree) {
        this.someStringThree = someStringThree;
    }

    public String getSomeStringFour() {
        return someStringFour;
    }

    public void setSomeStringFour(String someStringFour) {
        this.someStringFour = someStringFour;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @Override
    public String getEtag() {
        return etag;
    }

    @Override
    public TestDocument setEtag(String etag) {
        this.etag = etag;

        return this;
    }

    @Override
    public String generateEtagKeyIdentifier() {
        return getSomeStringOne() != null ? "data_api_testDocument_etag_" + getSomeStringOne() : "NoDocumentTag";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestDocument that = (TestDocument) o;

        return Objects.equals(getSomeStringOne(), that.getSomeStringOne()) &&
                Objects.equals(getSomeStringTwo(), that.getSomeStringTwo()) &&
                Objects.equals(getSomeStringThree(), that.getSomeStringThree()) &&
                Objects.equals(getSomeStringFour(), that.getSomeStringFour()) &&
                Objects.equals(getVersion(), that.getVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(someStringOne, someStringTwo, someStringThree, someStringFour, version);
    }
}
