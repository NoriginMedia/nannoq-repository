package com.nannoq.tools.repository.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
/**
 * This class defines an interface for models that operate on etags.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface ETagable {
    String getEtag();
    Map<String, String> generateAndSetEtag(Map<String, String> map);
    Map<String, String> reGenerateParent(Map<String, String> map);
    String generateEtagKeyIdentifier();
}
