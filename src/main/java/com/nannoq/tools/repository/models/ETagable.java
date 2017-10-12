package com.nannoq.tools.repository.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * File: ETagable
 * Project: gcm-backend
 * Package: com.noriginmedia.norigintube.model.utils
 * <p>
 * This class
 *
 * @author anders
 * @version 4/6/16
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface ETagable {
    String getEtag();
    Map<String, String> generateAndSetEtag(Map<String, String> map);
    Map<String, String> reGenerateParent(Map<String, String> map);
    String generateEtagKeyIdentifier();
}
