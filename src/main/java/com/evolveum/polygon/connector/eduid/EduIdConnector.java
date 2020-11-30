/*
 * Copyright (c) 2019 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.eduid;

import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;

import com.evolveum.polygon.rest.AbstractRestConnector;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author oscar
 *
 */
@ConnectorClass(displayNameKey = "eduid.connector.display", configurationClass = EduIdConfiguration.class)
public class EduIdConnector extends AbstractRestConnector<EduIdConfiguration> implements PoolableConnector, TestOp, SchemaOp, CreateOp, DeleteOp, UpdateOp, SearchOp<EduIdFilter> {

    private static final Log LOG = Log.getLog(EduIdConnector.class);

    protected static final String SCHEMAS_VALUE = "urn:mace:switch.ch:eduid:scim:1.0:affiliation";

    protected static final String SCHEMAS = "schemas";
    protected static final String ID = "id";
    protected static final String EXTERNAL_ID = "externalId";
    protected static final String GIVEN_NAME = "givenName";
    protected static final String SURNAME = "surname";
    protected static final String SWISS_EDU_ID_AFFILIATION_STATUS = "swissEduIDAffiliationStatus";
    protected static final String SWISS_EDU_ID_AFFILIATION_PERIOD_BEGIN = "swissEduIDAffiliationPeriodBegin";
    protected static final String SWISS_EDU_PERSON_UNIQUE_ID = "swissEduPersonUniqueID";
    protected static final String SWISS_EDU_ID = "swissEduID";
    protected static final String EDU_PERSON_AFFILIATION = "eduPersonAffiliation";
    protected static final String EMAIL = "email";

    protected static final String[] SINGLE_STRING_ATTRIBUTES = { ID, EXTERNAL_ID, GIVEN_NAME, SURNAME, SWISS_EDU_ID_AFFILIATION_STATUS, SWISS_EDU_ID_AFFILIATION_PERIOD_BEGIN,
            SWISS_EDU_PERSON_UNIQUE_ID, SWISS_EDU_ID, "swissEduPersonHomeOrganization", "swissEduPersonHomeOrganizationType", "displayName", "eduPersonUniqueId",
            "eduPersonPrincipalName", "schacHomeOrganization", "swissEduPersonDateOfBirth", "swissEduPersonMatriculationNumber", "employeeNumber",
            "eduPersonOrgDN", "preferredLanguage", "eduPersonPrimaryAffiliation", "eduPersonPrimaryOrgUnitDN", "uid", "fschImapPW"};
    protected static final String[] SINGLE_INT_ATTRIBUTES = { "swissEduPersonGender" };
    protected static final String[] MULTI_STRING_ATTRIBUTES = { SCHEMAS, EDU_PERSON_AFFILIATION, EMAIL, "eduPersonScopedAffiliation", "commonName", "schacHomeOrganizationType",
            "swissEduPersonCardUID", "swissEduPersonStudyLevel", "swissLibraryPersonAffiliation", "swissLibraryPersonResidence", "eduPersonAssurance", "telephoneNumber",
            "postalAddress", "eduPersonEntitlement", "homePostalAddress", "isMemberOf", "mobile", "eduPersonNickname", "ou", "eduPersonOrgUnitDN", "homePhone", "eduPersonTargetedID"};
    protected static final String[] MULTI_INT_ATTRIBUTES = { "swissEduPersonStaffCategory", "swissEduPersonStudyBranch1", "swissEduPersonStudyBranch2", "swissEduPersonStudyBranch3"};
    protected static final String[] REQUIRED_SINGLE_ATTRIBUTES = { ID, EXTERNAL_ID, GIVEN_NAME, SURNAME, SWISS_EDU_ID_AFFILIATION_STATUS,
            SWISS_EDU_ID_AFFILIATION_PERIOD_BEGIN, SWISS_EDU_PERSON_UNIQUE_ID, SWISS_EDU_ID};
    protected static final String[] REQUIRED_MULTI_ATTRIBUTES = {/* SCHEMAS, */EDU_PERSON_AFFILIATION, EMAIL};
    // TODO meta.* tags if returned - not mentioned in samples

    private static String CONTENT_TYPE = "application/scim+json";
    protected static String AFFILIATION_OBJECT_CLASS = "affiliation"; // ObjectClass.ACCOUNT_NAME
    protected static String AFFILIATIONS = "Affiliations";
    private static String UID = SWISS_EDU_PERSON_UNIQUE_ID;

    @Override
    public void test() {
        LOG.ok("test - reading ServiceProviderConfig");
        try {
            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "/ServiceProviderConfig");
            JSONObject response = callRequest(request, true);
            LOG.ok("test - returning: {0}", response);
        } catch (IOException e) {
            throw new ConnectorIOException("Error when testing connection: " + e.getMessage(), e);
        }
    }

    @Override
    public void init(Configuration configuration) {
        super.init(configuration);
        LOG.ok("configuration: {0}", ((EduIdConfiguration) this.getConfiguration()).toString());
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public Schema schema() {
        SchemaBuilder schemaBuilder = new SchemaBuilder(EduIdConnector.class);
        buildAffiliationObjectClass(schemaBuilder);
        return schemaBuilder.build();
    }

    private void buildAffiliationObjectClass(SchemaBuilder schemaBuilder) {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
        objClassBuilder.setType(AFFILIATION_OBJECT_CLASS);

        for (String singleAttribute : SINGLE_STRING_ATTRIBUTES) {
            AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(singleAttribute);
            if (ArrayUtils.contains(REQUIRED_SINGLE_ATTRIBUTES, singleAttribute) || ArrayUtils.contains(REQUIRED_MULTI_ATTRIBUTES, singleAttribute)) {
                attrBuilder.setRequired(true);
            }
            objClassBuilder.addAttributeInfo(attrBuilder.build());
        }
        for (String singleAttribute : SINGLE_INT_ATTRIBUTES) {
            AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(singleAttribute, Integer.class);
            if (ArrayUtils.contains(REQUIRED_SINGLE_ATTRIBUTES, singleAttribute) || ArrayUtils.contains(REQUIRED_MULTI_ATTRIBUTES, singleAttribute)) {
                attrBuilder.setRequired(true);
            }
            objClassBuilder.addAttributeInfo(attrBuilder.build());
        }

        for (String multiAttribute : MULTI_STRING_ATTRIBUTES) {
            AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(multiAttribute);
            attrBuilder.setMultiValued(true);
            if (ArrayUtils.contains(REQUIRED_SINGLE_ATTRIBUTES, multiAttribute) || ArrayUtils.contains(REQUIRED_MULTI_ATTRIBUTES, multiAttribute)) {
                attrBuilder.setRequired(true);
            }
            objClassBuilder.addAttributeInfo(attrBuilder.build());
        }
        for (String multiAttribute : MULTI_INT_ATTRIBUTES) {
            AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(multiAttribute, Integer.class);
            attrBuilder.setMultiValued(true);
            if (ArrayUtils.contains(REQUIRED_SINGLE_ATTRIBUTES, multiAttribute) || ArrayUtils.contains(REQUIRED_MULTI_ATTRIBUTES, multiAttribute)) {
                attrBuilder.setRequired(true);
            }
            objClassBuilder.addAttributeInfo(attrBuilder.build());
        }

        schemaBuilder.defineObjectClass(objClassBuilder.build());
    }


    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(AFFILIATION_OBJECT_CLASS)) {    // __ACCOUNT__
            return createOrUpdateAffiliation(null, attributes);
        } else {
            // not found
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    protected JSONObject callRequest(HttpEntityEnclosingRequestBase request, JSONObject jo) throws IOException {
        // don't log request here - password field !!!
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);

        authHeader(request);

        HttpEntity entity = new ByteArrayEntity(jo.toString().getBytes("UTF-8"));
        request.setEntity(entity);
        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        processEduIdResponseErrors(response);

        String result = EntityUtils.toString(response.getEntity(), "UTF-8");
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    protected JSONObject callRequest(HttpRequestBase request, boolean parseResult) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);

        authHeader(request);

        CloseableHttpResponse response = null;
        response = execute(request);
        LOG.ok("response: {0}", response);
        processEduIdResponseErrors(response);

        if (!parseResult) {
            closeResponse(response);
            return null;
        }
        String result = EntityUtils.toString(response.getEntity(), "UTF-8");
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    private void authHeader(HttpRequestBase request){
        // to prevent several calls http://stackoverflow.com/questions/20914311/httpclientbuilder-basic-auth
        // auth header
        final StringBuilder sb = new StringBuilder();
        if (getConfiguration().getPassword() != null) {
            getConfiguration().getPassword().access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    sb.append(new String(chars));
                }
            });
        } else {
            return;
        }
        byte[] credentials = org.apache.commons.codec.binary.Base64.encodeBase64((getConfiguration().getUsername() + ":" + sb.toString()).getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + new String(credentials, StandardCharsets.UTF_8));
    }

    protected JSONArray callRequest(HttpRequestBase request) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);

        authHeader(request);

        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        processEduIdResponseErrors(response);

        String result = EntityUtils.toString(response.getEntity(), "UTF-8");
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONArray(result);
    }

    private void processEduIdResponseErrors(CloseableHttpResponse response){
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 409) {
            String result = null;
            try {
                result = EntityUtils.toString(response.getEntity(), "UTF-8");
                LOG.ok("Result body: {0}", result);
            } catch (IOException e) {
                throw new ConnectorIOException("Error when trying to get response entity: "+response, e);
            }
            String errDetail, scimType;
            try {
                JSONObject jo = new JSONObject(result);
                errDetail = jo.getString("detail");
                scimType = jo.getString("scimType");
            } catch (JSONException e) {
                closeResponse(response);
                throw new ConnectorIOException(e.getMessage() + " when parsing result: " + result, e);
            }

            if ("uniqueness".equals(scimType)) {
                closeResponse(response);
                throw new AlreadyExistsException("uniqueness: " + errDetail);
            } else {
                closeResponse(response);
                throw new ConnectorIOException("Error when process response: " + result);
            }
        } else if (statusCode == 400) {
            String result = null;
            try {
                result = EntityUtils.toString(response.getEntity(), "UTF-8");
                LOG.ok("Result body: {0}", result);
            } catch (IOException e) {
                throw new ConnectorIOException("Error when trying to get response entity: "+response, e);
            }
            String errDetail, scimType;
            try {
                JSONObject jo = new JSONObject(result);
                errDetail = jo.getString("detail");
                scimType = jo.getString("scimType");
            } catch (JSONException e) {
                closeResponse(response);
                throw new ConnectorIOException(e.getMessage() + " when parsing result: " + result, e);
            }

            if ("invalidValue".equals(scimType)) {
                closeResponse(response);
                throw new InvalidAttributeValueException("invalidValue: " + errDetail);
            } else {
                closeResponse(response);
                throw new ConnectorIOException("Error when process response: " + result);
            }
        }
        super.processResponseErrors(response);
    }

    private Uid createOrUpdateAffiliation(Uid uid, Set<Attribute> attributes) {
        LOG.ok("createOrUpdateAffiliation, Uid: {0}, attributes: {1}", uid, attributes);
        if (attributes == null || attributes.isEmpty()) {
            LOG.ok("request ignored, empty attributes");
            return uid;
        }
        boolean create = uid == null;
        JSONObject jo = new JSONObject();
        if (!create) {
            // update, need to read old values
            try {
                HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + AFFILIATIONS + "/" + uid.getUidValue());
                jo = callRequest(request, true);
            } catch (IOException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
            if (jo == null) {
                throw new UnknownUidException("Affiliation with ID " + uid.getUidValue() + " does not exist");
            }
        }
        else {
            // check mandatory attributes
            for (String required : REQUIRED_SINGLE_ATTRIBUTES) {
                String value = getStringAttr(attributes, required);
                if (StringUtil.isBlank(value)) {
                    throw new InvalidAttributeValueException("Missing mandatory attribute " + required+" ,value: "+value);
                }
            }

            for (String required : REQUIRED_MULTI_ATTRIBUTES) {
                String[] values = getMultiValAttr(attributes, required, null);
                if (values==null || values.length==0) {
                    throw new InvalidAttributeValueException("Missing mandatory attribute " + required+" ,value: "+values);
                }
            }
            //static
            String schema[] = {SCHEMAS_VALUE};
            jo.put(SCHEMAS, schema);
        }

        for (String attribute : SINGLE_STRING_ATTRIBUTES){
            putStringIfExists(attributes, attribute, jo);
        }
        for (String attribute : MULTI_STRING_ATTRIBUTES){
            putStringArrayIfExists(attributes, attribute, jo);
        }
        for (String attribute : MULTI_INT_ATTRIBUTES){
            putIntArrayIfExists(attributes, attribute, jo);
        }

        LOG.ok("affiliation request: {0}", jo.toString());

        try {
            HttpEntityEnclosingRequestBase request;
            if (create) {
                request = new HttpPost(getConfiguration().getServiceAddress() + AFFILIATIONS);
            } else {
                // update
                request = new HttpPut(getConfiguration().getServiceAddress() + AFFILIATIONS + "/" + uid.getUidValue());
            }
            JSONObject joResponse = callRequest(request, jo);

            String newUid = joResponse.getString(UID);
            LOG.info("response UID: {0}", newUid);
            return new Uid(newUid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }


    private void putStringIfExists(Set<Attribute> attributes, String fieldName, JSONObject jo) {
        String fieldValue = getStringAttr(attributes, fieldName);
        if (fieldValue != null) {
            jo.put(fieldName, fieldValue);
        }
    }

    private void putStringArrayIfExists(Set<Attribute> attributes, String attributeName, JSONObject jo) {
        String[] values = getMultiValAttr(attributes, attributeName, null);
        if (values != null) {
            jo.put(attributeName, values);
        }
    }
    private void putIntArrayIfExists(Set<Attribute> attributes, String attributeName, JSONObject jo) {
        Integer[] values = getIntMultiValAttr(attributes, attributeName, null);
        if (values != null) {
            jo.put(attributeName, values);
        }
    }

    protected Integer[] getIntMultiValAttr(Set<Attribute> attributes, String attrName, Integer[] defaultVal) {
        for (Attribute attr : attributes) {
            if (attrName.equals(attr.getName())) {
                List<Object> vals = attr.getValue();
                if (vals == null || vals.isEmpty()) {
                    // set empty value
                    return new Integer[0];
                }
                Integer[] ret = new Integer[vals.size()];
                for (int i = 0; i < vals.size(); i++) {
                    Object valAsObject = vals.get(i);
                    if (valAsObject == null)
                        throw new InvalidAttributeValueException("Value " + null + " must be not null for attribute " + attrName);

                    Integer val = Integer.parseInt((String)valAsObject);
                    ret[i] = val;
                }
                return ret;
            }
        }
        // set default value when attrName not in changed attributes
        return defaultVal;
    }
    @Override
    public void checkAlive() {
        test();
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {
        try {
            if (objectClass.is(AFFILIATION_OBJECT_CLASS)) {
                LOG.ok("delete affiliation, Uid: {0}", uid);
                HttpDelete request = new HttpDelete(getConfiguration().getServiceAddress() + AFFILIATIONS + "/" + uid.getUidValue());
                callRequest(request, false);
            } else {
                // not found
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(AFFILIATION_OBJECT_CLASS)) {
            return createOrUpdateAffiliation(uid, attributes);
        } else {
            // not found
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }


    @Override
    public FilterTranslator<EduIdFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new EduIdFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass objectClass, EduIdFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            LOG.info("executeQuery on {0}, query: {1}, options: {2}", objectClass, query, options);
            if (objectClass.is(AFFILIATION_OBJECT_CLASS)) {
                //find by Uid (user Primary Key)
                if (query != null && query.byUid != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + AFFILIATIONS + "/" + query.byUid);
                    JSONObject affiliation = callRequest(request, true);
                    if (affiliation == null) {
                        throw new UnknownUidException("Affiliation with ID " + query.byUid + " does not exist");
                    }
                    ConnectorObject connectorObject = convertAffiliationToConnectorObject(affiliation);
                    handler.handle(connectorObject);
                } else {
                    throw new UnsupportedOperationException("Unsupported listing operation in SCIM API, options: " + options);
                }

            } else {
                // not found
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private ConnectorObject convertAffiliationToConnectorObject(JSONObject affiliation) throws IOException {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        ObjectClass objectClass = new ObjectClass(AFFILIATION_OBJECT_CLASS);
        builder.setObjectClass(objectClass);

        String uid = affiliation.getString(UID);
        builder.setUid(new Uid(uid));
        builder.setName(uid);

        for (String attribute : SINGLE_STRING_ATTRIBUTES){
            getStringIfExists(affiliation, attribute, builder);
        }
        for (String attribute : SINGLE_INT_ATTRIBUTES){
            getIntIfExists(affiliation, attribute, builder);
        }
        for (String attribute : MULTI_STRING_ATTRIBUTES){
            getMultiStringIfExists(affiliation, attribute, builder);
        }
        for (String attribute : MULTI_INT_ATTRIBUTES){
            getMultiIntIfExists(affiliation, attribute, builder);
        }

        ConnectorObject connectorObject = builder.build();
        LOG.ok("convertAffiliationToConnectorObject, affiliation: {0}, \n\tconnectorObject: {1}",
                uid, connectorObject);
        return connectorObject;
    }

    private void getStringIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            if (object.get(attrName) != null && !JSONObject.NULL.equals(object.get(attrName))) {
                addAttr(builder, attrName, object.getString(attrName));
            }
        }
    }

    private void getIntIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            if (object.get(attrName) != null && !JSONObject.NULL.equals(object.get(attrName))) {
                addAttr(builder, attrName, object.getInt(attrName));
            }
        }
    }

    private void getMultiStringIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            Object valueObject = object.get(attrName);
            if (object.get(attrName) != null && !JSONObject.NULL.equals(valueObject)) {
                List<String> values = new ArrayList<>();
                if (valueObject instanceof JSONArray) {
                    JSONArray objectArray = object.getJSONArray(attrName);
                    for (int i = 0; i < objectArray.length(); i++) {
                        values.add(objectArray.getString(i));
                    }
                    builder.addAttribute(attrName, values.toArray());
                } else if (valueObject instanceof String) {
                    addAttr(builder, attrName, object.getString(attrName));
                } else {
                    throw new InvalidAttributeValueException("Unsupported value '" + valueObject + "' for attribute name '" + attrName + "' from " + object);
                }
            }
        }
    }

    private void getMultiIntIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            Object valueObject = object.get(attrName);
            if (object.get(attrName) != null && !JSONObject.NULL.equals(valueObject)) {
                List<Integer> values = new ArrayList<>();
                if (valueObject instanceof JSONArray) {
                    JSONArray objectArray = object.getJSONArray(attrName);
                    for (int i = 0; i < objectArray.length(); i++) {
                        values.add(objectArray.getInt(i));
                    }
                    builder.addAttribute(attrName, values.toArray());
                } else if (valueObject instanceof String) {
                    addAttr(builder, attrName, object.getString(attrName));
                } else {
                    throw new InvalidAttributeValueException("Unsupported value '" + valueObject + "' for attribute name '" + attrName + "' from " + object);
                }
            }
        }
    }
}
