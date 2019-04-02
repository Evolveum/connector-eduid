/**
 * Copyright (c) 2019 Evolveum
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.eduid;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author oscar
 */
public class TestClient {

    private static final Log LOG = Log.getLog(TestClient.class);

    private static EduIdConfiguration conf;
    private static EduIdConnector conn;


    private static /*final */String DOMAIN = "";
    private static final String ID = "859379";

    ObjectClass affiliationObjectClass = new ObjectClass(EduIdConnector.AFFILIATION_OBJECT_CLASS);

    @BeforeClass
    public static void setUp() throws Exception {
        String fileName = "test.properties";

        final Properties properties = new Properties();
        InputStream inputStream = TestClient.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Sorry, unable to find " + fileName);
        }
        properties.load(inputStream);

        conf = new EduIdConfiguration();
        conf.setUsername(properties.getProperty("username"));
        conf.setPassword(new GuardedString(properties.getProperty("password").toCharArray()));
        conf.setServiceAddress(properties.getProperty("serviceAddress"));
        conf.setAuthMethod(properties.getProperty("authMethod"));
        conf.setTrustAllCertificates(Boolean.parseBoolean(properties.getProperty("trustAllCertificates")));

        conn = new EduIdConnector();
        conn.init(conf);

        DOMAIN = properties.getProperty("domain");
    }

    @Test
    public void testConn() {
        conn.test();
    }

    @Test
    public void testSchema() {
        Schema schema = conn.schema();
        LOG.info("schema: " + schema);
    }

    @Test
    public void testGet() throws IOException {
        HttpGet request = new HttpGet(conf.getServiceAddress() + EduIdConnector.AFFILIATIONS + "/" + ID+DOMAIN);
        CloseableHttpResponse response = conn.execute(request);
        conn.processResponseErrors(response);
        LOG.info("resp: {0}", response);
        String result = EntityUtils.toString(response.getEntity());
        LOG.info("content: {0}", result);
    }

    @Test
    public void testCreateJson() throws IOException {
        JSONObject jo = new JSONObject();
        jo.put("schemas", getJsonArray("urn:mace:switch.ch:eduid:scim:1.0:affiliation"));
        jo.put("id", ID);
        jo.put("externalId", ID);
        jo.put("swissEduID", "c4d52189-9c62-4f41-b2a1-d3b6ce07dc7e");
        jo.put("swissEduPersonUniqueID", ID);
        jo.put("email", getJsonArray("bsmith@example.com"));
        jo.put("givenName", "Barbara");
        jo.put("surname", "Smith");
        jo.put("swissEduIDAffiliationStatus", "current");
        jo.put("swissEduIDAffiliationPeriodBegin", "2018-01-01");
        jo.put("eduPersonAffiliation", getJsonArray("student"));

        LOG.info("json: {0}", jo.toString());
        //throw new IOException("e");
    }

    private JSONArray getJsonArray(String value) {
        JSONArray array = new JSONArray();
        array.put(value);
        return array;
    }

    @Test
    public void testCreateUser() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String randName = ID; //"1111";// + (new Random()).nextInt();
        String id = randName+DOMAIN;

//        attributes.add(AttributeBuilder.build(Name.NAME, randName));
        String[] schemas = {EduIdConnector.SCHEMAS_VALUE};
        attributes.add(AttributeBuilder.build(EduIdConnector.SCHEMAS, schemas));
        attributes.add(AttributeBuilder.build(EduIdConnector.ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.EXTERNAL_ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_PERSON_UNIQUE_ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID, "00007a31-2f1f-4f3f-9a6a-0064363eb6c4"));
        String[] emails = {"bsmith@example.com"};
        attributes.add(AttributeBuilder.build(EduIdConnector.EMAIL, emails));
        attributes.add(AttributeBuilder.build(EduIdConnector.GIVEN_NAME, "Barbara"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SURNAME, "Smith"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID_AFFILIATION_STATUS, "current"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID_AFFILIATION_PERIOD_BEGIN, "2018-01-01"));
        String[] affiliations = {"student"};
        attributes.add(AttributeBuilder.build(EduIdConnector.EDU_PERSON_AFFILIATION, affiliations));

        Uid userUid = conn.create(affiliationObjectClass, attributes, null);
        LOG.ok("New user Uid is: {0}, sent: {1}", userUid.getUidValue(), id);
    }

    @Test
    public void testDeleteUser() {
        Uid uid = new Uid(ID+DOMAIN);
        conn.delete(affiliationObjectClass, uid, null);
    }
    @Test
    public void testUpdateUser() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String randName = ID; //"1111";// + (new Random()).nextInt();
        String id = randName+DOMAIN;

//        attributes.add(AttributeBuilder.build(Name.NAME, randName));
        String[] schemas = {EduIdConnector.SCHEMAS_VALUE};
        attributes.add(AttributeBuilder.build(EduIdConnector.SCHEMAS, schemas));
        attributes.add(AttributeBuilder.build(EduIdConnector.ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.EXTERNAL_ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_PERSON_UNIQUE_ID, id));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID, "00007a31-2f1f-4f3f-9a6a-0064363eb6c4"));
        String[] emails = {"bsmithUpdate@example.com", "bsmithNew@example.com"};
        attributes.add(AttributeBuilder.build(EduIdConnector.EMAIL, emails));
        attributes.add(AttributeBuilder.build(EduIdConnector.GIVEN_NAME, "BarbaraUpdate"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SURNAME, "SmithUpdate"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID_AFFILIATION_STATUS, "suspended"));
        attributes.add(AttributeBuilder.build(EduIdConnector.SWISS_EDU_ID_AFFILIATION_PERIOD_BEGIN, "2018-03-03"));
        String[] affiliations = {"student", "employee"};
        attributes.add(AttributeBuilder.build(EduIdConnector.EDU_PERSON_AFFILIATION, affiliations));

        Uid uid = new Uid(id);
        Uid userUid = conn.update(affiliationObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());
    }


    @Test
    public void findByUid() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        EduIdFilter searchByUid = new EduIdFilter();
        searchByUid.byUid = "2470"+DOMAIN;
        LOG.ok("start finding: "+searchByUid.byUid);
        conn.executeQuery(affiliationObjectClass, searchByUid, rh, null);
        LOG.ok("end finding");
    }


    @Test
    public void findAll() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // all
        EduIdFilter filter = new EduIdFilter();
        conn.executeQuery(affiliationObjectClass, filter, rh, null);
    }

}
