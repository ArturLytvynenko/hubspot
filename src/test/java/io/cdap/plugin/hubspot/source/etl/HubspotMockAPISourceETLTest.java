/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.hubspot.source.etl;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public abstract class HubspotMockAPISourceETLTest extends BaseHubspotETLTest {
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8456);

  @Test
  public void testContactLists() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contact Lists")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists?hapikey=some-api-key&count=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactListsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists?hapikey=some-api-key&count=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactListsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Contact Lists", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testContacts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contacts")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists/all/contacts/all?hapikey=some-api-key&count=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists/all/contacts/all?hapikey=some-api-key&count=100&vidOffset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Contacts", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testEmailEvents() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Events")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/events?hapikey=some-api-key&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailEventsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/events?hapikey=some-api-key&limit=100&offset=CgoY__________9_"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailEventsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Email Events", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testEmailSubscription() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Subscription")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/subscriptions/timeline?hapikey=some-api-key&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailSubscriptionP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/subscriptions/timeline" +
                            "?hapikey=some-api-key&limit=100&offset=CP__________fw"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailSubscriptionP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Email Subscription", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testRecentCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Recent Companies")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/recent/modified?hapikey=some-api-key&count=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testRecentCompaniesP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/recent/modified?hapikey=some-api-key&count=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testRecentCompaniesP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Recent Companies", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Companies")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/paged?hapikey=some-api-key&count=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testCompaniesP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/paged?hapikey=some-api-key&count=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testCompaniesP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Companies", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testDeals() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deals")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/deals/v1/deal/paged?hapikey=some-api-key&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/deals/v1/deal/paged?hapikey=some-api-key&limit=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Deals", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testDealPipelines() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deal Pipelines")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-pipelines/v1/pipelines/deals?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealPipelinesP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Deal Pipelines", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testMarketingEmail() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Marketing Email")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/marketing-emails/v1/emails?hapikey=some-api-key&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testMarketingEmailP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/marketing-emails/v1/emails?hapikey=some-api-key&limit=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testMarketingEmailP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Marketing Email", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testProducts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Products")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/products/paged?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testProductsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/products/paged?hapikey=some-api-key&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testProductsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Products", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testTickets() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Tickets")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/tickets/paged?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testTicketsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/tickets/paged?hapikey=some-api-key&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testTicketsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Tickets", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsCategory() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsContent() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsObject() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/total" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100&offset=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsCategorySummarizeDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "summarize/daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/summarize/daily" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));

    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsContentDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/daily" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&f=client&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsObjectMonthly() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_SERVER_URL, getServerAddress())
      .put(SourceHubspotConfig.API_KEY, "some-api-key")
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "monthly")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/monthly" +
                            "?hapikey=some-api-key&start=20190101&end=20191111&f=client&limit=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  protected String getServerAddress() {
    return "http://localhost:" + wireMockRule.port();
  }
}
