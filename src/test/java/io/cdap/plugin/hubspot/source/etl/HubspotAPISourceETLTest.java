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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Map;

public abstract class HubspotAPISourceETLTest extends BaseHubspotETLTest {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  @Rule
  public TestName testName = new TestName();

  protected static String apiKey;

  @Test
  public void testContactLists() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contact Lists")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testContacts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contacts")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testEmailEvents() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Events")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testEmailSubscription() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Subscription")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testRecentCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Recent Companies")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Companies")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testDeals() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deals")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testDealPipelines() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deal Pipelines")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testMarketingEmail() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Marketing Email")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testProducts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Products")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testTickets() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Tickets")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsCategory() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsContent() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsObject() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsCategorySummarizeDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "summarize/daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsContentDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }

  @Test
  public void testAnalyticsObjectMonthly() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "monthly")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();


    List<StructuredRecord> records = getPipelineResults(properties, 1);
  }
}
