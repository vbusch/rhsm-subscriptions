/*
 * Copyright Red Hat, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Red Hat trademarks are not licensed under GPLv3. No permission is
 * granted to use or replicate Red Hat trademarks that are incorporated
 * in this software or its documentation.
 */
package com.redhat.swatch.billable.usage.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.redhat.swatch.billable.usage.model.TallyMeasurement;
import com.redhat.swatch.billable.usage.model.TallySnapshot;
import com.redhat.swatch.billable.usage.model.TallySummary;
import com.redhat.swatch.configuration.registry.SubscriptionDefinition;
import com.redhat.swatch.configuration.registry.SubscriptionDefinitionRegistry;
import com.redhat.swatch.configuration.registry.Variant;
import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import org.candlepin.subscriptions.billable.usage.BillableUsage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BillableUsageMapperTest {

  private static final String CORES = "Cores";
  private static final String ROSA = "rosa";

  private static SubscriptionDefinitionRegistry originalReference;
  private SubscriptionDefinitionRegistry subscriptionDefinitionRegistry;

  private final BillableUsageMapper mapper = new BillableUsageMapper();

  @BeforeAll
  static void setupClass() throws Exception {
    Field instance = SubscriptionDefinitionRegistry.class.getDeclaredField("instance");
    instance.setAccessible(true);
    originalReference =
        (SubscriptionDefinitionRegistry) instance.get(SubscriptionDefinitionRegistry.class);
  }

  @AfterAll
  static void tearDown() throws Exception {
    Field instance = SubscriptionDefinitionRegistry.class.getDeclaredField("instance");
    instance.setAccessible(true);
    instance.set(instance, originalReference);
  }

  @BeforeEach
  void setupTest() {
    subscriptionDefinitionRegistry = mock(SubscriptionDefinitionRegistry.class);
    setMock(subscriptionDefinitionRegistry);
    var variant = Variant.builder().tag(ROSA).build();
    var awsMetric =
        com.redhat.swatch.configuration.registry.Metric.builder()
            .awsDimension("AWS_METRIC_ID")
            .id(CORES)
            .build();
    var subscriptionDefinition =
        SubscriptionDefinition.builder()
            .variants(Set.of(variant))
            .metrics(Set.of(awsMetric))
            .build();
    variant.setSubscription(subscriptionDefinition);
    when(subscriptionDefinitionRegistry.getSubscriptions())
        .thenReturn(List.of(subscriptionDefinition));
  }

  private void setMock(SubscriptionDefinitionRegistry mock) {
    try {
      Field instance = SubscriptionDefinitionRegistry.class.getDeclaredField("instance");
      instance.setAccessible(true);
      instance.set(instance, mock);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void shouldSkipNonPaygProducts() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    "RHEL",
                    TallySnapshot.Granularity.HOURLY,
                    TallySnapshot.Sla.STANDARD,
                    TallySnapshot.Usage.PRODUCTION,
                    TallySnapshot.BillingProvider.AWS,
                    "123"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldSkipAnySla() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    ROSA,
                    TallySnapshot.Granularity.HOURLY,
                    TallySnapshot.Sla.ANY,
                    TallySnapshot.Usage.PRODUCTION,
                    TallySnapshot.BillingProvider.AWS,
                    "123"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldSkipAnyUsage() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    ROSA,
                    TallySnapshot.Granularity.HOURLY,
                    TallySnapshot.Sla.STANDARD,
                    TallySnapshot.Usage.ANY,
                    TallySnapshot.BillingProvider.AWS,
                    "123"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldSkipAnyBillingProvider() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    ROSA,
                    TallySnapshot.Granularity.HOURLY,
                    TallySnapshot.Sla.STANDARD,
                    TallySnapshot.Usage.PRODUCTION,
                    TallySnapshot.BillingProvider.ANY,
                    "123"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldSkipAnyBillingAccountId() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    ROSA,
                    TallySnapshot.Granularity.HOURLY,
                    TallySnapshot.Sla.STANDARD,
                    TallySnapshot.Usage.PRODUCTION,
                    TallySnapshot.BillingProvider.AWS,
                    "_ANY"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldProduceBillableUsageWhenOrgIdPresent() {
    String expectedOrgId = "org123";
    String expectedBillingAccountId = "bill123";
    double expectedCurrentTotal = 88.0;
    OffsetDateTime expectedSnapshotDate = OffsetDateTime.MIN;
    BillableUsage expected =
        new BillableUsage()
            .withOrgId(expectedOrgId)
            .withProductId(ROSA)
            .withSnapshotDate(expectedSnapshotDate)
            .withUsage(BillableUsage.Usage.PRODUCTION)
            .withSla(BillableUsage.Sla.STANDARD)
            .withBillingProvider(BillableUsage.BillingProvider.AWS)
            .withBillingAccountId(expectedBillingAccountId)
            .withMetricId(CORES)
            .withValue(42.0)
            .withCurrentTotal(expectedCurrentTotal);

    var summary =
        createExampleTallySummaryWithOrgId(
            ROSA,
            TallySnapshot.Granularity.HOURLY,
            TallySnapshot.Sla.STANDARD,
            TallySnapshot.Usage.PRODUCTION,
            TallySnapshot.BillingProvider.AWS,
            expectedBillingAccountId);
    summary.getTallySnapshots().stream()
        .flatMap(s -> s.getTallyMeasurements().stream())
        .forEach(m -> m.withCurrentTotal(expectedCurrentTotal));

    BillableUsage actual = mapper.fromTallySummary(summary).findAny().orElseThrow();
    expected.setUuid(actual.getUuid()); // this is auto-generated
    assertEquals(expected, actual);
  }

  @Test
  void shouldSkipNonDailySnapshots() {
    assertTrue(
        mapper
            .fromTallySummary(
                createExampleTallySummaryWithOrgId(
                    ROSA,
                    TallySnapshot.Granularity.YEARLY,
                    TallySnapshot.Sla.STANDARD,
                    TallySnapshot.Usage.PRODUCTION,
                    TallySnapshot.BillingProvider.AWS,
                    "123"))
            .findAny()
            .isEmpty());
  }

  @Test
  void shouldSkipSummaryWithNoMeasurements() {
    TallySummary tallySummary =
        createExampleTallySummaryWithOrgId(
            ROSA,
            TallySnapshot.Granularity.HOURLY,
            TallySnapshot.Sla.STANDARD,
            TallySnapshot.Usage.PRODUCTION,
            TallySnapshot.BillingProvider.AWS,
            "123");
    tallySummary.getTallySnapshots().get(0).setTallyMeasurements(null);
    assertTrue(mapper.fromTallySummary(tallySummary).findAny().isEmpty());
  }

  TallySummary createExampleTallySummaryWithOrgId(
      String productId,
      TallySnapshot.Granularity granularity,
      TallySnapshot.Sla sla,
      TallySnapshot.Usage usage,
      TallySnapshot.BillingProvider billingProvider,
      String billingAccountId) {
    return new TallySummary()
        .withOrgId("org123")
        .withTallySnapshots(
            List.of(
                new TallySnapshot()
                    .withSnapshotDate(OffsetDateTime.MIN)
                    .withProductId(productId)
                    .withGranularity(granularity)
                    .withTallyMeasurements(
                        List.of(
                            new TallyMeasurement()
                                .withMetricId(CORES)
                                .withHardwareMeasurementType("PHYSICAL")
                                .withValue(42.0),
                            new TallyMeasurement()
                                .withMetricId(CORES)
                                .withHardwareMeasurementType("TOTAL")
                                .withValue(42.0)))
                    .withSla(sla)
                    .withUsage(usage)
                    .withBillingProvider(billingProvider)
                    .withBillingAccountId(billingAccountId)));
  }
}
