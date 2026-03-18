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
package tests;

import static api.AzureTestHelper.createUsageAggregate;
import static com.redhat.swatch.component.tests.utils.Topics.BILLABLE_USAGE_HOURLY_AGGREGATE;
import static com.redhat.swatch.component.tests.utils.Topics.BILLABLE_USAGE_STATUS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import api.MessageValidators;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.candlepin.subscriptions.billable.usage.BillableUsage;
import org.candlepin.subscriptions.billable.usage.BillableUsage.Status;
import org.candlepin.subscriptions.billable.usage.BillableUsageAggregate;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SimpleAzureComponentTest extends BaseAzureComponentTest {

  /** Verify billable usage sent to Azure with Succeeded status */
  @Test
  public void testValidAzureUsageMessages() {
    // Setup

    String productId = "rhel-for-x86-els-payg-addon";
    String metricId = "vCPUs";
    String billingAccountId = UUID.randomUUID().toString();
    String azureResourceId = UUID.randomUUID().toString();
    String orgId = "123456";
    double totalValue = 2.0;
    String dimension = "vcpu_hours";

    // Setup Azure Wiremock endpoints
    wiremock.setupAzureUsageContext(azureResourceId, billingAccountId);

    // Send billable usage message to Kafka
    BillableUsageAggregate aggregateData =
        createUsageAggregate(productId, billingAccountId, metricId, totalValue, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    // Verify status topic shows "succeeded"
    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateMatches(billingAccountId, Status.SUCCEEDED),
        1);

    // Verify Azure usage was sent to Azure
    wiremock.verifyAzureUsage(azureResourceId, totalValue, dimension);
  }

  /** Verify billable usage with invalid timestamp format is handled properly */
  @Test
  @Tag("unhappy")
  public void testInvalidAzureUsageMessagesWrongDateFormat() {
    // Setup
    String productId = "rhel-for-x86-els-payg-addon";
    String metricId = "vCPUs";
    String billingAccountId = UUID.randomUUID().toString();
    String azureResourceId = UUID.randomUUID().toString();
    String orgId = "123456";
    double totalValue = 2.0;

    BillableUsageAggregate aggregate =
        createUsageAggregate(productId, billingAccountId, metricId, totalValue, orgId);

    Map<String, Object> aggregateMap = new java.util.HashMap<>();
    aggregateMap.put("totalValue", aggregate.getTotalValue());
    // Inject malformed field
    aggregateMap.put("windowTimestamp", "testerday");
    aggregateMap.put("aggregateId", aggregate.getAggregateId());
    aggregateMap.put("aggregateKey", aggregate.getAggregateKey());
    aggregateMap.put("snapshotDates", aggregate.getSnapshotDates());
    aggregateMap.put("status", aggregate.getStatus());
    aggregateMap.put("errorCode", aggregate.getErrorCode());
    aggregateMap.put("billedOn", aggregate.getBilledOn());
    aggregateMap.put("remittanceUuids", aggregate.getRemittanceUuids());

    // Send malformed billable usage message to Kafka
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateMap);

    // Wait for a status message (if any) and verify it contains the billing account ID
    // The status could be "failed", "error", or the message might not be sent at all
    kafkaBridge.waitForKafkaMessage(BILLABLE_USAGE_STATUS, MessageValidators.alwaysMatch(), 0);

    // Verify that no usage was sent to Azure
    wiremock.verifyNoAzureUsage(azureResourceId);
  }

  /** Verify null usage message is handled properly */
  @Test
  @Tag("unhappy")
  public void testNullUsageMessage() {
    // Setup
    String productId = "rhel-for-x86-els-payg-addon";
    String metricId = "vCPUs";
    String billingAccountId = UUID.randomUUID().toString();
    String azureResourceId = UUID.randomUUID().toString();
    String orgId = "123456";
    double totalValue = 2.0;
    String dimension = "vcpu_hours";

    // Setup Azure Wiremock endpoints
    wiremock.setupAzureUsageContext(azureResourceId, billingAccountId);

    // Step 1: Send billable usage with null message
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, Map.of());

    // The message should not have been sent at all
    kafkaBridge.waitForKafkaMessage(BILLABLE_USAGE_STATUS, MessageValidators.alwaysMatch(), 0);

    // Verify that no usage was sent to Azure
    wiremock.verifyNoAzureUsage();

    // Step 2: Send valid billable usage message to Kafka
    BillableUsageAggregate aggregateData =
        createUsageAggregate(productId, billingAccountId, metricId, totalValue, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    // Verify status topic shows "succeeded"
    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateMatches(billingAccountId, Status.SUCCEEDED),
        1);

    // Verify Azure usage was sent to Azure
    wiremock.verifyAzureUsage(azureResourceId, totalValue, dimension);
  }

  @Test
  @Tag("unhappy")
  public void testSubscriptionNotFound() {
    // Setup
    String productId = "rhel-for-x86-els-payg-addon";
    String metricId = "vCPUs";
    String billingAccountId = UUID.randomUUID().toString();
    String azureResourceId = UUID.randomUUID().toString();
    String orgId = "123456";
    double totalValue = 2.0;

    // Setup Azure Wiremock endpoints
    wiremock.setupAzureUsageContextToReturnSubscriptionNotFound(billingAccountId);

    // Send clean billable usage message to Kafka
    BillableUsageAggregate aggregateData =
        createUsageAggregate(productId, billingAccountId, metricId, totalValue, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    // Make sure kafka responds with subscription not found
    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            billingAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
        1);

    // Verify service produces appropriate log
    service.logs().assertContains("Subscription not found for aggregate=");

    // Verify that no usage was sent to Azure
    wiremock.verifyNoAzureUsage(azureResourceId);
  }

  /**
   * Reproduces the double-counting bug in swatch_producer_metered_total. When the producer fails
   * for aggregate 1 (totalValue=100) and then fails again for aggregate 2 (totalValue=120, which
   * includes the retried 100 + new 20), the metric counts 100+120=220 instead of 100+20=120.
   */
  @Test
  @Tag("unhappy")
  public void testMeteredTotalDoubleCountsOnFailedRetry() {
    String productId = "rhel-for-x86-els-payg-addon";
    String metricId = "vCPUs";
    String billingAccountId = UUID.randomUUID().toString();
    String metricName = "swatch_producer_metered_total";

    wiremock.setupAzureUsageContextToReturnSubscriptionNotFound(billingAccountId);

    // Capture initial metric value (counters accumulate across tests)
    double initialMetric =
        service.getMetricByTags(
            metricName,
            "product=\"" + productId + "\"",
            "status=\"failed\"",
            "error_code=\"subscription_not_found\"");

    // Aggregate 1: totalValue=100, metricIncrement=100 (first event, full amount is new)
    BillableUsageAggregate aggregate1 =
        createUsageAggregate(productId, billingAccountId, metricId, 100.0, orgId);
    aggregate1.setMetricIncrement(new BigDecimal("100.0"));
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregate1);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            billingAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
        1);

    // Verify metric incremented by 100
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              double current =
                  service.getMetricByTags(
                      metricName,
                      "product=\"" + productId + "\"",
                      "status=\"failed\"",
                      "error_code=\"subscription_not_found\"");
              assertEquals(
                  100.0,
                  current - initialMetric,
                  0.001,
                  "Metric should have incremented by 100 after aggregate 1");
            });

    // Aggregate 2: totalValue=120 (100 retried + 20 new), metricIncrement=20 (only delta)
    BillableUsageAggregate aggregate2 =
        createUsageAggregate(productId, billingAccountId, metricId, 120.0, orgId);
    aggregate2.setMetricIncrement(new BigDecimal("20.0"));
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregate2);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            billingAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
        2);

    // With metricIncrement set, producer uses delta (20) instead of totalValue (120)
    // Total metric: 100 + 20 = 120
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              double current =
                  service.getMetricByTags(
                      metricName,
                      "product=\"" + productId + "\"",
                      "status=\"failed\"",
                      "error_code=\"subscription_not_found\"");
              double delta = current - initialMetric;
              assertEquals(
                  120.0,
                  delta,
                  0.001,
                  "Metric delta should be 120 (100+20) with metricIncrement fix");
            });
  }
}
