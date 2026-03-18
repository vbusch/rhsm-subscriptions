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

import static api.AwsTestHelper.createUsageAggregate;
import static com.redhat.swatch.component.tests.utils.Topics.BILLABLE_USAGE_HOURLY_AGGREGATE;
import static com.redhat.swatch.component.tests.utils.Topics.BILLABLE_USAGE_STATUS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import api.MessageValidators;
import domain.Product;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import org.awaitility.Awaitility;
import org.candlepin.subscriptions.billable.usage.BillableUsage;
import org.candlepin.subscriptions.billable.usage.BillableUsageAggregate;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SimpleAwsComponentTest extends BaseAwsComponentTest {

  private static final double TOTAL_VALUE = 2.0;

  /** Verify billable usage is sent to AWS Marketplace with Succeeded status. */
  @Test
  public void testValidAwsUsageMessages() {
    String metricId = Product.ROSA.getFirstMetric().getId();

    wiremock.setupAwsUsageContext(
        awsAccountId, awsSellerAccountId, rhSubscriptionId, customerId, productCode);

    BillableUsageAggregate aggregateData =
        createUsageAggregate(Product.ROSA.getName(), awsAccountId, metricId, TOTAL_VALUE, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateMatches(awsAccountId, BillableUsage.Status.SUCCEEDED),
        1);
  }

  /** Verify null usage message is handled gracefully (no crash, no AWS call). */
  @Test
  @Tag("unhappy")
  public void testNullUsageMessage() {
    String metricId = Product.ROSA.getFirstMetric().getId();

    wiremock.setupAwsUsageContext(
        awsAccountId, awsSellerAccountId, rhSubscriptionId, customerId, productCode);

    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, Map.of());

    kafkaBridge.waitForKafkaMessage(BILLABLE_USAGE_STATUS, MessageValidators.alwaysMatch(), 0);

    wiremock.verifyNoAwsUsage();

    BillableUsageAggregate aggregateData =
        createUsageAggregate(Product.ROSA.getName(), awsAccountId, metricId, TOTAL_VALUE, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateMatches(awsAccountId, BillableUsage.Status.SUCCEEDED),
        1);
  }

  /**
   * Verify subscription not found scenario results in FAILED status with appropriate error code.
   */
  @Test
  @Tag("unhappy")
  public void testSubscriptionNotFound() {
    String metricId = Product.ROSA.getFirstMetric().getId();

    wiremock.setupAwsUsageContextToReturnSubscriptionNotFound(awsAccountId);

    BillableUsageAggregate aggregateData =
        createUsageAggregate(Product.ROSA.getName(), awsAccountId, metricId, TOTAL_VALUE, orgId);
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregateData);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            awsAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
        1);

    wiremock.verifyNoAwsUsage();
  }

  /**
   * Reproduces the double-counting bug in swatch_producer_metered_total. When the producer fails
   * for aggregate 1 (totalValue=100) and then fails again for aggregate 2 (totalValue=120, which
   * includes the retried 100 + new 20), the metric counts 100+120=220 instead of 100+20=120.
   */
  @Test
  @Tag("unhappy")
  public void testMeteredTotalDoubleCountsOnFailedRetry() {
    String metricId = Product.ROSA.getFirstMetric().getId();
    String metricName = "swatch_producer_metered_total";

    wiremock.setupAwsUsageContextToReturnSubscriptionNotFound(awsAccountId);

    // Capture initial metric value (counters accumulate across tests)
    double initialMetric =
        service.getMetricByTags(
            metricName,
            "product=\"" + Product.ROSA.getName() + "\"",
            "status=\"failed\"",
            "error_code=\"subscription_not_found\"");

    // Aggregate 1: totalValue=100, metricIncrement=100 (first event, full amount is new)
    BillableUsageAggregate aggregate1 =
        createUsageAggregate(Product.ROSA.getName(), awsAccountId, metricId, 100.0, orgId);
    aggregate1.setMetricIncrement(new BigDecimal("100.0"));
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregate1);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            awsAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
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
                      "product=\"" + Product.ROSA.getName() + "\"",
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
        createUsageAggregate(Product.ROSA.getName(), awsAccountId, metricId, 120.0, orgId);
    aggregate2.setMetricIncrement(new BigDecimal("20.0"));
    kafkaBridge.produceKafkaMessage(BILLABLE_USAGE_HOURLY_AGGREGATE, aggregate2);

    kafkaBridge.waitForKafkaMessage(
        BILLABLE_USAGE_STATUS,
        MessageValidators.aggregateFailure(
            awsAccountId, BillableUsage.ErrorCode.SUBSCRIPTION_NOT_FOUND),
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
                      "product=\"" + Product.ROSA.getName() + "\"",
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
