import { z } from "zod";
import { MetricHandler } from "@src/confluent/tools/handlers/metrics/metric-handler.js";
import { ToolConfig } from "@src/confluent/tools/base-tools.js";
import { ToolName } from "@src/confluent/tools/tool-name.js";
import { ClientManager } from "@src/confluent/client-manager.js";
import { CallToolResult } from "@src/confluent/schema.js";
import { EnvVar } from "@src/env-schema.js";
import env from "@src/env.js";

const getPrincipalMetricsArguments = z.object({
  clusterId: z
    .string()
    .describe("The Kafka cluster ID (e.g., lkc-xxxxxx)")
    .default(() => env.KAFKA_CLUSTER_ID || ""),
  metrics: z
    .array(z.string())
    .default([
      "io.confluent.kafka.server/request_bytes",
      "io.confluent.kafka.server/response_bytes",
      "io.confluent.kafka.server/active_connection_count",
      "io.confluent.kafka.server/request_count",
      "io.confluent.kafka.server/deprecated_request_count",
      "io.confluent.kafka.server/successful_authentication_count",
    ]),
  intervalStart: z.string().optional(),
  intervalEnd: z.string().optional(),
  limit: z.number().optional(),
  aggregationType: z.enum(["SUM", "MIN", "MAX"]).optional(),
  specificMetric: z
    .string()
    .optional()
    .describe("If provided, only this specific metric will be queried"),
  principalName: z
    .string()
    .optional()
    .describe("Optional principal name to filter results by"),
  includeRelatedMetrics: z
    .boolean()
    .optional()
    .describe(
      "If true, include related metrics information from the metrics descriptor API",
    ),
});

export class GetPrincipalMetricsHandler extends MetricHandler {
  getGroupBy() {
    return "metric.principal";
  }
  getFilterField() {
    return "resource.kafka.id";
  }
  getFilterValue(args: unknown) {
    return (args as { clusterId: string }).clusterId;
  }
  getSchema() {
    return getPrincipalMetricsArguments;
  }

  getRequiredEnvVars(): EnvVar[] {
    return [
      "CONFLUENT_CLOUD_API_KEY",
      "CONFLUENT_CLOUD_API_SECRET",
      "CONFLUENT_CLOUD_TELEMETRY_ENDPOINT",
    ];
  }

  isConfluentCloudOnly(): boolean {
    return true;
  }

  getToolConfig(): ToolConfig {
    return {
      name: ToolName.GET_PRINCIPAL_METRICS,
      description:
        "Get metrics for Kafka principals (users/services). Optionally filter by principalName. Support aggregation types (SUM, MIN, MAX) and set includeRelatedMetrics=true to get additional metadata about the metrics.",
      inputSchema: getPrincipalMetricsArguments.shape,
    };
  }

  async handle(
    clientManager: ClientManager,
    toolArguments: Record<string, unknown>,
  ): Promise<CallToolResult> {
    const args = this.getSchema().parse(toolArguments);
    // Override metrics if specificMetric is provided
    const metrics = args.specificMetric ? [args.specificMetric] : args.metrics;
    // Use default intervals if not provided
    let intervalStart = args.intervalStart;
    let intervalEnd = args.intervalEnd;
    if (!intervalStart || !intervalEnd) {
      const defaultIntervals = this.getDefaultIntervals();
      intervalStart = intervalStart || defaultIntervals.intervalStart;
      intervalEnd = intervalEnd || defaultIntervals.intervalEnd;
    }
    const { principalName, aggregationType, limit } = args;
    const postFilter = principalName
      ? (data: unknown) =>
          typeof data === "object" && data !== null
            ? {
                ...data,
                data: Array.isArray((data as { data?: unknown }).data)
                  ? ((data as { data?: unknown }).data as unknown[]).filter(
                      (row: unknown) =>
                        typeof row === "object" &&
                        row !== null &&
                        "metric.principal" in row &&
                        (row as Record<string, unknown>)["metric.principal"] ===
                          principalName,
                    )
                  : [],
              }
            : data
      : undefined;
    return this.handleMetricsWithFilter(
      clientManager,
      { ...args, metrics },
      aggregationType,
      limit,
      intervalStart,
      intervalEnd,
      postFilter,
    );
  }
}
