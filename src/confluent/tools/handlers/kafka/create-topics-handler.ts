import { ClientManager } from "@src/confluent/client-manager.js";
import { CallToolResult } from "@src/confluent/schema.js";
import {
  BaseToolHandler,
  ToolConfig,
} from "@src/confluent/tools/base-tools.js";
import { ToolName } from "@src/confluent/tools/tool-name.js";
import { EnvVar } from "@src/env-schema.js";
import { z } from "zod";

const createTopicArgs = z.object({
  topicNames: z
    .array(z.string().describe("Names of kafka topics to create"))
    .nonempty(),
});
export class CreateTopicsHandler extends BaseToolHandler {
  async handle(
    clientManager: ClientManager,
    toolArguments: Record<string, unknown>,
  ): Promise<CallToolResult> {
    const { topicNames } = createTopicArgs.parse(toolArguments);
    const success = await (
      await clientManager.getAdminClient()
    ).createTopics({
      topics: topicNames.map((name) => ({ topic: name })),
    });
    if (!success) {
      return this.createResponse(
        `Failed to create Kafka topics: ${topicNames.join(",")}`,
        true,
      );
    }
    return this.createResponse(`Created Kafka topics: ${topicNames.join(",")}`);
  }

  getToolConfig(): ToolConfig {
    return {
      name: ToolName.CREATE_TOPICS,
      description: "Create one or more Kafka topics.",
      inputSchema: createTopicArgs.shape,
    };
  }

  getRequiredEnvVars(): EnvVar[] {
    return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"];
  }
}
