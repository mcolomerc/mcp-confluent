import {
  PromptHandler,
  PromptConfig,
} from "@src/confluent/prompts/base-prompts.js";
import { PromptName } from "@src/confluent/prompts/prompt-name.js";
import { ReportClusterUsagePromptHandler } from "@src/confluent/prompts/handlers/report-cluster-usage-prompt-handler.js";
import { z } from "zod";

export interface PromptArgument {
  name: string;
  description: string;
  required?: boolean;
}

/**
 * Convert Zod schema to prompt arguments array
 * Similar to how tools generate their arguments from schemas
 */
function zodSchemaToPromptArguments(
  inputSchema: Record<string, z.ZodTypeAny>,
): PromptArgument[] {
  const args: PromptArgument[] = [];

  for (const [key, fieldSchema] of Object.entries(inputSchema)) {
    const zodType = fieldSchema as z.ZodTypeAny;
    let description = "";
    let required = true;

    // Extract description and required status
    if (zodType._def?.description) {
      description = zodType._def.description;
    }
    if (zodType instanceof z.ZodOptional) {
      required = false;
    }

    args.push({
      name: key,
      description,
      required,
    });
  }

  return args;
}

export class PromptFactory {
  private static promptHandlers: Map<PromptName, PromptHandler> = new Map([
    [PromptName.REPORT_CLUSTER_USAGE, new ReportClusterUsagePromptHandler()],
  ]);

  public static getPromptHandler(name: PromptName): PromptHandler | undefined {
    return this.promptHandlers.get(name);
  }

  public static getPromptHandlers(): Map<PromptName, PromptHandler> {
    return this.promptHandlers;
  }

  public static getPromptConfigs(): PromptConfig[] {
    return Array.from(this.promptHandlers.values()).map((handler) =>
      handler.getPromptConfig(),
    );
  }

  public static getPromptConfigsWithArguments(): Array<
    PromptConfig & { arguments: PromptArgument[] }
  > {
    return Array.from(this.promptHandlers.values()).map((handler) => {
      const config = handler.getPromptConfig();
      return {
        ...config,
        arguments: zodSchemaToPromptArguments(config.inputSchema),
      };
    });
  }

  public static getPromptNames(): PromptName[] {
    return Array.from(this.promptHandlers.keys());
  }
}
