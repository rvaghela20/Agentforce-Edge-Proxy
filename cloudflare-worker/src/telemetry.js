import { COST_PER_INPUT_TOKEN, COST_PER_OUTPUT_TOKEN } from "./constants.js";

export function estimateCost(promptTokens, completionTokens) {
  return promptTokens * COST_PER_INPUT_TOKEN + completionTokens * COST_PER_OUTPUT_TOKEN;
}
