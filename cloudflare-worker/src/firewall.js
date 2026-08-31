import { INPUT_MAX_LENGTH } from "./constants.js";

export const PROMPT_INJECTION_PATTERNS = [
  /ignore\s*(?:previous|above|the|standard|your|my)?\s*instructions/i,
  /disregard\s*(?:previous|above|the|standard|your|my)?\s*(?:instructions|rules)/i,
  /system\s+(?:role|prompt|instructions|context)/i,
  /you\s+are\s+now\s+(?:a|in)/i,
  /act\s+as\s+a/i,
  /override\s*(?:rules|instructions|system|filters)/i,
  /bypass\s*(?:rules|instructions|system|filters)/i,
  /forget\s*(?:rules|instructions|everything)/i,
  /reveal\s*(?:system\s+)?(?:prompt|instructions|guidelines)/i,
  /show\s*(?:system\s+)?(?:prompt|instructions|guidelines)/i,
  /print\s*(?:system\s+)?(?:prompt|instructions|guidelines)/i,
  /what\s+is\s+your\s+(?:system\s+)?prompt/i,
  /developer\s+mode/i,
  /debug\s+mode/i,
  /dan\s+mode/i,
  /\bsudo\b/i,
  /jailbreak/i
];

export const SOQL_INJECTION_PATTERNS = [
  /union\s+select/i,
  /or\s+1\s*=\s*1/i,
  /or\s+\'\\w*\'\s*=\s*\'\\w*\'/i,
  /\'\s*;\s*drop\s+table/i
];

// Returns { blocked, threatType } for a given input string.
export function checkInjection(input) {
  if (typeof input !== "string" || !input.trim()) {
    return { blocked: false, threatType: null };
  }

  if (input.length > INPUT_MAX_LENGTH) {
    return { blocked: true, threatType: "Input Too Long" };
  }

  for (const pat of PROMPT_INJECTION_PATTERNS) {
    if (pat.test(input))
      return { blocked: true, threatType: "Prompt Injection" };
  }
  for (const pat of SOQL_INJECTION_PATTERNS) {
    if (pat.test(input)) return { blocked: true, threatType: "SOQL Injection" };
  }

  return { blocked: false, threatType: null };
}
