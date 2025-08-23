# guardrails.py
import fnmatch
from policy_loader import load_policy

def policy_check(target: str, action: str):
    policy = load_policy()
    # First: denylist
    for rule in policy.get("denylist", []):
        if fnmatch.fnmatch(target, rule["pattern"]) and action in rule["actions"]:
            return False, rule.get("decision_code", "DENY_MATCH")

    # Then: allowlist
    for rule in policy.get("allowlist", []):
        if fnmatch.fnmatch(target, rule["pattern"]) and action in rule["actions"]:
            return True, "ALLOW_MATCH"

    # Finally: overrides
    for rule in policy.get("overrides", []):
        if fnmatch.fnmatch(target, rule["pattern"]) and action in rule.get("actions", []):
            return True, rule.get("decision_code", "OVERRIDE_MATCH")

    return False, "DENY_DEFAULT"
