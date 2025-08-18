from memory_engine import log_policy_decision, get_recent_policy_events

log_policy_decision(
    actor="cmd_test",
    target="example.com",
    action="read",
    decision="ALLOW_MATCH",
    reason="Testing from Command Prompt",
    meta={"source": "cmd"}
)

print(get_recent_policy_events()[-1])
