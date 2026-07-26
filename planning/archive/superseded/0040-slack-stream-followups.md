# Superseded Slack Stream Follow-ups

The following stream/card-specific backlog items were superseded by
[`0060-slack-snapshot-reporting`](../completed/0060-slack-snapshot-reporting.md),
which removed Slack streaming, task cards, and incremental timeline mutation in
favor of one canonical snapshot message.

## Items

| Item | Previous intent | Status |
| ---- | ---- | ---- |
| `0040-slack-queue-receipt-before-stream` | Split queued receipt from a claim-time stream to avoid idle stream expiry | superseded |
| `0048-slack-stream-error-classification` | Distinguish transient and permanent `appendStream` errors | superseded |
| `0051-slack-progress-sections-as-plan-tasks` | Represent progress subsections as streamed plan tasks | superseded |

The shipped snapshot path retains the useful outcomes without a stream
lifecycle: one immediate queued message, full `chat.update` renders for queue
and run state, bounded progress bars, debounced fine-grained updates, immediate
milestone/terminal flushes, and retryable update errors.
