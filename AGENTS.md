# Repository instructions

## Language

- Communicate with the user in Chinese.
- Write all repository documentation, code, and code comments in English.

## Design decisions

- Record meaningful trade-offs in `docs/design.md`: alternatives, the decision
  and its rationale, accepted limitations, and production reconsideration triggers.
- Distinguish confirmed decisions from proposals and unresolved questions.
- Keep the design document current as decisions change; production requirements
  may justify revisiting submission-specific choices.
- Prefer non-disruptive configuration changes: preserve quota history, prepare
  state before activation, and handle version refresh internally. Distinguish
  expected policy rejections from update failures and real dependency failures.

## Testing

- Use red-green-refactor TDD for core matching, accounting, and publication
  behaviors. Do not require test-first scaffolding or low-value tests of constants,
  trivial accessors, or framework behavior.
- Verify Redis accounting with real Redis and concurrent requests. Use actual
  Kong instances for multi-gateway integration evidence, not mocks alone.
- Provide a reproducible local demonstration and document its prerequisites.
