# AGENTS

Use the `just` recipes defined in `justfile` for test execution.

- For the full test suite, run `just test`.
- For a specific test or targeted cargo invocation, run `just redb` first, then run the specific test command.
- Do not skip `just redb` before targeted tests. It resets and starts the test database so you avoid the `PoolTimedOut` failures from stale or exhausted test DB state.
