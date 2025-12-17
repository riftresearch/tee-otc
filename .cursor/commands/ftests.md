Run the integration tests using the following command:
`just test`

If any fail, you can run the failing test in isolation like so:
`just redb && cargo nextest run <failing_test> --nocapture`

Then, keep fixing/running tests in a cycle until all tests pass!
Remember, NO "hacky" fixes at all - if there's an issue that seems like it would require a refactor
of the underlying code being test - let me know.
