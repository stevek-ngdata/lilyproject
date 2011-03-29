Repository impl tests
=====================

The tests for lily-repository-impl are in a separate project,
lily-repository-impl-tests.

This is because these tests depend on the common test utility code
in lily-repo-test-fw, which in turn depends on lily-repository-impl,
hence causing a circular dependency (Maven detects circular dependencies
on the level of projects).

So please add any new tests to lily-repository-impl-tests, so that
all tests stay together.