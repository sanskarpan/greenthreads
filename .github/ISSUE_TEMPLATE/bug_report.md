---
name: Bug Report
about: Report unexpected behavior or crashes
labels: bug
---

## Describe the Bug

<!-- A clear and concise description of what the bug is. -->

## To Reproduce

Steps to reproduce the behavior:

1.
2.
3.

## Expected Behavior

<!-- What did you expect to happen? -->

## Actual Behavior

<!-- What actually happened? Include any error messages, stack traces, or log output. -->

## Environment

| Field                        | Value |
| ---------------------------- | ----- |
| Go version (`go version`)    |       |
| OS / architecture            |       |
| greenthreads version / commit |      |
| Scheduler type used          | FIFO / RoundRobin / Priority / WorkStealing |

## Minimal Reproduction

<!-- Paste the smallest possible Go snippet that demonstrates the problem. -->

```go
package main

import (
    // ...
)

func main() {
    // minimal reproduction
}
```

## Additional Context

<!-- Anything else that might help: pprof output, race detector output, deadlock detector output, etc. -->
