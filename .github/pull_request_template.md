## Summary

<!-- What changes and why? -->

## Evidence

- [ ] Dynamically reproduced: include test/command plus expected and actual results.
- [ ] Statically confirmed only: explain why runtime reproduction is unnecessary or unsafe.
- [ ] Hypothesis only: list the missing evidence; do not present it as a confirmed defect.
- [ ] The reproduction failed before the fix and passes after it.
- [ ] Security/release reproduction used an isolated temporary environment with no external side effects.

## Verification

- [ ] `pnpm test --run`
- [ ] `pnpm type-check`
- [ ] `pnpm build`

<!-- Include exact command results and any intentionally known-failing `test.fails` cases. -->
