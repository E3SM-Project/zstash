## Summary

Objectives:
- Objective 1
- Objective 2
- ...
- Objective n

Issue resolution:
- Closes #<ISSUE_NUMBER_HERE>

Select one: This pull request is...
- [ ] a bug fix: increment the patch version
- [ ] a small improvement: increment the minor version
- [ ] a new feature: increment the minor version
- [ ] an incompatible (non-backwards compatible) API change: increment the major version

Please fill out either the "Small Change" or "Big Change" section (the latter includes the numbered subsections), and delete the other.

## Small Change

- [ ] To merge, I will use "Squash and merge". That is, this change should be a single commit.
- [ ] Logic: I have visually inspected the entire pull request myself.
- [ ] Pre-commit checks: All the pre-commits checks have passed.

## Big Change

- [ ] To merge, I will use "Create a merge commit". That is, this change is large enough to require multiple units of work (i.e., it should be multiple commits).

### 1. Does this do what we want it to do?

Required:
- [ ] Product Management: I have confirmed with the stakeholders that the objectives above are correct and complete.
- [ ] Testing: I have added at least one automated test. Every objective above is represented in at least one test.
- [ ] Testing: I have considered likely and/or severe edge cases and have included them in testing.

If applicable:
- [ ] Testing: this pull request adds at least one new possible command line option. I have tested using this option with and without any other option that may interact with it.

### 2. Are the implementation details accurate & efficient?

Required:
- [ ] Logic: I have visually inspected the entire pull request myself.
- [ ] Logic: I have left GitHub comments highlighting important pieces of code logic. I have had these code blocks reviewed by at least one other team member.

If applicable:
- [ ] Dependencies: This pull request introduces a new dependency. I have discussed this requirement with at least one other team member. The dependency is noted in `zstash/conda`, not just an `import` statement.

### 3. Is this well documented?

Required:
- [ ] Documentation: by looking at the docs, a new user could easily understand the functionality introduced by this pull request.

### 4. Is this code clean?

Required:
- [ ] Readability: The code is as simple as possible and well-commented, such that a new team member could understand what's happening.
- [ ] Pre-commit checks: All the pre-commits checks have passed.

If applicable:
- [ ] Software architecture: I have discussed relevant trade-offs in design decisions with at least one other team member. It is unlikely that this pull request will increase tech debt.
