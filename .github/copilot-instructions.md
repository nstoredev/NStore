
# Copilot / Agent Instructions for NStore

Technology Stack: C#, .NET compiled for netstandard2.0, NET8.0, NET10.0

## Coding Conventions

- All classes should be documented with XML comments.
- All classes should have a good coverage of unit tests.
- Tests for persistence layers are done using a base class with common tests, and each implementation only needs to provide the specific setup.
- Specific persistence providers can have special test that are useful for that provider only.