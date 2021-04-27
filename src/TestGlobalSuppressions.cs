// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage(
    "Blocker Code Smell",
    "S2699:Tests should include assertions",
    Justification = "In xUnit is normal to have test without assertion, the assertion is not to throw exception",
    Scope = "module")]
[assembly: SuppressMessage(
    "Minor Code Smell",
    "S101:Types should be named in PascalCase",
    Justification = "We do not want this rule",
    Scope = "module")]
[assembly: SuppressMessage(
    "Design",
    "RCS1090:Add call to 'ConfigureAwait' (or vice versa).",
    Justification = "No need to call ConfigureAwait on test code",
    Scope = "module")]
[assembly: SuppressMessage(
    "Style",
    "IDE1006:Naming Styles",
    Justification = "Rule is not needed.",
    Scope = "module")]
[assembly: SuppressMessage(
    "Readability",
    "RCS1098:Constant values should be placed on right side of comparisons.",
    Justification = "Not needed in test code",
    Scope = "module")]

