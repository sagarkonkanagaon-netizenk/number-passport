# Security policy

Number Passport is designed to run against production risk and finance warehouses, so we treat security
reports as the highest priority. The design is described in [docs/security.md](docs/security.md).

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes |

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately through GitHub:
[Security → Report a vulnerability](https://github.com/sagarkonkanagaon-netizenk/number-passport/security/advisories/new).
If that is not available to you, email **sagarkonkanagaon@gmail.com** with the subject
`[number-passport security]`.

Please include:

- the version (`passport --version`) and how you installed it;
- what an attacker can do, and the steps or a proof of concept;
- whether the issue is already public.

## What to expect

| Step | Target |
|------|--------|
| Acknowledgement | within 3 business days |
| First assessment and severity | within 7 business days |
| Fix or mitigation for critical and high issues | within 30 days |
| Public advisory and credit (if you want it) | when the fix is released |

## In scope

Especially welcome:

- any way to make the query guard pass a query that returns row-level data, writes, or reads files;
- any way to write through a connector, or bypass the read-only session;
- any way to make `passport verify` accept a modified evidence pack or changed code;
- code execution while parsing pipeline code (the code must only ever be parsed);
- secrets leaking into evidence packs, logs or MCP responses.

Out of scope: issues that need an attacker to already control `passport.yml`, the pipeline repository or the
database login, and the unauthenticated HTTP transport when it is exposed against the documented guidance.
