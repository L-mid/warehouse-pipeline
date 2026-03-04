# Decisions Log

Log of major decisions that affect structure, contracts, or long-term maintainability.


Template:
- ID:
- Date:
- Status: proposed | accepted | superseded
- Context:
- Decision:
- Consequences:

---

## D001 — Snapshot mode is default

- ID: D001
- Date: 2026-03-04
- Status: accepted
- Context: Demo should be able to run the pipeline with one command reliably and not fail due to lack of internet/external API changes.
- Decision: The default demo path uses snapshot mode (`v1`), not live HTTP.
- Consequences:
  - Onboarding is smoother.
  - Live mode remains available but not required.
  - Must maintain and test two paths. 