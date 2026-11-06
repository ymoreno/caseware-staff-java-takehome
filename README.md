# CaseWare Java Staff Take-Home

Submission for the Java Staff Software Developer IV take-home.

## Deliverables

- [Design document (PDF)](CaseWare-Java-Staff-Takehome-Yesid-Moreno.pdf)
- [Design document (Markdown)](docs/design.md)
- [Architecture and version DAG diagrams](docs/diagrams/)
- [Part 2: template publish fan-out worker](part2/)
- [AI usage note](docs/ai-usage.md)

## Part 2

Java 17 and Maven. The worker processes a `TemplatePublished` event, pages the engagement catalog for the affected template and region, and marks affected engagements as pending through a downstream that takes about one minute per call and has limited capacity.

```bash
cd part2
mvn test
mvn package
```

The [Part 2 README](part2/README.md) covers the concurrency model, failure handling, idempotency, checkpointing, replay and tradeoffs.

## Three decisions that drive the design

1. Pending state is derived from a small engagement metadata index, not from reading engagements.
2. Summaries are generated per version range from the template diff, never per engagement.
3. The slow per-engagement fan-out is asynchronous and never gates the seconds-level indicator.

## Open questions

The design document lists the assumptions and open questions I would confirm with the team, including the downstream quota, the applied version history for existing engagements, the value of X and whether Product accepts offering only the head of a template version DAG.

## AI usage

See [docs/ai-usage.md](docs/ai-usage.md).
