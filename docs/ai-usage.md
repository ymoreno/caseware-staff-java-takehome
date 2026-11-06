# AI usage

## How I used it

I used an AI assistant for drafting, implementation and a separate adversarial review pass. The design decisions and the final numbers were checked by hand. A second model reviewed the document and the code against the original prompt.

## Where it helped

- Converting the prompt into constraints and arithmetic.
- Drafting the design document, the worker and the tests.
- Generating the diagrams and the repetitive test scaffolding.
- Finding counterexamples in the version graph and the failure paths.

## Where I corrected or rejected it

- The first cost estimate for per-engagement summaries used four weeks per month and produced USD 64,000. I recalculated it with 52/12 and got USD 69,333.
- The first DLQ replay released the claim but did not rewind the cursor. The review caught it, and I changed it to a direct per-engagement reprocess.
- The first per-event lock removed its entry from a map and had a race. I replaced it with a fixed set of 256 striped locks.
- The first worker did not pass an idempotency key or a timeout to the downstream. I added both to the contract.
- I rejected per-engagement summaries and reading engagements for the indicator because the cost and the one-minute load do not fit the requirements.

## Where I would not trust AI

- Materiality, regulatory impact and compliance conclusions.
- Data residency and legal requirements.
- Cost estimates without arithmetic.
- Any destructive action on data or Git.

## What the reviewer should verify

The design document distinguishes facts from assumptions and lists the open questions. The most important assumptions are the metadata projection, the downstream quota, one template per engagement, and the product decision to offer only the head.
