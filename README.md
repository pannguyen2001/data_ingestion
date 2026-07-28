# Data ingestion
- Created on: 2026-July-23

Batch load, streaming load

cdc, watermark, meta data,.. streaming data,... file -> fulload, incremental load -> capture chage -> log -> report -> push to db 

Scenario: I have a file data, now i import first time -> load full -> from second time -> load incremental -> capture change -> date time, change type: del, create, update, col change, value change,...
-> report folder báed on datetime: reports/YYYY-MM-DD/YYYY-MM-DD_HH-MM-SS

# Setup
```
# If not have pyproject.toml
uv init
# Else
uv sync
# Add package
uv add <package_name>
# activate venv
source .venv/bin/activate
```

data/v1: origin batch data
data/v2: new data -> need load change cand capture, if not change, ignore

# Aim:
- Learn Data ingestion
- Handle million rows of records with low latency, fast processing, reliable, matainable, extendable, easy to debug, cllear error and alert

future: create data/vN to run and test
Add to data pipeline

source -> collect (crawling, download,...) -> save to staging -> group data by pattern (if need) -> transfer to parquet orsql file type (done)

(current) -> full load if checkpoint is empty, incremental load if checkpoint is not empty (checkpoint is the mark to detect that first load or not) -> batch prcoessing: load incremental, cdc,... -> log -> report detail -> update data to raw


# Task need do
Check first load or not (based on which: data file path? checkpoint?..)
Mo phong lai load 2 ngay khac nhau
If first load, load origin data to raw and capture history
If not first load, load new data to raw and capture history
When load: Capture:
- New change
- Update/Merge
- Deletion
- Report change, data

Requirement:
- Keep data quality
- No duplicate
- Recover
- Fallback
- Clear error and alert for debug
- ACID ??
- Adapt change

Future: run generation many data version -> load and capture -> increasing amount of data -> optimize -> add checkpoint and log, orchestration, monitoring, senfd alert to mail/slack/discord -> multiple file, multiple source, connect cloud -> streaming



AI reference:
- Kimi: https://www.kimi.com/chat/19fa77f7-b842-8452-8000-098a16fc21fe?chat_enter_method=new_chat
- Claude: https://claude.ai/chat/5e249bd8-1e09-48d0-bdf3-ed2a2d323612
- Chat GPT: https://chatgpt.com/c/6a684f47-a408-83ec-96b8-2cfc356a238b
- Gemini: https://gemini.google.com/app/ea8f7a611e2c0ddf?is_sa=1&is_sa=1&android-min-version=301356232&ios-min-version=322.0&campaign_id=bkws&utm_source=sem&utm_medium=paid-media&utm_campaign=bkws&pt=9008&mt=8&ct=p-growth-sem-bkws&gclsrc=aw.ds&gad_source=1&gad_campaignid=22165684207&gbraid=0AAAAApk5Bhl299pKiraqyznxZcohsqg23&gclid=CjwKCAjwwJzPBhBREiwAJfHRnSYYQdtbQE4xMyI14y4F37Ur_kccjkAOOPAWn3GM336Q6T5avZ2jdRoCNSUQAvD_BwE


Kimi response: The source:
- "Designing Data-Intensive Applications" (Martin Kleppmann), Chapters 10 (Batch Processing) and 11 (Stream Processing). This is the canonical text for the mental model you are missing.
- Polars User Guide — Lazy API: https://docs.pola.rs/user-guide/lazy/ (essential for production scale)
- Delta Lake Protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md (understand the atomicity and versioning concepts, even if you do not use Spark)
- "The Data Warehouse Toolkit" (Kimball & Ross), Chapter 2 (SCD Types). The authoritative source on slowly changing dimensions.


Chat GPT:
How I would study for this project

Don't read everything first and then code. Iterate between theory and implementation:

1. Pick one checkpoint (for example, Load Strategy).
2. Read 2–3 hours from the recommended sources.
3. Implement only that concept in your project.
4. Review your implementation and compare it with how production systems solve the same problem.
5. Write a short design note explaining why you chose that approach and its trade-offs.
Move to the next checkpoint.

That learning loop matches your goal of deeply understanding ingestion architecture rather than accumulating isolated knowledge. It also builds a portfolio project where every module is backed by a solid theoretical foundation instead of being just a coding exercise.

Summarize my knowledge to file -> review and implement
Code to practice -> review implement
Make sure that all fine before next chapter

Suitable for my current level, align to my requirement, carrer path and evolution, not over enginering or tell something too complicated over my current level or not related to current topic.
```

How to check that AI response correct or not
- say it self check, align to requirement from past to now -> self fix, self implement and say loudly if error for me
- or copy it's response an let it self judge it response, find issue and update -> loop 3 times
- show step by step reasoning
- cross check among multiple model for one question -> loop as above or copy response of model a to model b check
- keep subscision, not believe AI's first response, need review carefully before continue

```
re read all your response, self check and comare to our requirement, self find issue in your error and fix it, then response your final result, to make sure that all thing you give is correct. If self check any issue, say loudly for me.

from which you self realize and fix, re check and do this requirement, export as new specification for me:
###
which knowledge i need to research and understand as base foundation theory to do this project? Give me 5 best existed sources for each detail checkpoint, follow our resource requirement from past to now
###

Keep it align with my requirement and ability now, no over engineering or over my level now. Guide me to reach next career level, not make it hard to reach.

```