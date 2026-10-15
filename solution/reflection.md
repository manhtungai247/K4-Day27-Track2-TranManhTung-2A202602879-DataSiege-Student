# Reflection

The hardest issues for me to catch were the subtle shifts in features and embeddings. Their measurements can sit close to normal variation, so setting a threshold too low risks creating false alarms. Lineage faults were tricky for a different reason: a job can finish successfully even when an important upstream dependency is missing.

I used one tool call for each event and relied on the published baselines and event metadata for the decision. In Practice, the detector caught all 33 faults with no false alarms and used 180 of 220 credits. In Public, it caught 97.44% of faults with no false alarms, while spending 240 credits against a budget of 220. The Private run scored 32.41, with a 64.81% TPR, 0% FPR, and no budget overage. That result showed me that conservative thresholds kept false alarms and costs under control, but still missed some subtle faults.

With another pass, I would use `ctx.state` to track recent trends and compare them with the published baselines before changing any thresholds. I would first check proposed changes against clean data, since a small gain in detection is not worth a large increase in false alarms.
