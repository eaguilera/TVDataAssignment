## Task 2 – Broadcast Schedule Overlap Detection

### Objective
Identify overlapping broadcast schedules occurring on the same tenant and channel,
calculate the overlap duration in minutes, and flag severe overlaps where the overlap
exceeds 10 minutes.

---

### Overlap Detection Logic

Two time intervals overlap if and only if:

A.start_time < B.end_time  
AND  
B.start_time < A.end_time  

This condition correctly handles:
- Partial overlaps
- Full containment
- Edge-aligned overlaps (non-overlapping if end == start)

---

### Deduplication Strategy

To avoid:
- Self-matching
- Duplicate symmetric pairs (A-B and B-A)

To be enforced:
a.schedule < b.schedule_id

