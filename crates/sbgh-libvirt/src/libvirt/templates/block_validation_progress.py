from collections import deque
import math
import re


PROGRESS_PATTERN = re.compile(
    rb"Validating:\s*([0-9]{1,3})%\s*\(\s*([0-9]{1,20})\s*/\s*([0-9]{1,20})\s*\)"
)
MAX_PROGRESS_PENDING_BYTES = 4096


def parse_progress_chunk(pending, chunk, expected_total):
    """Return a bounded parse tail and the greatest valid live counter."""
    combined = pending + chunk
    greatest = None
    for match in PROGRESS_PATTERN.finditer(combined):
        percentage, current, total = (int(value) for value in match.groups())
        if percentage > 100 or total != expected_total or current > total:
            continue
        greatest = current if greatest is None else max(greatest, current)
    return combined[-MAX_PROGRESS_PENDING_BYTES:], greatest


def aggregate_block_progress(finished_ranges, active_counts, trusted_total):
    """Combine completed ranges and live command counters by block weight."""
    finished = sum(end - start + 1 for start, end in finished_ranges)
    active = sum(max(0, count) for count in active_counts)
    return min(trusted_total, finished + active)


class ValidationRate:
    """Estimate remaining processing time only for a stable active command set."""

    def __init__(self):
        """Keep at most sixty one-second intervals, excluding initialization."""
        self.samples = deque(maxlen=61)
        self.cohort = None

    def estimate(self, now, current, total, cohort, ready):
        """Return seconds or None until advances span ten stable seconds."""
        if not ready or cohort != self.cohort:
            self.samples.clear()
            self.cohort = cohort
        if not ready or current >= total:
            return None
        if self.samples and (now < self.samples[-1][0] or current < self.samples[-1][1]):
            self.samples.clear()
        if not self.samples or now - self.samples[-1][0] >= 1:
            self.samples.append((now, current))
        if len(self.samples) < 4 or now - self.samples[0][0] < 10:
            return None
        advances = [
            right[0]
            for left, right in zip(self.samples, list(self.samples)[1:])
            if right[1] > left[1]
        ]
        if len(advances) < 3 or now - advances[-1] >= 10:
            return None
        first, middle, last = self.samples[0], self.samples[len(self.samples) // 2], self.samples[-1]
        early = (middle[1] - first[1]) / (middle[0] - first[0])
        late = (last[1] - middle[1]) / (last[0] - middle[0])
        if early <= 0 or not 0.5 <= late / early <= 2:
            return None
        rate = (last[1] - first[1]) / (last[0] - first[0])
        return math.ceil((total - current) / rate)


def validation_progress(current, total, completed_shards, shard_count, active, pending, rate, now):
    """Describe observable processing separately from unmeasured command setup."""
    initializing = sum(state["current_command_progress"] == 0 for state in active.values())
    cohort = tuple(sorted(
        (index, state["current_command"]["ordinal"])
        for index, state in active.items()
    ))
    ready = bool(active) and not initializing and not pending and all(
        not state["pending_commands"] for state in active.values()
    )
    eta = rate.estimate(now, current, total, cohort, ready)
    message = f"shards {completed_shards}/{shard_count} complete"
    if initializing:
        message += f"; {initializing} initializing or waiting for processed-block progress"
    if pending:
        message += f"; {len(pending)} pending"
    if eta is not None:
        message += f"; processing ETA ~{eta}s (excludes teardown)"
    return {
        "phase": "initialize" if current == 0 and initializing else "validate",
        "current": current,
        "total": total,
        "message": message,
    }
