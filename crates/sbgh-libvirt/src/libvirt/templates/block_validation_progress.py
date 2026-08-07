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
