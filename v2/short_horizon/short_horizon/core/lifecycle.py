from __future__ import annotations


def compute_lifecycle_fraction(*, start_time_ms: int, end_time_ms: int, event_time_ms: int) -> float | None:
    duration_ms = end_time_ms - start_time_ms
    if duration_ms <= 0:
        return None
    return (event_time_ms - start_time_ms) / duration_ms


def is_in_bucket(*, fraction: float | None, bucket_start: float, bucket_end: float) -> bool:
    if fraction is None:
        return False
    return bucket_start <= fraction < bucket_end
