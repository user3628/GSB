from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .fixedpoint import SCALE, apply_scale, clamp, damp_toward, ratio_ppm
from .models import QuotaDecision, QuotaGroup, WindowEstimate


@dataclass(slots=True)
class QuotaEngine:
    settings: Settings

    def decide(
        self,
        *,
        group: QuotaGroup,
        previous_scale_ppm: int,
        short: WindowEstimate,
        long: WindowEstimate,
        admitted_short_weight: int,
    ) -> QuotaDecision:
        """Compute a damped quota using both windows and the current admission rate.

        All arithmetic is integer fixed-point. The target multiplier is derived
        from the larger normalized pressure and the admitted rate relative to the
        group's per-second target. The multiplier is then damped toward the prior
        committed multiplier before quotas are materialized.
        """

        short_pressure = ratio_ppm(short.estimated_weight, group.short_limit_weight)
        long_pressure = ratio_ppm(long.estimated_weight, group.long_limit_weight)
        pressure_ppm = max(short_pressure, long_pressure)
        admission_rate_ppm = ratio_ppm(
            admitted_short_weight,
            group.target_weight_per_sec,
            cap=2 * SCALE,
        )

        raw_scale = SCALE + (
            self.settings.quota_adjust_gain_ppm * (admission_rate_ppm - pressure_ppm)
        ) // SCALE
        raw_scale = clamp(
            raw_scale,
            self.settings.quota_min_scale_ppm,
            self.settings.quota_max_scale_ppm,
        )
        damped_scale = damp_toward(
            previous_scale_ppm,
            raw_scale,
            self.settings.quota_damping_ppm,
        )
        damped_scale = clamp(
            damped_scale,
            self.settings.quota_min_scale_ppm,
            self.settings.quota_max_scale_ppm,
        )
        short_quota = apply_scale(
            group.short_limit_weight,
            damped_scale,
            min_scale=self.settings.quota_min_scale_ppm,
            max_scale=self.settings.quota_max_scale_ppm,
        )
        long_quota = apply_scale(
            group.long_limit_weight,
            damped_scale,
            min_scale=self.settings.quota_min_scale_ppm,
            max_scale=self.settings.quota_max_scale_ppm,
        )
        if short_quota < 1 or long_quota < 1:
            raise ValueError("dynamic quota underflow")
        return QuotaDecision(short_quota=short_quota, long_quota=long_quota, scale_ppm=damped_scale)
