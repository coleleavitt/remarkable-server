//! VTIMEZONE components (RFC 5545 section 3.6.5): the UTC offset a zone defined in the same
//! iCalendar object has at a given local time, so `TZID=` times can be stored in UTC without a
//! time zone database. CalDAV servers must include a VTIMEZONE for every TZID their calendar
//! data uses (RFC 4791 section 4.1).

use chrono::{Days, Duration, NaiveDateTime};

use super::recurrence::{Exhausted, Rule};

/// A STANDARD or DAYLIGHT observance: from each onset on, the zone is `offset_to` from UTC.
#[derive(Debug, Default)]
pub(super) struct Observance {
    /// The first onset, in the local time in effect before it (`offset_from`).
    start: Option<NaiveDateTime>,
    /// Seconds east of UTC before an onset.
    offset_from: Option<i32>,
    /// Seconds east of UTC from an onset on.
    offset_to: Option<i32>,
    /// Later onsets; `None` without an RRULE or with one outside the supported subset, when
    /// only `start` and `rdates` count.
    rule: Option<Rule>,
    rdates: Vec<NaiveDateTime>,
}

/// `+0100`, `-0500`, `+053000` (and `+01:00`) as seconds east of UTC.
fn parse_offset(value: &str) -> Option<i32> {
    let value = value.trim().replace(':', "");
    let (sign, digits) = match value.as_bytes().first()? {
        b'+' => (1, &value[1..]),
        b'-' => (-1, &value[1..]),
        _ => return None,
    };
    if !matches!(digits.len(), 4 | 6) || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let field = |i: usize| digits.get(i..i + 2).and_then(|d| d.parse::<i32>().ok());
    let (hours, minutes) = (field(0)?, field(2)?);
    let seconds = if digits.len() == 6 { field(4)? } else { 0 };
    (hours < 24 && minutes < 60 && seconds < 60)
        .then(|| sign * (hours * 3600 + minutes * 60 + seconds))
}

fn parse_local(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value.trim(), "%Y%m%dT%H%M%S").ok()
}

impl Observance {
    /// Record one property of the observance (`name` upper-cased, parameters stripped).
    pub(super) fn set(&mut self, name: &str, value: &str) {
        match name {
            "DTSTART" => self.start = parse_local(value),
            "TZOFFSETFROM" => self.offset_from = parse_offset(value),
            "TZOFFSETTO" => self.offset_to = parse_offset(value),
            "RRULE" => self.rule = Rule::parse(value),
            "RDATE" => self.rdates.extend(value.split(',').filter_map(parse_local)),
            _ => {}
        }
    }

    /// The latest onset at or before `local`. Each period an RRULE walks costs one unit of
    /// `budget`.
    fn last_onset(
        &self,
        local: NaiveDateTime,
        budget: &mut usize,
    ) -> Result<Option<NaiveDateTime>, Exhausted> {
        let Some(start) = self.start else {
            return Ok(None);
        };
        // Sorted when the observance was added.
        let listed = self.rdates.partition_point(|d| *d <= local);
        let mut last = listed.checked_sub(1).map(|i| self.rdates[i]);
        if start > local {
            return Ok(last);
        }
        last = last.max(Some(start));
        if let Some(rule) = &self.rule {
            let before = Duration::seconds(self.offset_from.unwrap_or(0).into());
            // Walking from two years before `local` (or before a rule's end) finds any yearly
            // onset.
            let horizon = rule
                .until_date()
                .filter(|until| *until < local.date())
                .unwrap_or(local.date());
            let from = horizon.checked_sub_days(Days::new(800));
            rule.walk(start, from, local, budget, |onset| {
                let utc = onset.checked_sub_signed(before).unwrap_or(onset);
                if onset > local || !rule.until_allows(onset, utc) {
                    return false;
                }
                last = last.max(Some(onset));
                true
            })?;
        }
        Ok(last)
    }
}

/// A VTIMEZONE's observances.
#[derive(Debug, Default)]
pub(super) struct Zone {
    observances: Vec<Observance>,
}

impl Zone {
    pub(super) fn add(&mut self, mut observance: Observance) {
        if observance.start.is_some() && observance.offset_to.is_some() {
            observance.rdates.sort_unstable();
            self.observances.push(observance);
        }
    }

    /// Seconds east of UTC at wall-clock time `local`: the offset of the latest onset at or
    /// before it, or the offset the earliest onset changes from when `local` precedes them
    /// all. `None` when the zone has no usable observance. Each observance, and each period
    /// its RRULE walks, costs one unit of `budget`.
    pub(super) fn offset_at(
        &self,
        local: NaiveDateTime,
        budget: &mut usize,
    ) -> Result<Option<i32>, Exhausted> {
        let mut latest: Option<(NaiveDateTime, i32)> = None;
        for o in &self.observances {
            *budget = budget.checked_sub(1).ok_or(Exhausted)?;
            if let (Some(onset), Some(offset)) = (o.last_onset(local, budget)?, o.offset_to) {
                if latest.is_none_or(|(at, _)| onset > at) {
                    latest = Some((onset, offset));
                }
            }
        }
        Ok(match latest {
            Some((_, offset)) => Some(offset),
            None => self
                .observances
                .iter()
                .min_by_key(|o| o.start)
                .and_then(|o| o.offset_from.or(o.offset_to)),
        })
    }

    /// `local` in this zone as UTC; `None` when the zone has no usable observance.
    pub(super) fn to_utc(
        &self,
        local: NaiveDateTime,
        budget: &mut usize,
    ) -> Result<Option<NaiveDateTime>, Exhausted> {
        Ok(self
            .offset_at(local, budget)?
            .and_then(|offset| local.checked_sub_signed(Duration::seconds(offset.into()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> NaiveDateTime {
        parse_local(s).unwrap()
    }

    /// Offset lookups with a budget that does not run out here.
    trait Lookups {
        fn offset(&self, local: NaiveDateTime) -> Option<i32>;
        fn utc(&self, local: NaiveDateTime) -> Option<NaiveDateTime>;
    }

    impl Lookups for Zone {
        fn offset(&self, local: NaiveDateTime) -> Option<i32> {
            self.offset_at(local, &mut 1_000).unwrap()
        }

        fn utc(&self, local: NaiveDateTime) -> Option<NaiveDateTime> {
            self.to_utc(local, &mut 1_000).unwrap()
        }
    }

    fn observance(lines: &[(&str, &str)]) -> Observance {
        let mut o = Observance::default();
        for (name, value) in lines {
            o.set(name, value);
        }
        o
    }

    /// Europe/Berlin as Outlook, Google and SabreDAV write it.
    fn berlin() -> Zone {
        let mut zone = Zone::default();
        zone.add(observance(&[
            ("TZOFFSETFROM", "+0100"),
            ("TZOFFSETTO", "+0200"),
            ("DTSTART", "19810329T020000"),
            ("RRULE", "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"),
        ]));
        zone.add(observance(&[
            ("TZOFFSETFROM", "+0200"),
            ("TZOFFSETTO", "+0100"),
            ("DTSTART", "19961027T030000"),
            ("RRULE", "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"),
        ]));
        zone
    }

    #[test]
    fn offsets_follow_daylight_saving_rules() {
        let zone = berlin();
        assert_eq!(zone.offset(at("20260105T090000")), Some(3600));
        // 2026: summer time from Sunday 29 March to Sunday 25 October.
        assert_eq!(zone.offset(at("20260329T015959")), Some(3600));
        assert_eq!(zone.offset(at("20260329T030000")), Some(7200));
        assert_eq!(zone.offset(at("20260706T090000")), Some(7200));
        assert_eq!(zone.offset(at("20261025T040000")), Some(3600));
        assert_eq!(zone.utc(at("20260706T090000")), Some(at("20260706T070000")));
        // Before every onset: the offset the first one changes from.
        assert_eq!(zone.offset(at("19700101T000000")), Some(3600));
    }

    #[test]
    fn rules_that_ended_and_fixed_onsets() {
        // US Eastern: the pre-2007 rule ends with UNTIL (in UTC), the current one takes over.
        let mut eastern = Zone::default();
        for (from, to, start, rule) in [
            (
                "-0500",
                "-0400",
                "19870405T020000",
                "FREQ=YEARLY;BYMONTH=4;BYDAY=1SU;UNTIL=20060402T070000Z",
            ),
            (
                "-0400",
                "-0500",
                "19671029T020000",
                "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL=20061029T060000Z",
            ),
            (
                "-0500",
                "-0400",
                "20070311T020000",
                "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
            ),
            (
                "-0400",
                "-0500",
                "20071104T020000",
                "FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
            ),
        ] {
            eastern.add(observance(&[
                ("TZOFFSETFROM", from),
                ("TZOFFSETTO", to),
                ("DTSTART", start),
                ("RRULE", rule),
            ]));
        }
        // 2006: the old rule's last year, with its last onsets exactly at UNTIL.
        assert_eq!(eastern.offset(at("20060401T120000")), Some(-5 * 3600));
        assert_eq!(eastern.offset(at("20060402T120000")), Some(-4 * 3600));
        assert_eq!(eastern.offset(at("20061030T120000")), Some(-5 * 3600));
        // 2026: the new rule (8 March to 1 November).
        assert_eq!(eastern.offset(at("20260307T120000")), Some(-5 * 3600));
        assert_eq!(eastern.offset(at("20260310T120000")), Some(-4 * 3600));
        assert_eq!(eastern.offset(at("20261102T120000")), Some(-5 * 3600));

        // Summer time kept for good (Turkey, 2016): both rules end, and the last onset of
        // either still decides years later.
        let mut kept = Zone::default();
        for (from, to, start, rule) in [
            (
                "+0200",
                "+0300",
                "19810329T030000",
                "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20160327T010000Z",
            ),
            (
                "+0300",
                "+0200",
                "19961027T040000",
                "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL=20151025T010000Z",
            ),
        ] {
            kept.add(observance(&[
                ("TZOFFSETFROM", from),
                ("TZOFFSETTO", to),
                ("DTSTART", start),
                ("RRULE", rule),
            ]));
        }
        assert_eq!(kept.offset(at("20151201T000000")), Some(2 * 3600));
        assert_eq!(kept.offset(at("20260105T090000")), Some(3 * 3600));

        // No daylight saving time: one onset, no rule.
        let mut kolkata = Zone::default();
        kolkata.add(observance(&[
            ("TZOFFSETFROM", "+0530"),
            ("TZOFFSETTO", "+0530"),
            ("DTSTART", "19700101T000000"),
        ]));
        assert_eq!(
            kolkata.utc(at("20260105T090000")),
            Some(at("20260105T033000"))
        );
        // Onsets listed with RDATE.
        let mut listed = Zone::default();
        listed.add(observance(&[
            ("TZOFFSETFROM", "+0000"),
            ("TZOFFSETTO", "+0300"),
            ("DTSTART", "20000101T000000"),
            ("RDATE", "20200601T000000,20240601T000000"),
        ]));
        listed.add(observance(&[
            ("TZOFFSETFROM", "+0300"),
            ("TZOFFSETTO", "+0100"),
            ("DTSTART", "20100101T000000"),
            ("RDATE", "20220101T000000"),
        ]));
        assert_eq!(listed.offset(at("20210101T000000")), Some(3 * 3600));
        assert_eq!(listed.offset(at("20230101T000000")), Some(3600));
        assert_eq!(listed.offset(at("20250101T000000")), Some(3 * 3600));
    }

    #[test]
    fn offsets_parse() {
        assert_eq!(parse_offset("+0100"), Some(3600));
        assert_eq!(parse_offset("-0330"), Some(-12600));
        assert_eq!(parse_offset("+053010"), Some(19810));
        assert_eq!(parse_offset("+01:00"), Some(3600));
        for bad in ["", "0100", "+1", "+2500", "+0160", "+01000", "+ab00"] {
            assert_eq!(parse_offset(bad), None, "{}", bad);
        }
    }

    #[test]
    fn lookups_are_charged_to_the_budget() {
        let zone = berlin();
        let mut budget = 1_000;
        assert_eq!(
            zone.offset_at(at("20260706T090000"), &mut budget),
            Ok(Some(7200))
        );
        // Two observances, each skipping ahead to a few yearly periods.
        let cost = 1_000 - budget;
        assert!((2..=16).contains(&cost), "{}", cost);
        assert_eq!(
            zone.offset_at(at("20260706T090000"), &mut 3),
            Err(Exhausted)
        );
        // Listed onsets are found without walking through them.
        let mut listed = Zone::default();
        let rdates: Vec<String> = (0..10_000)
            .map(|i| format!("{}0101T000000", 2000 + i % 8000))
            .collect();
        listed.add(observance(&[
            ("TZOFFSETFROM", "+0000"),
            ("TZOFFSETTO", "+0100"),
            ("DTSTART", "19990101T000000"),
            ("RDATE", &rdates.join(",")),
        ]));
        let mut budget = 1;
        assert_eq!(
            listed.offset_at(at("20260706T090000"), &mut budget),
            Ok(Some(3600))
        );
        assert_eq!(budget, 0);
    }

    #[test]
    fn an_unusable_zone_has_no_offset() {
        let mut zone = Zone::default();
        zone.add(observance(&[("TZOFFSETTO", "+0100")]));
        assert_eq!(zone.offset(at("20260105T090000")), None);
    }
}
