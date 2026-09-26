//! RFC 5545 recurrence rules (RRULE), for the subset calendars use in practice: `FREQ` of
//! `DAILY`, `WEEKLY`, `MONTHLY` or `YEARLY` with `INTERVAL`, `COUNT`, `UNTIL`, `BYMONTH`,
//! `BYMONTHDAY`, `BYDAY` and `WKST`.
//!
//! A rule with any other part (`BYSETPOS`, `BYWEEKNO`, `BYYEARDAY`, `BYHOUR`, ...) or a
//! sub-daily frequency does not parse, and callers keep the event as its first occurrence.
//! The same engine walks the onsets of VTIMEZONE observances.

use chrono::{Datelike, Days, Months, NaiveDate, NaiveDateTime, Weekday};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Freq {
    Daily,
    Weekly,
    Monthly,
    Yearly,
}

/// `UNTIL`: a UTC date-time (`Z`), a local date-time, or a date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Until {
    Utc(NaiveDateTime),
    Local(NaiveDateTime),
    Date(NaiveDate),
}

/// A parsed RRULE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Rule {
    freq: Freq,
    interval: u32,
    count: Option<u32>,
    until: Option<Until>,
    by_month: Vec<u32>,
    by_month_day: Vec<i32>,
    /// `(ordinal, weekday)`; ordinal 0 means every such weekday of the period.
    by_day: Vec<(i32, Weekday)>,
    wkst: Weekday,
}

/// The period budget ran out before the walk ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Exhausted;

fn weekday(code: &str) -> Option<Weekday> {
    Some(match code {
        "MO" => Weekday::Mon,
        "TU" => Weekday::Tue,
        "WE" => Weekday::Wed,
        "TH" => Weekday::Thu,
        "FR" => Weekday::Fri,
        "SA" => Weekday::Sat,
        "SU" => Weekday::Sun,
        _ => return None,
    })
}

fn list<T>(value: &str, item: impl Fn(&str) -> Option<T>) -> Option<Vec<T>> {
    value.split(',').map(|v| item(v.trim())).collect()
}

fn parse_until(value: &str) -> Option<Until> {
    let value = value.trim();
    if let Some(utc) = value.strip_suffix('Z') {
        return NaiveDateTime::parse_from_str(utc, "%Y%m%dT%H%M%S")
            .ok()
            .map(Until::Utc);
    }
    if let Ok(local) = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S") {
        return Some(Until::Local(local));
    }
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .ok()
        .map(Until::Date)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let first = NaiveDate::from_ymd_opt(year, month, 1);
    let next = first.and_then(|d| d.checked_add_months(Months::new(1)));
    match (first, next) {
        (Some(first), Some(next)) => (next - first).num_days() as u32,
        _ => 0,
    }
}

/// Day of the month of the `n`th (from the end when negative) `weekday` of the month.
fn nth_weekday(year: i32, month: u32, n: i32, weekday: Weekday) -> Option<u32> {
    let len = days_in_month(year, month);
    let first = NaiveDate::from_ymd_opt(year, month, 1)?.weekday();
    let first_match = 1 + weekday.days_since(first);
    let matches = (len.checked_sub(first_match)? / 7) + 1;
    let index = match n {
        1.. => u32::try_from(n).ok()?.checked_sub(1)?,
        ..=-1 => matches.checked_sub(n.unsigned_abs())?,
        0 => return None,
    };
    (index < matches).then(|| first_match + 7 * index)
}

impl Rule {
    /// `None` for a malformed rule or one outside the supported subset.
    pub(super) fn parse(rule: &str) -> Option<Rule> {
        let mut freq = None;
        let mut parsed = Rule {
            freq: Freq::Daily,
            interval: 1,
            count: None,
            until: None,
            by_month: Vec::new(),
            by_month_day: Vec::new(),
            by_day: Vec::new(),
            wkst: Weekday::Mon,
        };
        for part in rule.trim().split(';').filter(|p| !p.trim().is_empty()) {
            let (key, value) = part.split_once('=')?;
            let value = value.trim().to_ascii_uppercase();
            match key.trim().to_ascii_uppercase().as_str() {
                "FREQ" => {
                    freq = Some(match value.as_str() {
                        "DAILY" => Freq::Daily,
                        "WEEKLY" => Freq::Weekly,
                        "MONTHLY" => Freq::Monthly,
                        "YEARLY" => Freq::Yearly,
                        _ => return None,
                    })
                }
                "INTERVAL" => parsed.interval = value.parse().ok().filter(|n| *n >= 1)?,
                "COUNT" => parsed.count = Some(value.parse().ok()?),
                "UNTIL" => parsed.until = Some(parse_until(&value)?),
                "BYMONTH" => {
                    parsed.by_month =
                        list(&value, |m| m.parse().ok().filter(|m| (1..=12).contains(m)))?
                }
                "BYMONTHDAY" => {
                    parsed.by_month_day = list(&value, |d| {
                        d.parse::<i32>()
                            .ok()
                            .filter(|d| *d != 0 && (-31..=31).contains(d))
                    })?
                }
                "BYDAY" => {
                    parsed.by_day = list(&value, |d| {
                        let split = d.len().checked_sub(2)?;
                        let (ordinal, day) = (d.get(..split)?, d.get(split..)?);
                        let ordinal = match ordinal {
                            "" => 0,
                            n => n
                                .trim_start_matches('+')
                                .parse::<i32>()
                                .ok()
                                .filter(|n| *n != 0 && (-53..=53).contains(n))?,
                        };
                        Some((ordinal, weekday(day)?))
                    })?
                }
                "WKST" => parsed.wkst = weekday(&value)?,
                _ => return None,
            }
        }
        parsed.freq = freq?;
        let ordinals = parsed.by_day.iter().any(|(n, _)| *n != 0);
        let supported = match parsed.freq {
            // Ordinals only mean something within a month or year.
            Freq::Daily => !ordinals,
            Freq::Weekly => !ordinals && parsed.by_month_day.is_empty(),
            Freq::Monthly => true,
            // Weekdays counted across the whole year (`BYDAY=20MO`) are not implemented.
            Freq::Yearly => parsed.by_day.is_empty() || !parsed.by_month.is_empty(),
        };
        supported.then_some(parsed)
    }

    pub(super) fn has_count(&self) -> bool {
        self.count.is_some()
    }

    /// The date of `UNTIL`, if the rule has one.
    pub(super) fn until_date(&self) -> Option<NaiveDate> {
        self.until.map(|until| match until {
            Until::Utc(at) | Until::Local(at) => at.date(),
            Until::Date(date) => date,
        })
    }

    /// Whether an occurrence starting at `local` (`utc` in UTC) is within `UNTIL`.
    pub(super) fn until_allows(&self, local: NaiveDateTime, utc: NaiveDateTime) -> bool {
        match self.until {
            None => true,
            Some(Until::Utc(until)) => utc <= until,
            Some(Until::Local(until)) => local <= until,
            Some(Until::Date(until)) => local.date() <= until,
        }
    }

    fn month_matches(&self, date: NaiveDate) -> bool {
        self.by_month.is_empty() || self.by_month.contains(&date.month())
    }

    /// Days of `year`-`month` the rule selects; `default_day` when it names none.
    fn days_of_month(&self, year: i32, month: u32, default_day: u32) -> Vec<u32> {
        let len = days_in_month(year, month) as i32;
        let month_days = (!self.by_month_day.is_empty()).then(|| {
            self.by_month_day
                .iter()
                .map(|d| if *d < 0 { len + 1 + d } else { *d })
                .filter(|d| (1..=len).contains(d))
                .map(|d| d as u32)
                .collect::<Vec<_>>()
        });
        let weekdays = (!self.by_day.is_empty()).then(|| {
            let mut days = Vec::new();
            for (n, wd) in &self.by_day {
                if *n != 0 {
                    days.extend(nth_weekday(year, month, *n, *wd));
                } else {
                    days.extend((1..=5).filter_map(|n| nth_weekday(year, month, n, *wd)));
                }
            }
            days
        });
        let mut days = match (month_days, weekdays) {
            (None, None) => vec![default_day]
                .into_iter()
                .filter(|d| *d as i32 <= len)
                .collect(),
            (Some(days), None) | (None, Some(days)) => days,
            (Some(a), Some(b)) => a.into_iter().filter(|d| b.contains(d)).collect(),
        };
        days.sort_unstable();
        days.dedup();
        days
    }

    /// The first day of period `k` and the dates the rule selects in it, in order. `None` once
    /// the dates leave chrono's range.
    fn period(&self, anchor: NaiveDate, k: u64) -> Option<(NaiveDate, Vec<NaiveDate>)> {
        let step = k.checked_mul(u64::from(self.interval))?;
        match self.freq {
            Freq::Daily => {
                let day = anchor.checked_add_days(Days::new(step))?;
                let selected = self.month_matches(day)
                    && (self.by_month_day.is_empty()
                        || self
                            .days_of_month(day.year(), day.month(), 0)
                            .contains(&day.day()))
                    && (self.by_day.is_empty()
                        || self.by_day.iter().any(|(_, wd)| *wd == day.weekday()));
                Some((day, if selected { vec![day] } else { Vec::new() }))
            }
            Freq::Weekly => {
                let week0 = anchor
                    .checked_sub_days(Days::new(anchor.weekday().days_since(self.wkst).into()))?;
                let week = week0.checked_add_days(Days::new(step.checked_mul(7)?))?;
                let mut days: Vec<Weekday> = self.by_day.iter().map(|(_, wd)| *wd).collect();
                if days.is_empty() {
                    days.push(anchor.weekday());
                }
                let mut dates: Vec<NaiveDate> = days
                    .into_iter()
                    .filter_map(|wd| {
                        week.checked_add_days(Days::new(wd.days_since(self.wkst).into()))
                    })
                    .filter(|d| self.month_matches(*d))
                    .collect();
                dates.sort_unstable();
                dates.dedup();
                Some((week, dates))
            }
            Freq::Monthly => {
                let first = anchor
                    .with_day(1)?
                    .checked_add_months(Months::new(u32::try_from(step).ok()?))?;
                if !self.month_matches(first) {
                    return Some((first, Vec::new()));
                }
                let dates = self
                    .days_of_month(first.year(), first.month(), anchor.day())
                    .into_iter()
                    .filter_map(|d| first.with_day(d))
                    .collect();
                Some((first, dates))
            }
            Freq::Yearly => {
                let year = anchor.year().checked_add(i32::try_from(step).ok()?)?;
                let first = NaiveDate::from_ymd_opt(year, 1, 1)?;
                let months: Vec<u32> = if !self.by_month.is_empty() {
                    let mut months = self.by_month.clone();
                    months.sort_unstable();
                    months.dedup();
                    months
                } else if !self.by_month_day.is_empty() {
                    (1..=12).collect()
                } else {
                    vec![anchor.month()]
                };
                let dates = months
                    .into_iter()
                    .flat_map(|m| {
                        self.days_of_month(year, m, anchor.day())
                            .into_iter()
                            .filter_map(move |d| NaiveDate::from_ymd_opt(year, m, d))
                    })
                    .collect();
                Some((first, dates))
            }
        }
    }

    /// Periods that end before `from`, so a walk without `COUNT` can start after them.
    fn periods_before(&self, anchor: NaiveDate, from: NaiveDate) -> u64 {
        let elapsed = match self.freq {
            Freq::Daily => (from - anchor).num_days(),
            Freq::Weekly => (from - anchor).num_days() / 7,
            Freq::Monthly => {
                i64::from(from.year() - anchor.year()) * 12 + i64::from(from.month())
                    - i64::from(anchor.month())
            }
            Freq::Yearly => i64::from(from.year() - anchor.year()),
        };
        // One period of slack for the week and month boundaries.
        u64::try_from(elapsed / i64::from(self.interval) - 1).unwrap_or(0)
    }

    /// Call `visit` with each occurrence's local start, in order, starting with `start`
    /// (DTSTART, always the first occurrence), until `visit` returns false or the periods pass
    /// `end`. `UNTIL` is left to `visit` (see [`Rule::until_allows`]), as it may need the
    /// occurrence in UTC. Without `COUNT`, periods that end before `from` are skipped (and
    /// `start` with them). Each period costs one unit of `budget`.
    pub(super) fn walk(
        &self,
        start: NaiveDateTime,
        from: Option<NaiveDate>,
        end: NaiveDateTime,
        budget: &mut usize,
        mut visit: impl FnMut(NaiveDateTime) -> bool,
    ) -> Result<(), Exhausted> {
        let (anchor, time) = (start.date(), start.time());
        let first = match (self.count, from) {
            (None, Some(from)) => self.periods_before(anchor, from),
            _ => 0,
        };
        let mut produced = 0u32;
        if first == 0 {
            produced = 1;
            if !visit(start) {
                return Ok(());
            }
        }
        let mut k = first;
        loop {
            if self.count.is_some_and(|count| produced >= count) {
                return Ok(());
            }
            *budget = budget.checked_sub(1).ok_or(Exhausted)?;
            let Some((period_start, dates)) = self.period(anchor, k) else {
                return Ok(());
            };
            if period_start > end.date() {
                return Ok(());
            }
            for date in dates {
                let at = date.and_time(time);
                if at <= start {
                    continue;
                }
                if at > end || self.count.is_some_and(|count| produced >= count) {
                    return Ok(());
                }
                produced += 1;
                if !visit(at) {
                    return Ok(());
                }
            }
            k += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%S").unwrap()
    }

    /// Occurrences from `start` until `end`, within `UNTIL` taken as local time.
    fn occurrences(rule: &str, start: &str, end: &str) -> Vec<String> {
        let rule = Rule::parse(rule).unwrap_or_else(|| panic!("{} does not parse", rule));
        let mut out = Vec::new();
        let mut budget = 10_000;
        rule.walk(at(start), None, at(end), &mut budget, |t| {
            if rule.until_allows(t, t) {
                out.push(t.format("%Y%m%dT%H%M%S").to_string());
                true
            } else {
                false
            }
        })
        .unwrap();
        out
    }

    #[test]
    fn nth_weekday_counts_from_either_end() {
        // March 2026 starts on a Sunday.
        assert_eq!(nth_weekday(2026, 3, 1, Weekday::Sun), Some(1));
        assert_eq!(nth_weekday(2026, 3, 2, Weekday::Sun), Some(8));
        assert_eq!(nth_weekday(2026, 3, -1, Weekday::Sun), Some(29));
        assert_eq!(nth_weekday(2026, 3, 5, Weekday::Sun), Some(29));
        assert_eq!(nth_weekday(2026, 3, 6, Weekday::Sun), None);
        assert_eq!(nth_weekday(2026, 2, -1, Weekday::Sat), Some(28));
        assert_eq!(nth_weekday(2026, 2, 0, Weekday::Sat), None);
    }

    #[test]
    fn daily_with_count_interval_and_until() {
        assert_eq!(
            occurrences("FREQ=DAILY;COUNT=3", "20260105T090000", "20270101T000000"),
            ["20260105T090000", "20260106T090000", "20260107T090000"]
        );
        assert_eq!(
            occurrences(
                "FREQ=DAILY;INTERVAL=2;UNTIL=20260110T090000",
                "20260105T090000",
                "20270101T000000"
            ),
            ["20260105T090000", "20260107T090000", "20260109T090000"]
        );
        // Weekdays only.
        assert_eq!(
            occurrences(
                "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR",
                "20260109T090000",
                "20260113T235959"
            ),
            ["20260109T090000", "20260112T090000", "20260113T090000"]
        );
    }

    #[test]
    fn weekly_by_day_and_week_start() {
        // Monday 2026-01-05.
        assert_eq!(
            occurrences(
                "FREQ=WEEKLY;BYDAY=MO,WE;COUNT=5",
                "20260105T100000",
                "20270101T000000"
            ),
            [
                "20260105T100000",
                "20260107T100000",
                "20260112T100000",
                "20260114T100000",
                "20260119T100000"
            ]
        );
        // RFC 5545's WKST example: the week start decides which weeks an interval skips.
        assert_eq!(
            occurrences(
                "FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=MO",
                "19970805T090000",
                "19980101T000000"
            ),
            [
                "19970805T090000",
                "19970810T090000",
                "19970819T090000",
                "19970824T090000"
            ]
        );
        assert_eq!(
            occurrences(
                "FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=SU",
                "19970805T090000",
                "19980101T000000"
            ),
            [
                "19970805T090000",
                "19970817T090000",
                "19970819T090000",
                "19970831T090000"
            ]
        );
    }

    #[test]
    fn monthly_by_weekday_ordinal_and_month_day() {
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;BYDAY=-1FR;COUNT=3",
                "20260130T080000",
                "20270101T000000"
            ),
            ["20260130T080000", "20260227T080000", "20260327T080000"]
        );
        // The 31st only in months that have one.
        assert_eq!(
            occurrences("FREQ=MONTHLY;COUNT=3", "20260131T080000", "20270101T000000"),
            ["20260131T080000", "20260331T080000", "20260531T080000"]
        );
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;BYMONTHDAY=-1;COUNT=2",
                "20260131T080000",
                "20270101T000000"
            ),
            ["20260131T080000", "20260228T080000"]
        );
        // Friday the 13th.
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;BYDAY=FR;BYMONTHDAY=13;COUNT=2",
                "20260213T000000",
                "20300101T000000"
            ),
            ["20260213T000000", "20260313T000000"]
        );
    }

    #[test]
    fn yearly_rules() {
        assert_eq!(
            occurrences("FREQ=YEARLY;COUNT=3", "20240229T000000", "20400101T000000"),
            ["20240229T000000", "20280229T000000", "20320229T000000"]
        );
        // US daylight saving time start since 2007.
        assert_eq!(
            occurrences(
                "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;COUNT=3",
                "20070311T020000",
                "20400101T000000"
            ),
            ["20070311T020000", "20080309T020000", "20090308T020000"]
        );
    }

    #[test]
    fn skipping_ahead_keeps_the_same_occurrences() {
        let rule = Rule::parse("FREQ=WEEKLY;INTERVAL=3;BYDAY=TU,TH").unwrap();
        let start = at("20200107T093000");
        let end = at("20260601T000000");
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let collect = |skip: Option<NaiveDate>| {
            let mut out = Vec::new();
            let mut budget = 10_000;
            rule.walk(start, skip, end, &mut budget, |t| {
                out.push(t);
                true
            })
            .unwrap();
            let left = budget;
            (
                out.into_iter()
                    .filter(|t| t.date() >= from)
                    .collect::<Vec<_>>(),
                left,
            )
        };
        let (all, full_left) = collect(None);
        let (skipped, skipped_left) = collect(Some(from));
        assert_eq!(all, skipped);
        assert!(!all.is_empty());
        assert!(
            skipped_left > full_left,
            "skipping ahead walks fewer periods"
        );
    }

    #[test]
    fn budget_and_end_bound_the_walk() {
        let rule = Rule::parse("FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30").unwrap();
        let mut budget = 10_000;
        let mut seen = 0;
        rule.walk(
            at("20000101T000000"),
            None,
            at("20300101T000000"),
            &mut budget,
            |_| {
                seen += 1;
                true
            },
        )
        .unwrap();
        // DTSTART only: February never has a 30th, and the walk stops at `end`.
        assert_eq!(seen, 1);
        assert!(budget > 9_900);
        let mut tiny = 3;
        let daily = Rule::parse("FREQ=DAILY").unwrap();
        assert_eq!(
            daily.walk(
                at("20000101T000000"),
                None,
                at("20300101T000000"),
                &mut tiny,
                |_| true
            ),
            Err(Exhausted)
        );
    }

    #[test]
    fn unsupported_rules_do_not_parse() {
        for rule in [
            "FREQ=HOURLY",
            "FREQ=MONTHLY;BYSETPOS=-1;BYDAY=MO,TU,WE,TH,FR",
            "FREQ=YEARLY;BYWEEKNO=20",
            "FREQ=YEARLY;BYDAY=20MO",
            "FREQ=WEEKLY;BYDAY=1MO",
            "FREQ=DAILY;INTERVAL=0",
            "FREQ=DAILY;BYMONTH=13",
            "BYDAY=MO",
            "FREQ=DAILY;UNTIL=soon",
            "FREQ=DAILY;X-NAME=1",
        ] {
            assert_eq!(Rule::parse(rule), None, "{}", rule);
        }
        assert!(Rule::parse("freq=weekly;byday=mo;").is_some());
    }

    proptest::proptest! {
        #[test]
        fn walks_are_ordered_bounded_and_never_panic(
            rule in "FREQ=(DAILY|WEEKLY|MONTHLY|YEARLY)(;INTERVAL=[1-9]{1,3})?(;COUNT=[0-9]{1,4})?(;BYMONTH=(1[0-2]|[1-9]))?(;BYMONTHDAY=-?[1-9]{1,2})?(;BYDAY=(-?[1-5])?(MO|TU|WE|TH|FR|SA|SU))?(;WKST=(MO|SU))?",
            year in 1i32..9999,
        ) {
            if let Some(rule) = Rule::parse(&rule) {
                let start = NaiveDate::from_ymd_opt(year, 1, 31).unwrap().and_hms_opt(9, 0, 0).unwrap();
                let end = NaiveDate::from_ymd_opt(year + 3, 12, 31)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let mut budget = 5_000;
                let mut last = None;
                let _ = rule.walk(start, None, end, &mut budget, |t| {
                    assert!(last.is_none_or(|l| t > l) && t <= end.max(start));
                    last = Some(t);
                    true
                });
            }
        }
    }
}
