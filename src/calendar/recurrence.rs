//! RFC 5545 recurrence rules (RRULE), for the subset calendars use in practice: `FREQ` of
//! `DAILY`, `WEEKLY`, `MONTHLY` or `YEARLY` with `INTERVAL`, `COUNT`, `UNTIL`, `BYMONTH`,
//! `BYMONTHDAY`, `BYDAY`, `BYSETPOS` (Outlook's and Apple's "last weekday of the month") and
//! `WKST`.
//!
//! A rule with any other part (`BYWEEKNO`, `BYYEARDAY`, `BYHOUR`, ...), a sub-daily frequency
//! or a `BYDAY` ordinal beyond 5 (no month has a sixth Monday) does not parse, and callers
//! keep the event as its first occurrence. The same engine walks the onsets of VTIMEZONE
//! observances.
//!
//! Walks are paid for in steps of a budget: each period costs one step plus one per `BY` list
//! entry it goes through, and each date its `BYMONTH`, `BYMONTHDAY` and `BYDAY` parts select
//! one more. The lists are deduplicated when parsed (at most 12 months, 62 month days, 77
//! weekdays and 732 set positions), so a step is a bounded amount of work whatever the rule
//! says.

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

/// A parsed RRULE. The `BY` lists are sorted and hold each value once.
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
    /// `BYSETPOS`: which of the dates a period's other `BY` parts select it keeps, counted
    /// from the end when negative. Each date is one instance, as `BYHOUR` and the like are
    /// not supported.
    by_set_pos: Vec<i32>,
    wkst: Weekday,
}

/// The step budget ran out before the walk ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Exhausted;

/// Take `steps` from `budget`. Once it cannot pay, the budget is spent for good.
pub(super) fn charge(budget: &mut usize, steps: usize) -> Result<(), Exhausted> {
    match budget.checked_sub(steps) {
        Some(left) => {
            *budget = left;
            Ok(())
        }
        None => {
            *budget = 0;
            Err(Exhausted)
        }
    }
}

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

/// The items of a comma-separated list, sorted by `key` and each kept once.
fn list<T, K: Ord>(
    value: &str,
    item: impl Fn(&str) -> Option<T>,
    key: impl Fn(&T) -> K,
) -> Option<Vec<T>> {
    let mut items: Vec<T> = value
        .split(',')
        .map(|v| item(v.trim()))
        .collect::<Option<_>>()?;
    items.sort_unstable_by_key(&key);
    items.dedup_by(|a, b| key(a) == key(b));
    Some(items)
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

/// Days of a month as a bit set: bit `d` stands for day `d`.
type DaySet = u32;

/// The days in `set`, in order.
fn days_in(set: DaySet) -> impl Iterator<Item = u32> {
    (1..=31).filter(move |d| set & (1 << d) != 0)
}

/// A month: how many days it has and the weekday it starts on.
#[derive(Debug, Clone, Copy)]
struct Month {
    len: u32,
    first: Weekday,
}

impl Month {
    /// `None` when the month is outside chrono's range.
    fn of(year: i32, month: u32) -> Option<Month> {
        let first = NaiveDate::from_ymd_opt(year, month, 1)?.weekday();
        let len = match month {
            2 if NaiveDate::from_ymd_opt(year, 2, 29).is_some() => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        Some(Month { len, first })
    }

    fn all(self) -> DaySet {
        (u32::MAX >> (32 - self.len)) << 1
    }

    /// Day `d` of the month, counted from the end when negative.
    fn day(self, d: i32) -> Option<u32> {
        let day = if d < 0 { self.len as i32 + 1 + d } else { d };
        u32::try_from(day)
            .ok()
            .filter(|day| (1..=self.len).contains(day))
    }

    /// The day of the `n`th (from the end when negative) `weekday` of the month.
    fn nth_weekday(self, n: i32, weekday: Weekday) -> Option<u32> {
        let first_match = 1 + weekday.days_since(self.first);
        let matches = (self.len - first_match) / 7 + 1;
        let index = match n {
            1.. => u32::try_from(n).ok()? - 1,
            ..=-1 => matches.checked_sub(n.unsigned_abs())?,
            0 => return None,
        };
        (index < matches).then(|| first_match + 7 * index)
    }

    /// The days that fall on `weekday`.
    fn weekdays(self, weekday: Weekday) -> DaySet {
        let mut days = 0;
        let mut day = 1 + weekday.days_since(self.first);
        while day <= self.len {
            days |= 1 << day;
            day += 7;
        }
        days
    }
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
            by_set_pos: Vec::new(),
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
                    parsed.by_month = list(
                        &value,
                        |m| m.parse().ok().filter(|m| (1..=12).contains(m)),
                        |m| *m,
                    )?
                }
                "BYMONTHDAY" => {
                    parsed.by_month_day = list(
                        &value,
                        |d| {
                            d.parse::<i32>()
                                .ok()
                                .filter(|d| *d != 0 && (-31..=31).contains(d))
                        },
                        |d| *d,
                    )?
                }
                // Ordinals count within a month (see `supported` below), which has at most
                // five of each weekday.
                "BYDAY" => {
                    parsed.by_day = list(
                        &value,
                        |d| {
                            let split = d.len().checked_sub(2)?;
                            let (ordinal, day) = (d.get(..split)?, d.get(split..)?);
                            let ordinal = match ordinal {
                                "" => 0,
                                n => n
                                    .trim_start_matches('+')
                                    .parse::<i32>()
                                    .ok()
                                    .filter(|n| *n != 0 && (-5..=5).contains(n))?,
                            };
                            Some((ordinal, weekday(day)?))
                        },
                        |(n, wd)| (*n, wd.num_days_from_monday()),
                    )?
                }
                "BYSETPOS" => {
                    parsed.by_set_pos = list(
                        &value,
                        |p| {
                            p.trim_start_matches('+')
                                .parse::<i32>()
                                .ok()
                                .filter(|p| *p != 0 && (-366..=366).contains(p))
                        },
                        |p| *p,
                    )?
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
        // BYSETPOS picks from what another BY part selects (RFC 5545 section 3.3.10).
        let selects = !(parsed.by_month.is_empty()
            && parsed.by_month_day.is_empty()
            && parsed.by_day.is_empty());
        let set_pos_ok = parsed.by_set_pos.is_empty() || selects;
        (supported && set_pos_ok).then_some(parsed)
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

    fn month_matches(&self, month: u32) -> bool {
        self.by_month.is_empty() || self.by_month.contains(&month)
    }

    /// Days of `month` the rule selects; `default_day` when it names none.
    fn days_of_month(&self, month: Month, default_day: u32) -> DaySet {
        if self.by_month_day.is_empty() && self.by_day.is_empty() {
            return if (1..=month.len).contains(&default_day) {
                1 << default_day
            } else {
                0
            };
        }
        let mut month_days = if self.by_month_day.is_empty() {
            month.all()
        } else {
            0
        };
        for d in &self.by_month_day {
            month_days |= month.day(*d).map_or(0, |day| 1 << day);
        }
        let mut weekdays = if self.by_day.is_empty() {
            month.all()
        } else {
            0
        };
        for (n, wd) in &self.by_day {
            weekdays |= match n {
                0 => month.weekdays(*wd),
                n => month.nth_weekday(*n, *wd).map_or(0, |day| 1 << day),
            };
        }
        month_days & weekdays
    }

    /// Steps one period costs besides the dates it selects: one, plus each `BY` list entry it
    /// goes through (in every month it looks at, for a yearly rule).
    fn period_cost(&self) -> usize {
        let months = match self.freq {
            Freq::Yearly if !self.by_month.is_empty() => self.by_month.len(),
            Freq::Yearly if !self.by_month_day.is_empty() => 12,
            _ => 1,
        };
        1 + self.by_month.len()
            + months * (self.by_month_day.len() + self.by_day.len())
            + self.by_set_pos.len()
    }

    /// The dates at the `BYSETPOS` positions of a period's `dates` (in order), in order; all
    /// of them without `BYSETPOS`.
    fn at_set_positions(&self, dates: Vec<NaiveDate>) -> Vec<NaiveDate> {
        if self.by_set_pos.is_empty() {
            return dates;
        }
        let mut kept: Vec<NaiveDate> = self
            .by_set_pos
            .iter()
            .filter_map(|&pos| {
                let index = match usize::try_from(pos) {
                    Ok(from_start) => from_start.checked_sub(1)?,
                    Err(_) => dates
                        .len()
                        .checked_sub(pos.unsigned_abs().try_into().ok()?)?,
                };
                dates.get(index).copied()
            })
            .collect();
        kept.sort_unstable();
        kept.dedup();
        kept
    }

    /// The first day of period `k` and the dates the rule selects in it, in order. `None` once
    /// the dates leave chrono's range.
    fn period(&self, anchor: NaiveDate, k: u64) -> Option<(NaiveDate, Vec<NaiveDate>)> {
        let step = k.checked_mul(u64::from(self.interval))?;
        match self.freq {
            Freq::Daily => {
                let day = anchor.checked_add_days(Days::new(step))?;
                let month = Month::of(day.year(), day.month())?;
                let selected = self.month_matches(day.month())
                    && self.days_of_month(month, day.day()) & (1 << day.day()) != 0;
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
                    .filter(|d| self.month_matches(d.month()))
                    .collect();
                dates.sort_unstable();
                dates.dedup();
                Some((week, dates))
            }
            Freq::Monthly => {
                let first = anchor
                    .with_day(1)?
                    .checked_add_months(Months::new(u32::try_from(step).ok()?))?;
                if !self.month_matches(first.month()) {
                    return Some((first, Vec::new()));
                }
                let month = Month::of(first.year(), first.month())?;
                let dates = days_in(self.days_of_month(month, anchor.day()))
                    .filter_map(|d| first.with_day(d))
                    .collect();
                Some((first, dates))
            }
            Freq::Yearly => {
                let year = anchor.year().checked_add(i32::try_from(step).ok()?)?;
                let first = NaiveDate::from_ymd_opt(year, 1, 1)?;
                let months: Vec<u32> = if !self.by_month.is_empty() {
                    self.by_month.clone()
                } else if !self.by_month_day.is_empty() {
                    (1..=12).collect()
                } else {
                    vec![anchor.month()]
                };
                let mut dates = Vec::new();
                for m in months {
                    let Some(month) = Month::of(year, m) else {
                        continue;
                    };
                    dates.extend(
                        days_in(self.days_of_month(month, anchor.day()))
                            .filter_map(|d| NaiveDate::from_ymd_opt(year, m, d)),
                    );
                }
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
    /// `start` with them). Each period walked costs [`Rule::period_cost`] steps of `budget`,
    /// and each date it selects (before `BYSETPOS` picks from them) one more.
    pub(super) fn walk(
        &self,
        start: NaiveDateTime,
        from: Option<NaiveDate>,
        end: NaiveDateTime,
        budget: &mut usize,
        mut visit: impl FnMut(NaiveDateTime) -> bool,
    ) -> Result<(), Exhausted> {
        let (anchor, time) = (start.date(), start.time());
        let cost = self.period_cost();
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
            charge(budget, cost)?;
            let Some((period_start, dates)) = self.period(anchor, k) else {
                return Ok(());
            };
            if period_start > end.date() {
                return Ok(());
            }
            charge(budget, dates.len())?;
            // Positions count every date of the period, those before DTSTART included.
            for date in self.at_set_positions(dates) {
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
        let march = Month::of(2026, 3).unwrap();
        assert_eq!(march.nth_weekday(1, Weekday::Sun), Some(1));
        assert_eq!(march.nth_weekday(2, Weekday::Sun), Some(8));
        assert_eq!(march.nth_weekday(-1, Weekday::Sun), Some(29));
        assert_eq!(march.nth_weekday(5, Weekday::Sun), Some(29));
        assert_eq!(march.nth_weekday(6, Weekday::Sun), None);
        assert_eq!(march.nth_weekday(-6, Weekday::Sun), None);
        let february = Month::of(2026, 2).unwrap();
        assert_eq!(february.nth_weekday(-1, Weekday::Sat), Some(28));
        assert_eq!(february.nth_weekday(0, Weekday::Sat), None);
        assert_eq!(
            days_in(march.weekdays(Weekday::Sun)).collect::<Vec<_>>(),
            [1, 8, 15, 22, 29]
        );
        assert_eq!(
            (march.day(-1), march.day(31), march.day(-31)),
            (Some(31), Some(31), Some(1))
        );
        // Leap years, and April's missing 31st counted from either end.
        assert_eq!(Month::of(2024, 2).unwrap().len, 29);
        assert_eq!(Month::of(1900, 2).unwrap().len, 28);
        let april = Month::of(2026, 4).unwrap();
        assert_eq!((april.day(31), april.day(-31)), (None, None));
    }

    #[test]
    fn repeated_list_entries_are_kept_once() {
        let repeated = |part: &str, value: &str| {
            let values = vec![value; 200_000].join(",");
            Rule::parse(&format!("FREQ=MONTHLY;{}={}", part, values))
        };
        assert_eq!(
            repeated("BYMONTHDAY", "1"),
            Rule::parse("FREQ=MONTHLY;BYMONTHDAY=1")
        );
        assert_eq!(
            repeated("BYDAY", "-1SU"),
            Rule::parse("FREQ=MONTHLY;BYDAY=-1SU")
        );
        assert_eq!(
            repeated("BYMONTH", "3"),
            Rule::parse("FREQ=MONTHLY;BYMONTH=3")
        );
        // Sorted too, so the order they are written in does not matter.
        assert_eq!(
            Rule::parse("FREQ=YEARLY;BYMONTH=10,3,10;BYDAY=SU,-1SU,1MO,SU"),
            Rule::parse("FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU,SU,1MO")
        );
        let all_days = Rule::parse(&format!(
            "FREQ=MONTHLY;BYMONTHDAY={}",
            (-31..=31)
                .chain(-31..=31)
                .filter(|d| *d != 0)
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
        assert_eq!(all_days.by_month_day.len(), 62);
    }

    /// Steps a walk from `start` to `end` takes, and the occurrences it visits.
    fn cost(rule: &str, start: &str, end: &str) -> (usize, usize) {
        let rule = Rule::parse(rule).unwrap();
        let mut budget = usize::MAX;
        let mut visited = 0;
        rule.walk(at(start), None, at(end), &mut budget, |_| {
            visited += 1;
            true
        })
        .unwrap();
        (usize::MAX - budget, visited)
    }

    #[test]
    fn walks_are_charged_for_list_entries_and_dates() {
        // Every day of a year from one yearly period: each date is paid for.
        let every_day = (1..=31)
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let (steps, visited) = cost(
            &format!("FREQ=YEARLY;BYMONTHDAY={}", every_day),
            "20260101T090000",
            "20261231T235959",
        );
        assert_eq!(visited, 365);
        assert!(steps >= 365 + 12 * 31, "{}", steps);
        // Long lists that select nothing still cost each entry in every period.
        let (steps, visited) = cost(
            "FREQ=MONTHLY;BYMONTHDAY=29,30,31,-29,-30,-31;BYDAY=2MO,2TU,2WE,3MO,3TU,3WE",
            "20260101T090000",
            "20261231T235959",
        );
        assert_eq!(visited, 1);
        assert!(steps >= 12 * (1 + 6 + 6), "{}", steps);
        // A plain daily rule costs a couple of steps a day.
        let (steps, visited) = cost("FREQ=DAILY", "20260101T090000", "20261231T235959");
        assert_eq!(visited, 365);
        assert!((365..=3 * 366).contains(&steps), "{}", steps);
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
    fn set_positions_pick_from_each_period() {
        // RFC 5545 section 3.8.5.3's examples: the third Tuesday, Wednesday or Thursday of the
        // month for three months...
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;COUNT=3;BYDAY=TU,WE,TH;BYSETPOS=3",
                "19970904T090000",
                "19980101T000000"
            ),
            ["19970904T090000", "19971007T090000", "19971106T090000"]
        );
        // ... and the second-to-last weekday of the month.
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-2",
                "19970929T090000",
                "19980331T235959"
            ),
            [
                "19970929T090000",
                "19971030T090000",
                "19971127T090000",
                "19971230T090000",
                "19980129T090000",
                "19980226T090000",
                "19980330T090000"
            ]
        );
        // The last weekday of the month, as Outlook writes it, from a DTSTART that is one.
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;INTERVAL=1;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;WKST=SU",
                "20260130T160000",
                "20260601T000000"
            ),
            [
                "20260130T160000",
                "20260227T160000",
                "20260331T160000",
                "20260430T160000",
                "20260529T160000"
            ]
        );
        // First and last of the set, the same date named twice kept once, and positions past
        // the set's end ignored.
        assert_eq!(
            occurrences(
                "FREQ=MONTHLY;BYMONTHDAY=1,15,-1;BYSETPOS=1,-1,-3,9;COUNT=4",
                "20260101T080000",
                "20270101T000000"
            ),
            [
                "20260101T080000",
                "20260131T080000",
                "20260201T080000",
                "20260228T080000"
            ]
        );
        // Yearly: the last Sunday of March or October, whichever is later (October).
        assert_eq!(
            occurrences(
                "FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU;BYSETPOS=-1;COUNT=2",
                "20251026T020000",
                "20300101T000000"
            ),
            ["20251026T020000", "20261025T020000"]
        );
        // Weekly: the first working day of each week.
        assert_eq!(
            occurrences(
                "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=1;COUNT=3",
                "20260105T090000",
                "20270101T000000"
            ),
            ["20260105T090000", "20260112T090000", "20260119T090000"]
        );
        // Skipping ahead to the window keeps the same occurrences.
        let rule = Rule::parse("FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1").unwrap();
        let walk = |from: Option<NaiveDate>| {
            let mut out = Vec::new();
            rule.walk(
                at("20200131T160000"),
                from,
                at("20260801T000000"),
                &mut 100_000,
                |t| {
                    out.push(t);
                    true
                },
            )
            .unwrap();
            out.retain(|t| t.date() >= NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
            out
        };
        assert_eq!(walk(None), walk(NaiveDate::from_ymd_opt(2026, 1, 1)));
        assert_eq!(walk(None).len(), 7);
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
        assert!(budget > 9_800);
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
        // A budget that ran out stays spent, even for a step it could have paid.
        assert_eq!(tiny, 0);
        let mut left = 5;
        assert_eq!(charge(&mut left, 6), Err(Exhausted));
        assert_eq!((left, charge(&mut left, 0)), (0, Ok(())));
    }

    #[test]
    fn unsupported_rules_do_not_parse() {
        for rule in [
            "FREQ=HOURLY",
            "FREQ=YEARLY;BYWEEKNO=20",
            "FREQ=YEARLY;BYYEARDAY=100",
            // BYSETPOS needs another BY part to pick from, and a position within a year.
            "FREQ=MONTHLY;BYSETPOS=1",
            "FREQ=MONTHLY;BYDAY=MO;BYSETPOS=0",
            "FREQ=MONTHLY;BYDAY=MO;BYSETPOS=367",
            "FREQ=MONTHLY;BYDAY=MO;BYSETPOS=last",
            "FREQ=YEARLY;BYDAY=20MO",
            "FREQ=WEEKLY;BYDAY=1MO",
            "FREQ=MONTHLY;BYDAY=6MO",
            "FREQ=YEARLY;BYMONTH=3;BYDAY=-6SU",
            "FREQ=DAILY;INTERVAL=0",
            "FREQ=DAILY;BYMONTH=13",
            "BYDAY=MO",
            "FREQ=DAILY;UNTIL=soon",
            "FREQ=DAILY;X-NAME=1",
        ] {
            assert_eq!(Rule::parse(rule), None, "{}", rule);
        }
        assert!(Rule::parse("freq=weekly;byday=mo;").is_some());
        assert!(Rule::parse("FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1").is_some());
    }

    proptest::proptest! {
        #[test]
        fn walks_are_ordered_bounded_and_never_panic(
            rule in "FREQ=(DAILY|WEEKLY|MONTHLY|YEARLY)(;INTERVAL=[1-9]{1,3})?(;COUNT=[0-9]{1,4})?(;BYMONTH=(1[0-2]|[1-9]))?(;BYMONTHDAY=-?[1-9]{1,2})?(;BYDAY=(-?[1-5])?(MO|TU|WE|TH|FR|SA|SU)(,(MO|TU|WE|TH|FR|SA|SU))?)?(;BYSETPOS=-?[1-9]{1,2})?(;WKST=(MO|SU))?",
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
