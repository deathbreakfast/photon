//! Filter and paging options for [`StoragePort::list_events`](super::StoragePort::list_events).

use chrono::{DateTime, Utc};

/// What to list: topic and key filters, seq cursors, a time range, order and page size.
///
/// Every filter is optional; an empty query with a `limit` lists the newest events across all
/// topics when `newest_first` is set.
///
/// # Filters
///
/// - `topic` / `key`: exact match on `topic_name` / `topic_key`.
/// - `after_seq` / `before_seq`: exclusive seq bounds (`seq > after_seq`, `seq < before_seq`).
///   Seq counts per `(topic, key)` partition, so cursors only make sense when `topic` is set.
/// - `since` / `until`: half-open time range on `created_at` (`since <= created_at < until`).
/// - `limit`: page size; zero returns an empty page.
///
/// # Order and paging
///
/// Events are ordered by `seq` when `topic` is set, otherwise by `created_at` then `seq`.
/// `newest_first` returns the page in descending order.
///
/// A page is the `limit` events nearest its cursor: with only `after_seq` set it holds the next
/// events above the cursor, with only `before_seq` set the events just below it. With no cursor
/// (or both) the page starts at the end `newest_first` points to. So, while showing events newest
/// first, pass the last seq on screen as `before_seq` for the older page and the first seq on
/// screen as `after_seq` for the newer one.
///
/// # Example
///
/// ```rust
/// use chrono::{Duration, Utc};
/// use photon_backend::ListEventsQuery;
///
/// // Newest 50 `orders` events from the last hour.
/// let query = ListEventsQuery {
///     topic: Some("orders".into()),
///     since: Some(Utc::now() - Duration::hours(1)),
///     newest_first: true,
///     limit: 50,
///     ..ListEventsQuery::default()
/// };
/// assert!(query.until.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListEventsQuery {
    /// Only events on this topic.
    pub topic: Option<String>,
    /// Only events with this partition key.
    pub key: Option<String>,
    /// Only events with `seq > after_seq`.
    pub after_seq: Option<i64>,
    /// Only events with `seq < before_seq`.
    pub before_seq: Option<i64>,
    /// Only events created at or after this time.
    pub since: Option<DateTime<Utc>>,
    /// Only events created before this time.
    pub until: Option<DateTime<Utc>>,
    /// Return the page in descending order.
    pub newest_first: bool,
    /// Page size (zero returns an empty page).
    pub limit: usize,
}

impl ListEventsQuery {
    /// Whether the page is taken from the oldest end of the matching events.
    ///
    /// True with only `after_seq` set, false with only `before_seq` set, otherwise
    /// `!newest_first`.
    #[must_use]
    pub const fn pages_from_oldest(&self) -> bool {
        match (self.after_seq, self.before_seq) {
            (Some(_), None) => true,
            (None, Some(_)) => false,
            _ => !self.newest_first,
        }
    }

    /// Whether the page orders by `seq` (a topic is set) rather than `created_at`.
    #[must_use]
    pub const fn orders_by_seq(&self) -> bool {
        self.topic.is_some()
    }
}
