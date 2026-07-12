//! Topic partition key matching for subscribe filters.

/// Returns whether `ev` matches the subscribe topic and optional key filter.
pub fn topic_filter_matches(
    ev: &crate::models::Event,
    topic: &str,
    filter: Option<&String>,
) -> bool {
    if ev.topic_name != topic {
        return false;
    }
    filter.is_none_or(|k| ev.topic_key.as_deref() == Some(k.as_str()))
}
