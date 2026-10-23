//! Topic-filter helpers used by ACL checks.
//!
//! Publish checks reuse MQTT matching. Subscribe checks ask whether every topic
//! matched by the requested filter is also matched by the ACL filter.

/// Returns whether `filter` matches the topic name `topic`.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let filters: Vec<&str> = filter.split('/').collect();
    let topics: Vec<&str> = topic.split('/').collect();
    if topic.starts_with('$')
        && filters
            .first()
            .is_some_and(|level| *level == "+" || *level == "#")
    {
        return false;
    }
    match_levels(&filters, &topics)
}

fn match_levels(filter: &[&str], topic: &[&str]) -> bool {
    let mut filter_index = 0;
    let mut topic_index = 0;
    while filter_index < filter.len() {
        if filter[filter_index] == "#" {
            return filter_index + 1 == filter.len();
        }
        if topic_index >= topic.len() {
            return false;
        }
        if filter[filter_index] != "+" && filter[filter_index] != topic[topic_index] {
            return false;
        }
        filter_index += 1;
        topic_index += 1;
    }
    topic_index == topic.len()
}

/// Returns whether every topic matched by `requested` is also matched by `acl`.
pub fn filter_covers(acl: &str, requested: &str) -> bool {
    let acl_levels: Vec<&str> = acl.split('/').collect();
    let requested_levels: Vec<&str> = requested.split('/').collect();
    covers(&acl_levels, &requested_levels, true)
}

fn covers(acl: &[&str], requested: &[&str], at_root: bool) -> bool {
    if acl.first().is_some_and(|level| *level == "#") && acl.len() == 1 {
        if at_root
            && requested
                .first()
                .is_some_and(|level| level.starts_with('$'))
        {
            return false;
        }
        return true;
    }
    if requested.is_empty() {
        return acl.is_empty();
    }
    if requested[0] == "#" && requested.len() == 1 {
        return false;
    }
    if acl.is_empty() {
        return false;
    }
    let level_ok = if at_root && requested[0].starts_with('$') && (acl[0] == "+" || acl[0] == "#") {
        false
    } else if acl[0] == "+" {
        true
    } else {
        acl[0] == requested[0]
    };
    level_ok && covers(&acl[1..], &requested[1..], false)
}

#[cfg(test)]
mod tests {
    use super::{filter_covers, topic_matches};

    #[test]
    fn topic_matches_should_honor_wildcards_and_system_topics() {
        assert!(topic_matches("a/b", "a/b"));
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(!topic_matches("#", "$SYS/load"));
        assert!(!topic_matches("+/load", "$SYS/load"));
        assert!(topic_matches("$SYS/#", "$SYS/load"));
    }

    #[test]
    fn filter_covers_should_allow_only_narrower_subscriptions() {
        assert!(filter_covers("#", "sensors/temp"));
        assert!(filter_covers("#", "#"));
        assert!(!filter_covers("#", "$SYS/#"));
        assert!(filter_covers("a/#", "a/b"));
        assert!(filter_covers("a/#", "a/+"));
        assert!(filter_covers("a/#", "a"));
        assert!(filter_covers("a/+", "a/b"));
        assert!(!filter_covers("a/+", "a/#"));
        assert!(!filter_covers("a/b", "a/+"));
        assert!(filter_covers("a/b", "a/b"));
        assert!(filter_covers("$SYS/#", "$SYS/load"));
    }
}
