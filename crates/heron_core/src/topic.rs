//! Topic-filter matching and the in-memory subscription tree.

use std::collections::HashMap;

use bytes::Bytes;

use crate::message::SubOpts;
use crate::qos::QoS;

/// A subscriber selected for one publication.
#[derive(Debug, Clone)]
pub struct MatchedSub {
    /// Subscriber client identifier.
    pub client_id: String,
    /// Highest QoS among the client's matching filters.
    pub qos: QoS,
    /// True only when every matching filter set no-local.
    pub no_local: bool,
    /// True when any matching filter set retain-as-published.
    pub retain_as_published: bool,
}

/// Retained application message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// Topic name.
    pub topic: String,
    /// Payload.
    pub payload: Bytes,
    /// Original QoS.
    pub qos: QoS,
}

#[derive(Debug, Default)]
struct Node {
    children: HashMap<String, Node>,
    subs: HashMap<String, SubOpts>,
}

/// Subscription index plus retained messages.
#[derive(Debug, Default)]
pub struct TopicTree {
    root: Node,
    retained: HashMap<String, Retained>,
}

impl TopicTree {
    /// Empty tree.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace one client's subscription.
    pub fn subscribe(&mut self, client_id: &str, filter: &str, opts: SubOpts) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.to_owned()).or_default();
        }
        node.subs.insert(client_id.to_owned(), opts);
    }

    /// Remove one filter. Returns whether it was present.
    pub fn unsubscribe(&mut self, client_id: &str, filter: &str) -> bool {
        fn walk(node: &mut Node, levels: &[&str], client_id: &str) -> bool {
            if levels.is_empty() {
                return node.subs.remove(client_id).is_some();
            }
            let Some(child) = node.children.get_mut(levels[0]) else {
                return false;
            };
            let removed = walk(child, &levels[1..], client_id);
            if child.subs.is_empty() && child.children.is_empty() {
                node.children.remove(levels[0]);
            }
            removed
        }
        let levels: Vec<&str> = filter.split('/').collect();
        walk(&mut self.root, &levels, client_id)
    }

    /// Drop every filter owned by `client_id`.
    pub fn remove_client(&mut self, client_id: &str) {
        fn walk(node: &mut Node, client_id: &str) {
            node.subs.remove(client_id);
            let names: Vec<String> = node.children.keys().cloned().collect();
            for name in names {
                if let Some(child) = node.children.get_mut(&name) {
                    walk(child, client_id);
                    if child.subs.is_empty() && child.children.is_empty() {
                        node.children.remove(&name);
                    }
                }
            }
        }
        walk(&mut self.root, client_id);
    }

    /// Subscribers whose filters match `topic`.
    ///
    /// Overlapping filters for the same client collapse into one match at the
    /// highest QoS.
    pub fn match_topic(&self, topic: &str) -> Vec<MatchedSub> {
        let levels: Vec<&str> = topic.split('/').collect();
        let dollar = levels.first().is_some_and(|level| level.starts_with('$'));
        let mut found: HashMap<String, MatchedSub> = HashMap::new();
        walk_match(&self.root, &levels, dollar, true, &mut found);
        found.into_values().collect()
    }

    /// Store or delete a retained message. An empty payload deletes it.
    pub fn set_retained(&mut self, topic: &str, payload: Bytes, qos: QoS) -> bool {
        if payload.is_empty() {
            self.retained.remove(topic).is_some()
        } else {
            self.retained.insert(
                topic.to_owned(),
                Retained {
                    topic: topic.to_owned(),
                    payload,
                    qos,
                },
            );
            true
        }
    }

    /// Retained messages that match `filter`.
    pub fn retained_for(&self, filter: &str) -> Vec<Retained> {
        self.retained
            .values()
            .filter(|message| topic_matches(filter, &message.topic))
            .cloned()
            .collect()
    }
}

fn walk_match(
    node: &Node,
    levels: &[&str],
    dollar: bool,
    at_root: bool,
    found: &mut HashMap<String, MatchedSub>,
) {
    if levels.is_empty() {
        absorb(node, found);
        if let Some(hash) = node.children.get("#") {
            absorb(hash, found);
        }
        return;
    }

    if let Some(child) = node.children.get(levels[0]) {
        walk_match(child, &levels[1..], dollar, false, found);
    }
    if at_root && dollar {
        return;
    }
    if let Some(plus) = node.children.get("+") {
        walk_match(plus, &levels[1..], dollar, false, found);
    }
    if let Some(hash) = node.children.get("#") {
        absorb(hash, found);
    }
}

fn absorb(node: &Node, found: &mut HashMap<String, MatchedSub>) {
    for (client_id, opts) in &node.subs {
        found
            .entry(client_id.clone())
            .and_modify(|existing| {
                if opts.qos > existing.qos {
                    existing.qos = opts.qos;
                }
                existing.no_local &= opts.no_local;
                existing.retain_as_published |= opts.retain_as_published;
            })
            .or_insert_with(|| MatchedSub {
                client_id: client_id.clone(),
                qos: opts.qos,
                no_local: opts.no_local,
                retain_as_published: opts.retain_as_published,
            });
    }
}

/// Returns whether `filter` matches `topic`, including the `$` wildcard rule.
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

/// Structural check for a topic filter. Shared subscriptions are reported separately.
pub fn classify_filter(filter: &str) -> Result<(), FilterClass> {
    if filter.is_empty() || filter.len() > 512 || filter.contains('\0') {
        return Err(FilterClass::Invalid);
    }
    if filter.starts_with("$share/") {
        return Err(FilterClass::Shared);
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && *level != "#" {
            return Err(FilterClass::Invalid);
        }
        if level.contains('+') && *level != "+" {
            return Err(FilterClass::Invalid);
        }
        if *level == "#" && index + 1 != levels.len() {
            return Err(FilterClass::Invalid);
        }
    }
    Ok(())
}

/// Why a filter cannot be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterClass {
    /// The filter is not a valid MQTT topic filter.
    Invalid,
    /// The filter is a shared subscription.
    Shared,
}

/// Structural check for a publish topic name.
pub fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty() && topic.len() <= 512 && !topic.contains(['+', '#', '\0'])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_matches_should_honor_wildcards_and_system_topics() {
        assert!(topic_matches("sport/tennis", "sport/tennis"));
        assert!(topic_matches("sport/+/player", "sport/tennis/player"));
        assert!(topic_matches("sport/#", "sport"));
        assert!(topic_matches("sport/#", "sport/tennis/player"));
        assert!(topic_matches("#", "sport"));
        assert!(topic_matches("a/+/b", "a//b"));
        assert!(!topic_matches("sport/tennis", "sport/golf"));
        assert!(!topic_matches("sport/+", "sport/tennis/player"));
        assert!(!topic_matches("#", "$SYS/load"));
        assert!(!topic_matches("+/load", "$SYS/load"));
        assert!(topic_matches("$SYS/#", "$SYS/load"));
        assert!(topic_matches("$SYS/+", "$SYS/load"));
    }

    #[test]
    fn match_topic_should_collapse_overlapping_filters_to_highest_qos() {
        let mut tree = TopicTree::new();
        tree.subscribe("c1", "sport/#", SubOpts::v4(QoS::AtLeastOnce));
        tree.subscribe(
            "c1",
            "sport/tennis",
            SubOpts {
                qos: QoS::ExactlyOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: 0,
            },
        );
        tree.subscribe("c2", "sport/+", SubOpts::v4(QoS::AtMostOnce));

        let mut matches = tree.match_topic("sport/tennis");
        matches.sort_by(|left, right| left.client_id.cmp(&right.client_id));

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].client_id, "c1");
        assert_eq!(matches[0].qos, QoS::ExactlyOnce);
        assert!(!matches[0].no_local);
        assert!(matches[0].retain_as_published);
        assert_eq!(matches[1].qos, QoS::AtMostOnce);
    }

    #[test]
    fn set_retained_should_delete_when_payload_is_empty() {
        let mut tree = TopicTree::new();
        tree.set_retained("a/b", Bytes::from_static(b"x"), QoS::AtLeastOnce);
        assert_eq!(tree.retained_for("a/b").len(), 1);
        tree.set_retained("a/b", Bytes::new(), QoS::AtMostOnce);
        assert!(tree.retained_for("a/b").is_empty());
    }

    #[test]
    fn classify_filter_should_reject_shared_and_misplaced_hash() {
        assert_eq!(classify_filter("a/b"), Ok(()));
        assert_eq!(classify_filter("$share/g/a"), Err(FilterClass::Shared));
        assert_eq!(classify_filter("a/#/b"), Err(FilterClass::Invalid));
        assert!(!valid_topic_name("a/+"));
        assert!(valid_topic_name("a/b"));
    }
}
