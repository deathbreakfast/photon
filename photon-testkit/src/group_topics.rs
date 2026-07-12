//! Inventory-registered consumer-group test topics.

use photon_backend::TopicDescriptor;

photon_backend::inventory::submit! {
    TopicDescriptor::group(
        "testkit.group",
        8,
        Some("partition_key"),
        "{}",
    )
}

photon_backend::inventory::submit! {
    TopicDescriptor::group(
        "testkit.broker.group",
        8,
        Some("partition_key"),
        "{}",
    )
}

photon_backend::inventory::submit! {
    TopicDescriptor::group(
        "testkit.group.rr",
        8,
        Some("partition_key"),
        "{}",
    )
}

photon_backend::inventory::submit! {
    TopicDescriptor::group(
        "testkit.group.rebalance",
        8,
        Some("partition_key"),
        "{}",
    )
}
