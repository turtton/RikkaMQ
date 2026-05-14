pub(crate) const QUEUE_FIELD: &str = "info";

pub(crate) fn group(name: &str) -> String {
    format!("g:{name}")
}

pub(crate) fn failed(name: &str) -> String {
    format!("failed:{name}")
}

pub(crate) fn delayed(name: &str) -> String {
    format!("delayed:{name}")
}
