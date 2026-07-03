//! D574 canonical tuple encoding for generated compound fact ids/keys.

pub(crate) fn canonical_tuple_key(parts: &[&str]) -> String {
    serde_json::to_string(parts).expect("string tuple encoding cannot fail")
}

pub(crate) fn compound_tuple_key(prefix: &str, parts: &[&str]) -> String {
    format!("{prefix}:{}", canonical_tuple_key(parts))
}

#[cfg(test)]
pub(crate) fn parse_canonical_tuple_key(value: &str) -> Option<Vec<String>> {
    serde_json::from_str::<Vec<String>>(value).ok()
}

#[cfg(test)]
mod tests {
    use super::{canonical_tuple_key, compound_tuple_key, parse_canonical_tuple_key};

    #[test]
    fn tuple_keys_do_not_collide_on_old_delimiters() {
        assert_ne!(
            canonical_tuple_key(&["a:b", "c"]),
            canonical_tuple_key(&["a", "b:c"])
        );
        assert_ne!(
            canonical_tuple_key(&["a\0b", "c"]),
            canonical_tuple_key(&["a", "b\0c"])
        );
    }

    #[test]
    fn tuple_keys_round_trip_string_coordinates() {
        let key = canonical_tuple_key(&["policy", "proposal"]);
        assert_eq!(key, r#"["policy","proposal"]"#);
        assert_eq!(
            compound_tuple_key("admission", &["policy", "proposal"]),
            r#"admission:["policy","proposal"]"#
        );
        assert_eq!(
            parse_canonical_tuple_key(&key),
            Some(vec!["policy".to_owned(), "proposal".to_owned()])
        );
        assert_eq!(parse_canonical_tuple_key("policy:proposal"), None);
        assert_eq!(parse_canonical_tuple_key(r#"["ok",1]"#), None);
    }
}
