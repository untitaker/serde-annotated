#[macro_use] extern crate serde_derive;

extern crate serde;
#[cfg(test)] #[macro_use] extern crate serde_json;
#[cfg(not(test))] extern crate serde_json;

use std::collections::BTreeMap;

use serde::ser::Serialize;
use serde::de::{Deserialize, Deserializer};

/// Substitution for serde_json::RawValue, TODO: replace this type once RawValue actually works
/// https://github.com/serde-rs/json/issues/497
#[derive(Debug, Deserialize, Serialize)]
struct OpaqueValue(serde_json::Value);

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum ValueWithMeta<V, M> {
    ExplicitMeta {
        #[serde(rename = "_meta")]
        meta_map: M,
        #[serde(rename = "_value")]
        value: V
    },
    InlineMeta {
        #[serde(rename = "_meta")]
        meta_map: M,
        #[serde(flatten)]
        value: V
    },
    NoMeta(V)
}

use ValueWithMeta::*;

impl<V, M> ValueWithMeta<V, M> {
    fn with_meta(self, meta: Option<M>) -> Self {
        let value = match self {
            ExplicitMeta { value, .. } | InlineMeta { value, .. } | NoMeta(value) => value
        };

        match meta {
            Some(meta_map) => ExplicitMeta { meta_map, value },
            None => NoMeta(value)
        }
    }
}

/// Holds a value with metadata, where the topmost level of the metadata is rearrangable but
/// everything further below is opaque (unparsed)
type ShallowValueWithMeta = ValueWithMeta<ShallowValue, BTreeMap<String, OpaqueValue>>;

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum ShallowValue {
    Array(Vec<OpaqueValueWithMeta>),
    Map(BTreeMap<String, OpaqueValueWithMeta>),
    // We would want to use RawValue here but it does not matter much in terms of performance
    Primitive(OpaqueValue)
}

type OpaqueValueWithMeta = ValueWithMeta<OpaqueValue, OpaqueValue>;

fn coerce_over_json<A, B>(source: &A) -> Result<B, serde_json::Error> 
where A: Serialize,
      for <'de> B: Deserialize<'de>
{
    serde_json::from_str(&serde_json::to_string(source)?)
}

pub struct Annotated<V, M> {
    value: V,
    meta: M
}

impl<'de, V, M> Deserialize<'de> for Annotated<V, M>
where for <'de2> V: Deserialize<'de2>,
      for <'de2> M: Deserialize<'de2> + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value: ShallowValueWithMeta = Deserialize::deserialize(deserializer)?;
        match value {
            InlineMeta { mut meta_map, value } | ExplicitMeta { mut meta_map, value } => {
                let value = match value {
                    ShallowValue::Map(obj) => ShallowValue::Map(obj.into_iter()
                        .map(|(key, value)| {
                            let value = value.with_meta(meta_map.remove(&key));
                            (key, value)
                        })
                        .collect()),
                    ShallowValue::Array(arr) => ShallowValue::Array(arr.into_iter()
                        .enumerate()
                        .map(|(i, value)| {
                            value.with_meta(meta_map.remove(&format!("{}", i)))
                        })
                        .collect()),
                    primitive => primitive
                };


                let meta = match meta_map.remove("") {
                    Some(x) => coerce_over_json(&x).map_err(serde::de::Error::custom)?,
                    None => Default::default()
                };

                Ok(Annotated {
                    value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
                    meta
                })
            },
            value => {
                Ok(Annotated {
                    value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
                    meta: Default::default()
                })
            }
        }
    }
}

#[cfg(test)]
mod test_deserialize {
    use serde_json;
    use serde::de::{Deserialize};
    use super::{Annotated, ShallowValueWithMeta, ShallowValue, OpaqueValueWithMeta, OpaqueValue};

    #[test]
    fn value_input() {
        fn inner<T>() where for<'de> T: Deserialize<'de> {
            let _: T = serde_json::from_value(json!(42)).unwrap();
            let _: T = serde_json::from_value(json!({"_meta": 42, "foo": "bar"})).unwrap();
        }

        inner::<ShallowValueWithMeta>();
        inner::<ShallowValue>();
        inner::<OpaqueValueWithMeta>();
        inner::<OpaqueValue>();
    }

    #[test]
    fn basics() {
        #[derive(Deserialize, Default)]
        struct Meta {
            meta_foo: Option<usize>
        }

        #[derive(Deserialize)]
        struct Event {
            foo: Annotated<usize, Meta>
        }

        let event: Annotated<Event, Meta> = serde_json::from_value(json!({
            "_meta": {
                "": {"meta_foo": 420},
                "foo": {"": {"meta_foo": 42}},
            },
            "foo": 69
        })).unwrap();

        assert_eq!(event.value.foo.value, 69);
        assert_eq!(event.meta.meta_foo, Some(420));
        assert_eq!(event.value.foo.meta.meta_foo, Some(42));
    }
} 
