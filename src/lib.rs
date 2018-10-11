#[macro_use]
extern crate serde_derive;

extern crate serde;
#[cfg(test)]
#[macro_use]
extern crate serde_json;
#[cfg(not(test))]
extern crate serde_json;

use std::collections::BTreeMap;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

/// Substitution for serde_json::RawValue, TODO: replace this type once RawValue actually works
/// https://github.com/serde-rs/json/issues/497
#[derive(Debug, Deserialize, Serialize)]
struct OpaqueValue(serde_json::Value);

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum ValueWithMeta<V, M> {
    Explicit {
        #[serde(rename = "_meta")]
        meta_map: M,
        #[serde(rename = "_value")]
        value: V,
    },
    Inline {
        #[serde(rename = "_meta")]
        meta_map: M,
        #[serde(flatten)]
        value: V,
    },
    NoMeta(V),
}

impl<V, M> ValueWithMeta<V, M> {
    #[inline]
    fn new(value: V, meta: Option<M>) -> Self {
        match meta {
            Some(meta_map) => ValueWithMeta::Explicit { meta_map, value },
            None => ValueWithMeta::NoMeta(value),
        }
    }

    #[inline]
    fn into_tuple(self) -> (V, Option<M>) {
        match self {
            ValueWithMeta::Explicit { value, meta_map }
            | ValueWithMeta::Inline { value, meta_map } => (value, Some(meta_map)),
            ValueWithMeta::NoMeta(value) => (value, None),
        }
    }

    fn with_meta(self, meta: Option<M>) -> Self {
        let (value, _) = self.into_tuple();
        Self::new(value, meta)
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
    Primitive(OpaqueValue),
}

type OpaqueValueWithMeta = ValueWithMeta<OpaqueValue, OpaqueValue>;

fn coerce_over_json<A, B>(source: &A) -> Result<B, serde_json::Error>
where
    A: Serialize,
    for<'de> B: Deserialize<'de>,
{
    serde_json::from_str(&serde_json::to_string(source)?)
}

pub struct Annotated<V, M> {
    value: V,
    meta: M,
}

impl<'de, V, M> Deserialize<'de> for Annotated<V, M>
where
    for<'de2> V: Deserialize<'de2>,
    for<'de2> M: Deserialize<'de2> + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value: ShallowValueWithMeta = Deserialize::deserialize(deserializer)?;
        match value {
            ValueWithMeta::Inline {
                mut meta_map,
                value,
            }
            | ValueWithMeta::Explicit {
                mut meta_map,
                value,
            } => {
                let value = match value {
                    ShallowValue::Map(obj) => ShallowValue::Map(
                        obj.into_iter()
                            .map(|(key, mut value_with_meta)| {
                                value_with_meta = value_with_meta.with_meta(meta_map.remove(&key));
                                (key, value_with_meta)
                            }).collect(),
                    ),
                    ShallowValue::Array(arr) => ShallowValue::Array(
                        arr.into_iter()
                            .enumerate()
                            .map(|(i, value_with_meta)| {
                                value_with_meta.with_meta(meta_map.remove(&format!("{}", i)))
                            }).collect(),
                    ),
                    primitive => primitive,
                };

                let meta = match meta_map.remove("") {
                    Some(x) => coerce_over_json(&x).map_err(serde::de::Error::custom)?,
                    None => Default::default(),
                };

                Ok(Annotated {
                    value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
                    meta,
                })
            }
            value => Ok(Annotated {
                value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
                meta: Default::default(),
            }),
        }
    }
}

impl<V, M> Serialize for Annotated<V, M>
where
    V: Serialize,
    M: Serialize + PartialEq + Default,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value: ShallowValue =
            coerce_over_json(&self.value).map_err(serde::ser::Error::custom)?;
        let mut meta_map: BTreeMap<String, OpaqueValue> = BTreeMap::new();
        if self.meta != Default::default() {
            meta_map.insert(
                "".to_owned(),
                coerce_over_json(&self.meta).map_err(serde::ser::Error::custom)?,
            );
        }

        let (as_inline, value) = match value {
            ShallowValue::Map(obj) => (
                true,
                ShallowValue::Map({
                    obj.into_iter()
                        .map(|(key, value_with_meta)| {
                            let (value, meta) = value_with_meta.into_tuple();
                            if let Some(meta) = meta {
                                meta_map.insert(key.clone(), meta);
                            }
                            (key, ValueWithMeta::NoMeta(value))
                        }).collect()
                }),
            ),
            ShallowValue::Array(arr) => (
                false,
                ShallowValue::Array({
                    arr.into_iter()
                        .enumerate()
                        .map(|(i, value_with_meta)| {
                            let (value, meta) = value_with_meta.into_tuple();
                            if let Some(meta) = meta {
                                meta_map.insert(format!("{}", i), meta);
                            };

                            ValueWithMeta::NoMeta(value)
                        }).collect()
                }),
            ),
            primitive => (false, primitive),
        };

        let value_with_meta = if meta_map.is_empty() {
            ValueWithMeta::NoMeta(value)
        } else if as_inline {
            ValueWithMeta::Inline { meta_map, value }
        } else {
            ValueWithMeta::Explicit { meta_map, value }
        };

        Serialize::serialize(&value_with_meta, serializer)
    }
}

#[cfg(test)]
mod test_deserialize {
    use super::{Annotated, OpaqueValue, OpaqueValueWithMeta, ShallowValue, ShallowValueWithMeta};
    use serde::de::Deserialize;
    use serde_json;

    #[test]
    fn value_input() {
        fn inner<T>()
        where
            for<'de> T: Deserialize<'de>,
        {
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
            meta_foo: Option<usize>,
        }

        #[derive(Deserialize)]
        struct Event {
            foo: Annotated<usize, Meta>,
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

#[cfg(test)]
mod test_serialize {
    use super::Annotated;
    use serde_json;

    #[test]
    fn basics() {
        #[derive(Serialize, Default, PartialEq)]
        struct Meta {
            meta_foo: Option<usize>,
        }

        #[derive(Serialize)]
        struct Event {
            foo: Annotated<usize, Meta>,
        }

        let event = Annotated {
            value: Event {
                foo: Annotated {
                    value: 69,
                    meta: Meta { meta_foo: Some(42) },
                },
            },
            meta: Meta {
                meta_foo: Some(420),
            },
        };

        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(
            value,
            json!({
            "_meta": {
                "": {"meta_foo": 420},
                "foo": {"": {"meta_foo": 42}},
            },
            "foo": 69
        })
        );
    }
}
