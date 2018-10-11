#[macro_use]
extern crate serde_derive;

extern crate serde;
#[cfg(test)]
#[macro_use]
extern crate serde_json;
#[cfg(not(test))]
extern crate serde_json;

use std::collections::BTreeMap;
use std::fmt;

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
        // This format is used to communicate data between Annotated<..> layers when the value is
        // not a map.
        //
        // If you get an error saying:
        // - "unexpected type: map", or
        // - "unexpected key: __serde_annotated_foo"
        // it means that one of your fields on V (for `Annotated<V, _>`) isn't wrapped in
        // `Annotated`.
        //
        // ```rust
        // struct Foo { bar: usize }  // Bad
        // struct Foo { bar: Annotated<usize, Meta> } // Good
        // serde_json::from_str::<Annotated<Foo, Meta>>(...)
        //
        // struct Foo { bar: Vec<usize> }  // Bad
        // struct Foo { bar: Annotated<Vec<Annotated<usize, Meta>>, Meta> } // Good
        // ```
        #[serde(rename = "__serde_annotated_value")]
        value: V,
        #[serde(rename = "__serde_annotated_meta")]
        meta: Option<M>,
        #[serde(rename = "__serde_annotated_path")]
        path: Option<String>,
    },
    Inline {
        #[serde(flatten)]
        value: V,
        #[serde(rename = "_meta")]
        meta: M,
    },
    NoMeta(V),
}

impl<V, M> ValueWithMeta<V, M> {
    #[inline]
    fn new(value: V, meta: Option<M>) -> Self {
        match meta {
            Some(meta) => ValueWithMeta::Explicit {
                meta: Some(meta),
                value,
                path: None,
            },
            None => ValueWithMeta::NoMeta(value),
        }
    }

    #[inline]
    fn into_tuple(self) -> (V, Option<M>, Option<String>) {
        match self {
            ValueWithMeta::Explicit { value, meta, path } => (value, meta, path),
            ValueWithMeta::Inline { value, meta } => (value, Some(meta), None),
            ValueWithMeta::NoMeta(value) => (value, None, None),
        }
    }

    fn with_meta(self, meta: Option<M>) -> Self {
        let (value, _, _) = self.into_tuple();
        Self::new(value, meta)
    }

    fn with_path(self, path: String) -> Self {
        let (value, meta, _) = self.into_tuple();
        ValueWithMeta::Explicit {
            value,
            meta,
            path: Some(path),
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

#[derive(Debug)]
pub struct Annotated<V, M> {
    value: V,
    meta: M,
}

impl<V, M> Annotated<V, M> {
    pub fn new(value: V, meta: M) -> Self {
        Annotated { value, meta }
    }
}

pub trait SetPath {
    fn set_path(&mut self, path: String) {
        let _path = path;
    }
}

fn append_path(mut path: String, segment: &str) -> String {
    if path.is_empty() || path == "." {
        return segment.to_owned();
    }

    path.push_str(".");
    path.push_str(segment);
    path
}

impl<'de, V, M> Deserialize<'de> for Annotated<V, M>
where
    for<'de2> V: Deserialize<'de2>,
    for<'de2> M: Deserialize<'de2> + Default + SetPath + fmt::Debug,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value_with_meta: ShallowValueWithMeta = Deserialize::deserialize(deserializer)?;
        let (value, mut meta, path) = value_with_meta.into_tuple();
        let path = path.unwrap_or_else(|| ".".to_owned());
        println!("{:?}", meta);

        let value = match value {
            ShallowValue::Map(obj) => ShallowValue::Map(
                obj.into_iter()
                    .map(|(key, mut value_with_meta)| {
                        if let Some(ref mut meta) = meta {
                            value_with_meta = value_with_meta.with_meta(meta.remove(&key));
                        }
                        value_with_meta =
                            value_with_meta.with_path(append_path(path.clone(), &key));
                        (key, value_with_meta)
                    }).collect(),
            ),
            ShallowValue::Array(arr) => ShallowValue::Array(
                arr.into_iter()
                    .enumerate()
                    .map(|(i, mut value_with_meta)| {
                        let key = format!("{}", i);
                        if let Some(ref mut meta) = meta {
                            value_with_meta = value_with_meta.with_meta(meta.remove(&key));
                        }
                        value_with_meta =
                            value_with_meta.with_path(append_path(path.clone(), &key));
                        value_with_meta
                    }).collect(),
            ),
            primitive => primitive,
        };

        let mut meta_structured: M = Default::default();

        if let Some(ref mut meta) = meta {
            if let Some(x) = meta.remove("") {
                meta_structured = coerce_over_json(&x).map_err(serde::de::Error::custom)?;
            }

            if !meta.is_empty() {
                return Err(serde::de::Error::custom("Too many values in metamap"));
            }
        };

        meta_structured.set_path(path);
        println!("{:?}", meta_structured);

        Ok(Annotated {
            value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
            meta: meta_structured,
        })
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
        let mut meta: BTreeMap<String, OpaqueValue> = BTreeMap::new();
        if self.meta != Default::default() {
            meta.insert(
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
                            let (value, m, _) = value_with_meta.into_tuple();
                            if let Some(m) = m {
                                meta.insert(key.clone(), m);
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
                            let (value, m, _) = value_with_meta.into_tuple();
                            if let Some(m) = m {
                                meta.insert(format!("{}", i), m);
                            };

                            ValueWithMeta::NoMeta(value)
                        }).collect()
                }),
            ),
            primitive => (false, primitive),
        };

        let value_with_meta = if meta.is_empty() {
            ValueWithMeta::NoMeta(value)
        } else if as_inline {
            ValueWithMeta::Inline { meta, value }
        } else {
            ValueWithMeta::Explicit {
                meta: Some(meta),
                value,
                path: None,
            }
        };

        Serialize::serialize(&value_with_meta, serializer)
    }
}

#[cfg(test)]
mod test_deserialize {
    use super::{
        Annotated, OpaqueValue, OpaqueValueWithMeta, SetPath, ShallowValue, ShallowValueWithMeta,
    };
    use serde::de::Deserialize;
    use serde_json;

    use std::collections::BTreeMap;

    impl SetPath for () {}
    impl SetPath for usize {}

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
        #[derive(Debug, Deserialize, Default)]
        struct Meta {
            meta_foo: Option<usize>,
            path: Option<String>,
        }

        impl SetPath for Meta {
            fn set_path(&mut self, path: String) {
                self.path = Some(path)
            }
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

        assert_eq!(event.meta.path, Some(".".to_owned()));
        assert_eq!(event.value.foo.meta.path, Some("foo".to_owned()));
    }

    #[test]
    fn arrays() {
        let result: Annotated<Vec<Annotated<usize, ()>>, ()> =
            serde_json::from_value(json!([420])).unwrap();
        assert_eq!(result.value.len(), 1);
        assert_eq!(result.value.get(0).unwrap().value, 420);

        let result: Result<Annotated<Vec<usize>, ()>, _> = serde_json::from_value(json!([1]));
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid type: map, expected usize at line 1 column 1"
        );
    }

    #[test]
    fn arrays_metadata() {
        #[derive(Debug, Deserialize, Default, PartialEq)]
        struct Meta {
            meta_foo: usize,
            path: Option<String>,
        }

        impl SetPath for Meta {
            fn set_path(&mut self, path: String) {
                self.path = Some(path);
            }
        }

        #[derive(Deserialize)]
        struct Event {
            foo: Annotated<Vec<Annotated<String, Meta>>, Meta>,
        }

        let result: Annotated<Event, Meta> = serde_json::from_value(json!({
            "_meta": {
                "foo": {
                    "": {"meta_foo": 42},
                    "0": {"": {"meta_foo": 420}}
                }
            },
            "foo": ["hi"]
        })).unwrap();

        let foo = result.value.foo;
        let foo0 = foo.value.get(0).unwrap();

        assert_eq!(foo.meta.meta_foo, 42);
        assert_eq!(foo.value.len(), 1);
        assert_eq!(foo0.value, "hi");
        assert_eq!(foo0.meta.path.as_ref().unwrap(), "foo.0");
        assert_eq!(foo0.meta.meta_foo, 420);
    }

    #[test]
    fn objects_metadata() {
        #[derive(Debug, Deserialize, Default, PartialEq)]
        struct Meta {
            meta_foo: usize,
            path: Option<String>,
        }

        impl SetPath for Meta {
            fn set_path(&mut self, path: String) {
                self.path = Some(path);
            }
        }

        #[derive(Deserialize)]
        struct Event {
            foo: Annotated<BTreeMap<String, Annotated<String, Meta>>, Meta>,
        }

        let result: Annotated<Event, Meta> = serde_json::from_value(json!({
            "_meta": {
                "foo": {
                    "": {"meta_foo": 42},
                    "key": {"": {"meta_foo": 420}}
                }
            },
            "foo": { "key": "hi"}
        })).unwrap();

        let foo = result.value.foo;
        let foo_key = foo.value.get("key").unwrap();

        assert_eq!(foo.meta.meta_foo, 42);
        assert_eq!(foo.value.len(), 1);
        assert_eq!(foo_key.value, "hi");
        assert_eq!(foo_key.meta.path.as_ref().unwrap(), "foo.key");
        assert_eq!(foo_key.meta.meta_foo, 420);
    }
}

#[cfg(test)]
mod test_serialize {
    use super::Annotated;
    use serde_json;

    #[test]
    fn basics() {
        #[derive(Debug, Serialize, Default, PartialEq)]
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
