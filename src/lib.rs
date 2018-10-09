#[macro_use] extern crate serde_derive;

extern crate serde;
#[macro_use] extern crate serde_json;

use std::collections::BTreeMap;

use serde::ser::Serialize;
use serde::de::{Deserialize, Deserializer};

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum Value<'a> {
    WithMeta {
        #[serde(rename = "_meta")]
        meta_map: BTreeMap<String, &'a serde_json::value::RawValue>,
        #[serde(flatten)]
        #[serde(borrow)]
        value: SubValue<'a>
    },
    Arbitrary(#[serde(borrow)] &'a serde_json::value::RawValue)
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum SubValue<'a> {
    #[serde(borrow)]
    Array(Vec<SubSubValue<'a>>),
    #[serde(borrow)]
    Map(BTreeMap<String, SubSubValue<'a>>),
    #[serde(borrow)]
    Arbitrary(&'a serde_json::value::RawValue)
}


#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum SubSubValue<'a> {
    WithMeta {
        #[serde(rename = "_meta")]
        #[serde(borrow)]
        meta: &'a serde_json::value::RawValue,
        #[serde(flatten)]
        #[serde(borrow)]
        value: &'a serde_json::value::RawValue
    },
    Arbitrary(&'a serde_json::value::RawValue)
}

impl<'a> SubSubValue<'a> {
    fn with_meta(self, meta: Option<&'a serde_json::value::RawValue>) -> Self {
        let value = match self {
            SubSubValue::WithMeta { value, .. } => value,
            SubSubValue::Arbitrary(value) => value
        };

        match meta {
            Some(meta) => SubSubValue::WithMeta { meta, value },
            None => SubSubValue::Arbitrary(value)
        }
    }
}

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
        let value: Value = Deserialize::deserialize(deserializer)?;

        if let Value::WithMeta { mut meta_map, value } = value {
            let value = match value {
                SubValue::Map(obj) => SubValue::Map(obj.into_iter()
                    .map(|(key, value)| {
                        let value = value.with_meta(meta_map.remove(&key));
                        (key, value)
                    })
                    .collect()),
                SubValue::Array(arr) => SubValue::Array(arr.into_iter()
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
        } else {
            Ok(Annotated {
                value: coerce_over_json(&value).map_err(serde::de::Error::custom)?,
                meta: Default::default()
            })
        }
    }
}

#[cfg(test)]
mod test_deserialize {
    use serde_json;
    use super::Annotated;

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
