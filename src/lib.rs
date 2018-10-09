#[macro_use] extern crate serde_derive;

extern crate serde;
#[macro_use] extern crate serde_json;

use std::collections::BTreeMap;

use serde::de::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum Value<'a> {
    WithMeta {
        #[serde(rename = "_meta")]
        meta_map: BTreeMap<String, &'a serde_json::value::RawValue>,
        #[serde(flatten)]
        #[serde(borrow)]
        value: SubValue<'a>
    },
    Arbitrary(&'a serde_json::value::RawValue)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SubValue<'a> {
    #[serde(borrow)]
    Array(Vec<Value<'a>>),
    #[serde(borrow)]
    Map(BTreeMap<String, Value<'a>>),
    #[serde(borrow)]
    Arbitrary(&'a serde_json::value::RawValue)
}

pub struct Annotated<V, M> {
    value: V,
    meta: M
}

fn deserialize_from_value<'de, T, D: Deserializer<'de>>(value: serde_json::Value)
-> Result<T, D::Error> 
where for <'de2> T: Deserialize<'de2>
{
    serde_json::from_value(value).map_err(|e| serde::de::Error::custom(e))
}

impl<'de, V, M> Deserialize<'de> for Annotated<V, M>
where for <'de2> V: Deserialize<'de2>,
      for <'de2> M: Deserialize<'de2> + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value: serde_json::Value = Deserialize::deserialize(deserializer)?;

        if let serde_json::Value::Object(mut obj) = value {
            let mut meta_map: serde_json::Map<String, serde_json::Value> = match obj.remove("_meta") {
                Some(x) => deserialize_from_value::<_, D>(x)?,
                None => Default::default()
            };
            let value = match obj.remove("_value").unwrap_or(serde_json::Value::Object(obj)) {
                serde_json::Value::Object(obj) => serde_json::Value::Object(obj.into_iter()
                    .map(|(key, value)| {
                        let value = json!({
                            "_meta": meta_map.remove(&key).unwrap_or_else(|| json!({})),
                            "_value": value
                        });
                        (key, value)
                    })
                    .collect()),
                serde_json::Value::Array(arr) => serde_json::Value::Array(arr.into_iter()
                    .enumerate()
                    .map(|(i, value)| {
                        json!({
                            "_meta": meta_map.remove(&format!("{}", i)).unwrap_or_else(|| json!({})),
                            "_value": value
                        })
                    })
                    .collect()),
                primitive => primitive
            };

            let meta = match meta_map.remove("") {
                Some(x) => deserialize_from_value::<_, D>(x)?,
                None => Default::default()
            };

            Ok(Annotated {
                value: deserialize_from_value::<_, D>(value)?,
                meta
            })
        } else {
            Ok(Annotated {
                value: deserialize_from_value::<_, D>(value)?,
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
