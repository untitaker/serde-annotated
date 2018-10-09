#[cfg(test)] #[macro_use] extern crate serde_derive;

extern crate serde;
#[macro_use] extern crate serde_json;

use serde_json::value::Value;
use serde_json::Map;
use serde::de::{Deserialize, Deserializer};

pub struct Annotated<V, M> {
    value: V,
    meta: M
}

fn append_path(path: &str, segment: &str) -> String {
    if path == "" || path == "." {
        return segment.to_owned();
    }

    if segment == "" || segment == "." {
        return path.to_owned();
    }

    format!("{}.{}", path, segment)
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
        let value: Value = Deserialize::deserialize(deserializer)?;

        if let Value::Object(mut obj) = value {
            let mut meta_map: Map<String, Value> = match obj.remove("_meta") {
                Some(x) => deserialize_from_value::<_, D>(x)?,
                None => Default::default()
            };

            let value = match obj.remove("_value").unwrap_or(Value::Object(obj)) {
                Value::Object(obj) => Value::Object(obj.into_iter()
                    .map(|(key, value)| {
                        let value = json!({
                            "_meta": meta_map.remove(&key).unwrap_or_else(|| json!({})),
                            "_value": value
                        });
                        (key, value)
                    })
                    .collect()),
                Value::Array(arr) => Value::Array(arr.into_iter()
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
