extern crate difference;
extern crate serde_annotated;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

#[macro_use] mod common;

use std::collections::BTreeMap;
use std::fs;

#[derive(Serialize, Deserialize, Default, PartialEq)]
struct Meta {
    foo_meta: Option<usize>
}

type Annotated<T> = serde_annotated::Annotated<T, Meta>;


#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Value {
    Map(BTreeMap<String, Annotated<Value>>),
    Array(Vec<Annotated<Value>>),
    Primitive(serde_json::Value)
}

macro_rules! create_roundtrip_test {
    ($filename:ident) => {
        #[test]
        fn $filename() {
            // Read fixture and normalize formatting
            let string = serde_json::to_string_pretty(
                &serde_json::from_str::<serde_json::Value>(
                    &fs::read_to_string(format!("tests/roundtrip/{}.json", stringify!($filename))).unwrap()
                ).unwrap()
            ).unwrap();

            let structured: Annotated<Value> = serde_json::from_str(&string).unwrap();
            let string2 = serde_json::to_string_pretty(&structured).unwrap();
            assert_eq_str!(string, string2);
        }
    }
}

create_roundtrip_test!(cocoa);
create_roundtrip_test!(cordova);
create_roundtrip_test!(dotnet);
create_roundtrip_test!(electron_main);
create_roundtrip_test!(electron_renderer);
create_roundtrip_test!(legacy_js_exception);
create_roundtrip_test!(legacy_js_message);
create_roundtrip_test!(legacy_js_onerror);
create_roundtrip_test!(legacy_js_promise);
create_roundtrip_test!(legacy_node_exception);
create_roundtrip_test!(legacy_node_express);
create_roundtrip_test!(legacy_node_message);
create_roundtrip_test!(legacy_node_onerror);
create_roundtrip_test!(legacy_node_promise);
create_roundtrip_test!(legacy_python);
create_roundtrip_test!(legacy_swift);
