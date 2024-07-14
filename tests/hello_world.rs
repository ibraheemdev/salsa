//! Test that a `tracked` fn on a `salsa::input`
//! compiles and executes successfully.

mod common;
use common::{HasLogger, Logger};

use expect_test::expect;
use test_log::test;

#[salsa::db]
trait Db: HasLogger {}

// #[salsa::input]
// struct MyInput {
//     field: u32,
// }

salsa::plumbing::setup_input! {
    attrs: [],
    vis: ,
    Struct: MyInput,
    new_fn: new,
    field_options: [(clone, not_applicable)],
    field_ids: [field],
    field_setter_ids: [set_field],
    field_tys: [u32],
    field_indices: [0],
    num_fields: 1,
    unused_names: [
        zalsa1,
        zalsa_struct1,
        Configuration1,
        CACHE1,
        Db1,
        NonNull1,
        Revision1,
        ValueStruct1,
    ]
}

#[salsa::tracked]
fn final_result(db: &dyn Db, input: MyInput) -> u32 {
    db.push_log(format!("final_result({:?})", input));
    intermediate_result(db, input).field(db) * 2
}

#[salsa::tracked]
struct MyTracked<'db> {
    field: u32,
}

#[salsa::tracked]
fn intermediate_result<'db>(db: &'db dyn Db, input: MyInput) -> MyTracked<'db> {
    db.push_log(format!("intermediate_result({:?})", input));
    MyTracked::new(db, input.field(db) / 2)
}

#[salsa::db]
#[derive(Default)]
struct Database {
    storage: salsa::Storage<Self>,
    logger: Logger,
}

impl salsa::Database for Database {}

impl Db for Database {}

impl HasLogger for Database {
    fn logger(&self) -> &Logger {
        &self.logger
    }
}

#[test]
fn execute() {
    let mut db = Database::default();

    let input = MyInput::new(&db, 22);
    assert_eq!(final_result(&db, input), 22);
    db.assert_logs(expect![[r#"
        [
            "final_result(MyInput { [salsa id]: 0 })",
            "intermediate_result(MyInput { [salsa id]: 0 })",
        ]"#]]);

    // Intermediate result is the same, so final result does
    // not need to be recomputed:
    input.set_field(&mut db).to(23);
    assert_eq!(final_result(&db, input), 22);
    db.assert_logs(expect![[r#"
        [
            "intermediate_result(MyInput { [salsa id]: 0 })",
        ]"#]]);

    input.set_field(&mut db).to(24);
    assert_eq!(final_result(&db, input), 24);
    db.assert_logs(expect![[r#"
        [
            "intermediate_result(MyInput { [salsa id]: 0 })",
            "final_result(MyInput { [salsa id]: 0 })",
        ]"#]]);
}

/// Create and mutate a distinct input. No re-execution required.
#[test]
fn red_herring() {
    let mut db = Database::default();

    let input = MyInput::new(&db, 22);
    assert_eq!(final_result(&db, input), 22);
    db.assert_logs(expect![[r#"
        [
            "final_result(MyInput { [salsa id]: 0 })",
            "intermediate_result(MyInput { [salsa id]: 0 })",
        ]"#]]);

    // Create a distinct input and mutate it.
    // This will trigger a new revision in the database
    // but shouldn't actually invalidate our existing ones.
    let input2 = MyInput::new(&db, 44);
    input2.set_field(&mut db).to(66);

    // Re-run the query on the original input. Nothing re-executes!
    assert_eq!(final_result(&db, input), 22);
    db.assert_logs(expect![[r#"
        []"#]]);
}
