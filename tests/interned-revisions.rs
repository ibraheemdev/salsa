//! Test that a `tracked` fn on a `salsa::input`
//! compiles and executes successfully.

mod common;
use common::LogDatabase;
use expect_test::expect;
use salsa::{Database, Setter};
use test_log::test;

#[salsa::input]
struct Input {
    field1: usize,
}

#[salsa::interned]
struct Interned<'db> {
    field1: usize,
}

#[test]
fn test_intern_new() {
    #[salsa::tracked]
    fn function<'db>(db: &'db dyn Database, input: Input) -> Interned<'db> {
        Interned::new(db, input.field1(db))
    }

    let mut db = common::EventLoggerDatabase::default();
    let input = Input::new(&db, 0);

    let result_in_rev_1 = function(&db, input);
    assert_eq!(result_in_rev_1.field1(&db), 0);

    // Modify the input to force a new value to be created.
    input.set_field1(&mut db).to(1);

    let result_in_rev_2 = function(&db, input);
    assert_eq!(result_in_rev_2.field1(&db), 1);

    db.assert_logs(expect![[r#"
        [
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(400)), revision: R1 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(401)), revision: R2 }",
        ]"#]]);
}

#[test]
fn test_reintern() {
    #[salsa::tracked]
    fn function(db: &dyn Database, input: Input) -> Interned<'_> {
        let _ = input.field1(db);
        Interned::new(db, 0)
    }

    let mut db = common::EventLoggerDatabase::default();

    let input = Input::new(&db, 0);
    let result_in_rev_1 = function(&db, input);
    db.assert_logs(expect![[r#"
        [
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(400)), revision: R1 }",
        ]"#]]);

    assert_eq!(result_in_rev_1.field1(&db), 0);

    // Modify the input to force the value to be re-interned.
    input.set_field1(&mut db).to(1);

    let result_in_rev_2 = function(&db, input);
    db.assert_logs(expect![[r#"
        [
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidValidateInternedValue { key: Interned(Id(400)), revision: R2 }",
        ]"#]]);

    assert_eq!(result_in_rev_2.field1(&db), 0);
}

#[test]
fn test_durability() {
    #[salsa::tracked]
    fn function<'db>(db: &'db dyn Database, _input: Input) -> Interned<'db> {
        Interned::new(db, 0)
    }

    let mut db = common::EventLoggerDatabase::default();
    let input = Input::new(&db, 0);

    let result_in_rev_1 = function(&db, input);
    assert_eq!(result_in_rev_1.field1(&db), 0);

    // Modify the input to bump the revision without re-interning the value, as there
    // is no read dependency.
    input.set_field1(&mut db).to(1);

    let result_in_rev_2 = function(&db, input);
    assert_eq!(result_in_rev_2.field1(&db), 0);

    db.assert_logs(expect![[r#"
        [
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(400)), revision: R1 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "DidValidateMemoizedValue { database_key: function(Id(0)) }",
        ]"#]]);
}

#[test]
fn test_reuse() {
    #[salsa::tracked]
    fn function<'db>(db: &'db dyn Database, input: Input) -> Interned<'db> {
        Interned::new(db, input.field1(db))
    }

    let mut db = common::EventLoggerDatabase::default();
    let input = Input::new(&db, 0);

    let result = function(&db, input);
    assert_eq!(result.field1(&db), 0);

    // Modify the input to bump the revision and intern a new value.
    //
    // The slot will not be reused for the first few revisions, but after
    // that we should not allocate any more slots.
    for i in 1..10 {
        input.set_field1(&mut db).to(i);

        let result = function(&db, input);
        assert_eq!(result.field1(&db), i);
    }

    // Values that have been reused should be re-interned.
    for i in 1..10 {
        let result = function(&db, Input::new(&db, i));
        assert_eq!(result.field1(&db), i);
    }

    db.assert_logs(expect![[r#"
        [
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(400)), revision: R1 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(401)), revision: R2 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidInternValue { key: Interned(Id(402)), revision: R3 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(400)), revision: R4 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(401)), revision: R5 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(402)), revision: R6 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(400)), revision: R7 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(401)), revision: R8 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(402)), revision: R9 }",
            "DidSetCancellationFlag",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(0)) }",
            "DidReuseInternedValue { key: Interned(Id(400)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(1)) }",
            "DidReuseInternedValue { key: Interned(Id(401)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(2)) }",
            "DidInternValue { key: Interned(Id(403)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(3)) }",
            "DidInternValue { key: Interned(Id(404)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(4)) }",
            "DidInternValue { key: Interned(Id(405)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(5)) }",
            "DidInternValue { key: Interned(Id(406)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(6)) }",
            "DidInternValue { key: Interned(Id(407)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(7)) }",
            "DidInternValue { key: Interned(Id(408)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(8)) }",
            "DidValidateInternedValue { key: Interned(Id(402)), revision: R10 }",
            "WillCheckCancellation",
            "WillExecute { database_key: function(Id(9)) }",
        ]"#]]);
}
