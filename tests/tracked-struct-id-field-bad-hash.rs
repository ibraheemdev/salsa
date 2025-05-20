//! Test for a tracked struct where an untracked field has a
//! very poorly chosen hash impl (always returns 0).
//! This demonstrates that the untracked fields on a struct
//! can change values and yet the struct can have the same
//! id (because struct ids are based on the *hash* of the
//! untracked fields).

use salsa::{Database as Db, Setter};
use test_log::test;

#[salsa::input]
struct MyInput {
    field: bool,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct BadHash {
    field: bool,
}

impl From<bool> for BadHash {
    fn from(value: bool) -> Self {
        Self { field: value }
    }
}

impl std::hash::Hash for BadHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_i16(0);
    }
}

#[salsa::tracked]
struct MyTracked<'db> {
    field: BadHash,
}

#[salsa::tracked]
fn the_fn(db: &dyn Db, input: MyInput) {
    let tracked0 = MyTracked::new(db, BadHash::from(input.field(db)));
    assert_eq!(tracked0.field(db).field, input.field(db));
}

#[test]
fn execute() {
    let mut db = salsa::DatabaseImpl::new();

    let input = MyInput::new(&db, true);
    the_fn(&db, input);
    input.set_field(&mut db).to(false);
    the_fn(&db, input);
}

#[salsa::tracked]
fn create_tracked<'db>(db: &'db dyn Db, input: MyInput) -> MyTracked<'db> {
    MyTracked::new(db, BadHash::from(input.field(db)))
}

#[salsa::tracked]
fn with_tracked<'db>(db: &'db dyn Db, tracked: MyTracked<'db>) -> bool {
    tracked.field(db).field
}

#[test]
// Because tracked struct IDs are generational, the ID of the second tracked struct ends
// up being different than the ID of the first. However, this test still has precedence
// and represents valid behavior for IDs that get leaked.
#[ignore]
fn dependent_query() {
    let mut db = salsa::DatabaseImpl::new();

    let input = MyInput::new(&db, true);
    let tracked = create_tracked(&db, input);
    assert!(with_tracked(&db, tracked));

    input.set_field(&mut db).to(false);

    // We now re-run the query that creates the tracked struct.
    // Salsa will re-use the `MyTracked` struct from the previous revision
    // because it thinks it is unchanged because of `BadHash`'s bad hash function.
    // That's why Salsa updates the `MyTracked` struct in-place and the struct
    // should be considered re-created even though it still has the same id.
    let tracked = create_tracked(&db, input);
    assert!(!with_tracked(&db, tracked));
}
