use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::mem;
use std::ptr::{self, NonNull};

use crate::sync::atomic::{AtomicPtr, Ordering};
use crate::zalsa::MemoIngredientIndex;
use crate::zalsa::Zalsa;
use crate::DatabaseKeyIndex;

/// The "memo table" stores the memoized results of tracked function calls.
/// Every tracked function must take a salsa struct as its first argument
/// and memo tables are attached to those salsa structs as auxiliary data.
pub struct MemoTable {
    memos: Box<[MemoEntry]>,
}

impl MemoTable {
    /// Create a `MemoTable` with slots for memos from the provided `MemoTableTypes`.
    ///
    /// # Safety
    ///
    /// The created memo table must only be accessed with the same `MemoTableTypes`.
    pub unsafe fn new(types: &MemoTableTypes) -> Self {
        // Note that the safety invariant guarantees that any indices in-bounds for
        // this table are also in-bounds for its `MemoTableTypes`, as `MemoTableTypes`
        // is append-only.
        Self {
            memos: (0..types.len()).map(|_| MemoEntry::default()).collect(),
        }
    }

    /// Reset any memos in the table.
    ///
    /// Note that the memo entries should be freed manually before calling this function.
    pub fn reset(&mut self) {
        for memo in &mut self.memos {
            *memo = MemoEntry::default();
        }
    }
}

pub trait Memo: Any + Send + Sync {
    /// Removes the outputs that were created when this query ran. This includes
    /// tracked structs and specified queries.
    fn remove_outputs(&self, zalsa: &Zalsa, executor: DatabaseKeyIndex);

    /// Returns `true` if this memo should be serialized.
    #[cfg(feature = "persistence")]
    fn should_serialize(&self) -> bool;

    /// Returns memory usage information about the memoized value.
    #[cfg(feature = "salsa_unstable")]
    fn memory_usage(&self) -> crate::database::MemoInfo;
}

/// Data for a memoized entry.
/// This is a type-erased `Box<M>`, where `M` is the type of memo associated
/// with that particular ingredient index.
///
/// # Implementation note
///
/// Every entry is associated with some ingredient that has been added to the database.
/// That ingredient has a fixed type of values that it produces etc.
/// Therefore, once a given entry goes from `Empty` to `Full`,
/// the type-id associated with that entry should never change.
///
/// We take advantage of this and use an `AtomicPtr` to store the actual memo.
/// This allows us to store into the memo-entry without acquiring a write-lock.
/// However, using `AtomicPtr` means we cannot use a `Box<dyn Any>` or any other wide pointer.
/// Therefore, we hide the type by transmuting to `DummyMemo`; but we must then be very careful
/// when freeing `MemoEntryData` values to transmute things back. See the `Drop` impl for
/// [`MemoEntry`][] for details.
#[derive(Default, Debug)]
struct MemoEntry {
    /// An [`AtomicPtr`][] to a `Box<M>` for the erased memo type `M`
    atomic_memo: AtomicPtr<DummyMemo>,
}

#[derive(Clone, Copy, Debug)]
pub struct MemoEntryType {
    /// The `type_id` of the erased memo type `M`
    type_id: TypeId,

    /// A type-coercion function for the erased memo type `M`
    to_dyn_fn: fn(NonNull<DummyMemo>) -> NonNull<dyn Memo>,
}

impl MemoEntryType {
    fn to_dummy<M: Memo>(memo: NonNull<M>) -> NonNull<DummyMemo> {
        memo.cast()
    }

    unsafe fn from_dummy<M: Memo>(memo: NonNull<DummyMemo>) -> NonNull<M> {
        memo.cast()
    }

    const fn to_dyn_fn<M: Memo>() -> fn(NonNull<DummyMemo>) -> NonNull<dyn Memo> {
        let f: fn(NonNull<M>) -> NonNull<dyn Memo> = |x| x;

        // SAFETY: `M: Sized` and `DummyMemo: Sized`, as such they are ABI compatible behind a
        // `NonNull` making it safe to do type erasure.
        unsafe {
            mem::transmute::<
                fn(NonNull<M>) -> NonNull<dyn Memo>,
                fn(NonNull<DummyMemo>) -> NonNull<dyn Memo>,
            >(f)
        }
    }

    #[inline]
    pub fn of<M: Memo>() -> Self {
        Self {
            type_id: TypeId::of::<M>(),
            to_dyn_fn: Self::to_dyn_fn::<M>(),
        }
    }
}

/// Dummy placeholder type that we use when erasing the memo type `M` in [`MemoEntryData`][].
#[derive(Debug)]
struct DummyMemo;

impl Memo for DummyMemo {
    fn remove_outputs(&self, _zalsa: &Zalsa, _executor: DatabaseKeyIndex) {}

    #[cfg(feature = "persistence")]
    fn should_serialize(&self) -> bool {
        false
    }

    #[cfg(feature = "salsa_unstable")]
    fn memory_usage(&self) -> crate::database::MemoInfo {
        crate::database::MemoInfo {
            debug_name: "dummy",
            output: crate::database::SlotInfo {
                debug_name: "dummy",
                size_of_metadata: 0,
                size_of_fields: 0,
                heap_size_of_fields: None,
                memos: Vec::new(),
            },
        }
    }
}

#[derive(Default)]
pub struct MemoTableTypes {
    types: Vec<MemoEntryType>,
}

impl MemoTableTypes {
    pub(crate) fn set(
        &mut self,
        memo_ingredient_index: MemoIngredientIndex,
        memo_type: MemoEntryType,
    ) {
        self.types
            .insert(memo_ingredient_index.as_usize(), memo_type);
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    /// # Safety
    ///
    /// The types table must be the correct one of `memos`.
    #[inline]
    pub(crate) unsafe fn attach_memos<'a>(
        &'a self,
        memos: &'a MemoTable,
    ) -> MemoTableWithTypes<'a> {
        MemoTableWithTypes { types: self, memos }
    }

    /// # Safety
    ///
    /// The types table must be the correct one of `memos`.
    #[inline]
    pub(crate) unsafe fn attach_memos_mut<'a>(
        &'a self,
        memos: &'a mut MemoTable,
    ) -> MemoTableWithTypesMut<'a> {
        MemoTableWithTypesMut { types: self, memos }
    }
}

pub struct MemoTableWithTypes<'a> {
    types: &'a MemoTableTypes,
    memos: &'a MemoTable,
}

impl MemoTableWithTypes<'_> {
    pub(crate) fn insert<M: Memo>(
        self,
        memo_ingredient_index: MemoIngredientIndex,
        memo: NonNull<M>,
    ) -> Option<NonNull<M>> {
        let MemoEntry { atomic_memo } = self.memos.memos.get(memo_ingredient_index.as_usize())?;

        // SAFETY: Any indices that are in-bounds for the `MemoTable` are also in-bounds for its
        // corresponding `MemoTableTypes`, by construction.
        let type_ = unsafe {
            self.types
                .types
                .get_unchecked(memo_ingredient_index.as_usize())
        };

        // Verify that the we are casting to the correct type.
        if type_.type_id != TypeId::of::<M>() {
            type_assert_failed(memo_ingredient_index);
        }

        let old_memo = atomic_memo.swap(MemoEntryType::to_dummy(memo).as_ptr(), Ordering::AcqRel);

        // SAFETY: We asserted that the type is correct above.
        NonNull::new(old_memo).map(|old_memo| unsafe { MemoEntryType::from_dummy(old_memo) })
    }

    /// Returns a pointer to the memo at the given index, if one has been inserted.
    #[inline]
    pub(crate) fn get<M: Memo>(
        self,
        memo_ingredient_index: MemoIngredientIndex,
    ) -> Option<NonNull<M>> {
        let MemoEntry { atomic_memo } = self.memos.memos.get(memo_ingredient_index.as_usize())?;

        // SAFETY: Any indices that are in-bounds for the `MemoTable` are also in-bounds for its
        // corresponding `MemoTableTypes`, by construction.
        let type_ = unsafe {
            self.types
                .types
                .get_unchecked(memo_ingredient_index.as_usize())
        };

        // Verify that the we are casting to the correct type.
        if type_.type_id != TypeId::of::<M>() {
            type_assert_failed(memo_ingredient_index);
        }

        NonNull::new(atomic_memo.load(Ordering::Acquire))
            // SAFETY: We asserted that the type is correct above.
            .map(|memo| unsafe { MemoEntryType::from_dummy(memo) })
    }

    #[cfg(feature = "salsa_unstable")]
    pub(crate) fn memory_usage(&self) -> Vec<crate::database::MemoInfo> {
        let mut memory_usage = Vec::new();
        for (index, memo) in self.memos.memos.iter().enumerate() {
            let Some(memo) = NonNull::new(memo.atomic_memo.load(Ordering::Acquire)) else {
                continue;
            };

            let Some(type_) = self.types.types.get(index) else {
                continue;
            };

            // SAFETY: The `TypeId` is asserted in `insert()`.
            let dyn_memo: &dyn Memo = unsafe { (type_.to_dyn_fn)(memo).as_ref() };
            memory_usage.push(dyn_memo.memory_usage());
        }

        memory_usage
    }
}

pub struct MemoTableWithTypesMut<'a> {
    types: &'a MemoTableTypes,
    memos: &'a mut MemoTable,
}

impl MemoTableWithTypesMut<'_> {
    /// Calls `f` on the memo at `memo_ingredient_index`.
    ///
    /// If the memo is not present, `f` is not called.
    pub(crate) fn map_memo<M: Memo>(
        self,
        memo_ingredient_index: MemoIngredientIndex,
        f: impl FnOnce(&mut M),
    ) {
        let Some(MemoEntry { atomic_memo }) =
            self.memos.memos.get_mut(memo_ingredient_index.as_usize())
        else {
            return;
        };

        // SAFETY: Any indices that are in-bounds for the `MemoTable` are also in-bounds for its
        // corresponding `MemoTableTypes`, by construction.
        let type_ = unsafe {
            self.types
                .types
                .get_unchecked(memo_ingredient_index.as_usize())
        };

        // Verify that the we are casting to the correct type.
        if type_.type_id != TypeId::of::<M>() {
            type_assert_failed(memo_ingredient_index);
        }

        let Some(memo) = NonNull::new(*atomic_memo.get_mut()) else {
            return;
        };

        // SAFETY: We asserted that the type is correct above.
        f(unsafe { MemoEntryType::from_dummy(memo).as_mut() });
    }

    #[cfg(feature = "persistence")]
    pub(crate) fn insert<M: Memo>(
        &mut self,
        memo_ingredient_index: MemoIngredientIndex,
        memo: NonNull<M>,
    ) {
        let MemoEntry { atomic_memo } = &mut self.memos.memos[memo_ingredient_index.as_usize()];

        // SAFETY: Any indices that are in-bounds for the `MemoTable` are also in-bounds for its
        // corresponding `MemoTableTypes`, by construction.
        let type_ = unsafe {
            self.types
                .types
                .get_unchecked(memo_ingredient_index.as_usize())
        };

        // Verify that the we are casting to the correct type.
        if type_.type_id != TypeId::of::<M>() {
            type_assert_failed(memo_ingredient_index);
        }

        *atomic_memo.get_mut() = MemoEntryType::to_dummy(memo).as_ptr();
    }

    /// To drop an entry, we need its type, so we don't implement `Drop`, and instead have this method.
    ///
    /// Note that calling this multiple times is safe, dropping an uninitialized entry is a no-op.
    ///
    /// # Safety
    ///
    /// The caller needs to make sure to not call this function until no more references into
    /// the database exist as there may be outstanding borrows into the pointer contents.
    #[inline]
    pub unsafe fn drop(&mut self) {
        let types = self.types.types.iter();
        for (type_, memo) in std::iter::zip(types, &mut self.memos.memos) {
            // SAFETY: The types match as per our constructor invariant.
            unsafe { memo.take(type_) };
        }
    }

    /// # Safety
    ///
    /// The caller needs to make sure to not call this function until no more references into
    /// the database exist as there may be outstanding borrows into the pointer contents.
    pub(crate) unsafe fn take_memos(
        &mut self,
        mut f: impl FnMut(MemoIngredientIndex, Box<dyn Memo>),
    ) {
        self.memos
            .memos
            .iter_mut()
            .zip(self.types.types.iter())
            .enumerate()
            .filter_map(|(index, (memo, type_))| {
                // SAFETY: The types match as per our constructor invariant.
                let memo = unsafe { memo.take(type_)? };
                Some((MemoIngredientIndex::from_usize(index), memo))
            })
            .for_each(|(index, memo)| f(index, memo));
    }
}

/// This function is explicitly outlined to avoid debug machinery in the hot-path.
#[cold]
#[inline(never)]
fn type_assert_failed(memo_ingredient_index: MemoIngredientIndex) -> ! {
    panic!("inconsistent type-id for `{memo_ingredient_index:?}`")
}

impl MemoEntry {
    /// # Safety
    ///
    /// The type must match.
    #[inline]
    unsafe fn take(&mut self, type_: &MemoEntryType) -> Option<Box<dyn Memo>> {
        let memo = mem::replace(self.atomic_memo.get_mut(), ptr::null_mut());
        let memo = NonNull::new(memo)?;
        // SAFETY: Our preconditions.
        Some(unsafe { Box::from_raw((type_.to_dyn_fn)(memo).as_ptr()) })
    }
}

impl Drop for DummyMemo {
    fn drop(&mut self) {
        unreachable!("should never get here")
    }
}

impl std::fmt::Debug for MemoTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoTable").finish_non_exhaustive()
    }
}

#[cfg(feature = "persistence")]
pub(crate) mod persistence {
    use super::{MemoEntry, MemoTableWithTypes};
    use crate::plumbing::Ingredient;
    use crate::table::memo::{Memo, MemoTableWithTypesMut};
    use crate::zalsa::{MemoIngredientIndex, Zalsa};
    use crate::IngredientIndex;

    use serde::ser::SerializeMap;
    use serde::{de, Deserialize};

    use std::fmt;
    use std::marker::PhantomData;
    use std::ptr::NonNull;
    use std::sync::atomic::Ordering;

    #[derive(serde::Serialize)]
    pub(crate) struct SerializeValueWithMemos<'db, V> {
        inner: &'db V,
        memos: SerializeMemos<'db>,
    }

    impl<'db, V> SerializeValueWithMemos<'db, V> {
        pub(crate) fn new(
            zalsa: &'db Zalsa,
            inner: &'db V,
            struct_ingredient_index: IngredientIndex,
            memos: MemoTableWithTypes<'db>,
        ) -> Self {
            Self {
                inner,
                memos: SerializeMemos {
                    zalsa,
                    memos,
                    struct_ingredient_index,
                },
            }
        }
    }

    pub(crate) struct SerializeMemos<'db> {
        pub(crate) zalsa: &'db Zalsa,
        pub(crate) memos: MemoTableWithTypes<'db>,
        pub(crate) struct_ingredient_index: IngredientIndex,
    }

    impl serde::Serialize for SerializeMemos<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let Self {
                zalsa,
                memos,
                struct_ingredient_index,
            } = self;

            let mut map = serializer.serialize_map(None)?;

            for (memo_ingredient_index, MemoEntry { atomic_memo }) in
                memos.memos.memos.iter().enumerate()
            {
                // SAFETY: Any indices that are in-bounds for the `MemoTable` are also in-bounds for its
                // corresponding `MemoTableTypes`, by construction.
                let type_ = unsafe { memos.types.types.get_unchecked(memo_ingredient_index) };

                let Some(memo) = NonNull::new(atomic_memo.load(Ordering::Acquire)) else {
                    continue;
                };

                // SAFETY: `memo` is non-null.
                let memo: &dyn Memo = unsafe { (type_.to_dyn_fn)(memo).as_ref() };
                if !memo.should_serialize() {
                    continue;
                }

                let ingredient_index = zalsa.ingredient_index_for_memo(
                    *struct_ingredient_index,
                    MemoIngredientIndex::from_usize(memo_ingredient_index),
                );

                // TODO: Add a `serialize` method directly to the `Memo` trait, to avoid
                // looking up the ingredient here.
                let ingredient = zalsa.lookup_ingredient(ingredient_index);

                map.serialize_entry(
                    &memo_ingredient_index,
                    &SerializeMemo {
                        zalsa,
                        ingredient,
                        memo,
                    },
                )?;
            }

            map.end()
        }
    }

    struct SerializeMemo<'db> {
        zalsa: &'db Zalsa,
        memo: &'db dyn Memo,
        ingredient: &'db dyn Ingredient,
    }

    impl serde::Serialize for SerializeMemo<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let Self {
                memo,
                zalsa,
                ingredient,
            } = *self;

            let mut result = None;
            let mut serializer = Some(serializer);

            ingredient.serialize_memo(zalsa, memo, &mut |serialize_memo| {
                let serializer = serializer.take().expect(
                    "`Ingredient::serialize_memo` must invoke the serialization callback only once",
                );

                result = Some(erased_serde::serialize(&serialize_memo, serializer))
            });

            result.expect("`Ingredient::serialize_memo` must invoke the serialization callback")
        }
    }

    pub(crate) struct DeserializeValueWithMemos<'table, 'db, T> {
        memos: DeserializeMemos<'table, 'db>,
        _value: PhantomData<T>,
    }

    impl<'table, 'db, T> DeserializeValueWithMemos<'table, 'db, T> {
        pub(crate) fn new(
            zalsa: &'db Zalsa,
            struct_ingredient_index: IngredientIndex,
            memos: &'table mut MemoTableWithTypesMut<'db>,
        ) -> Self {
            Self {
                memos: DeserializeMemos {
                    zalsa,
                    memos,
                    struct_ingredient_index,
                },
                _value: PhantomData,
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(field_identifier, rename_all = "lowercase")]
    enum ValueWithMemosField {
        Inner,
        Memos,
    }

    impl<'de, T> de::DeserializeSeed<'de> for DeserializeValueWithMemos<'_, '_, T>
    where
        T: Deserialize<'de>,
    {
        type Value = T;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            // Note that we have to deserialize using a manual visitor here because the
            // `Deserialize` derive does not support fields that use `DeserializeSeed`.
            deserializer.deserialize_struct("Value", &["inner", "memos"], self)
        }
    }

    impl<'de, T> serde::de::Visitor<'de> for DeserializeValueWithMemos<'_, '_, T>
    where
        T: Deserialize<'de>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("struct Value")
        }

        fn visit_map<V>(mut self, mut map: V) -> Result<T, V::Error>
        where
            V: serde::de::MapAccess<'de>,
        {
            let mut inner = None;
            let mut memos = None;

            while let Some(key) = map.next_key()? {
                match key {
                    ValueWithMemosField::Inner => {
                        if inner.is_some() {
                            return Err(serde::de::Error::duplicate_field("inner"));
                        }

                        inner = Some(map.next_value()?);
                    }
                    ValueWithMemosField::Memos => {
                        if memos.is_some() {
                            return Err(serde::de::Error::duplicate_field("memos"));
                        }

                        memos = Some(map.next_value_seed(&mut self.memos)?);
                    }
                }
            }

            let inner = inner.ok_or_else(|| serde::de::Error::missing_field("inner"))?;
            let () = memos.ok_or_else(|| serde::de::Error::missing_field("memso"))?;

            Ok(inner)
        }
    }

    pub(crate) struct DeserializeMemos<'table, 'db> {
        pub(crate) zalsa: &'db Zalsa,
        pub(crate) struct_ingredient_index: IngredientIndex,
        pub(crate) memos: &'table mut MemoTableWithTypesMut<'db>,
    }

    impl<'de> de::DeserializeSeed<'de> for &mut DeserializeMemos<'_, '_> {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_map(self)
        }
    }

    impl<'de> de::Visitor<'de> for &mut DeserializeMemos<'_, '_> {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: de::MapAccess<'de>,
        {
            let DeserializeMemos {
                zalsa,
                struct_ingredient_index,
                ref mut memos,
            } = *self;

            while let Some(memo_ingredient_index) = access.next_key::<usize>()? {
                let ingredient_index = zalsa.ingredient_index_for_memo(
                    struct_ingredient_index,
                    MemoIngredientIndex::from_usize(memo_ingredient_index),
                );

                let ingredient = zalsa.lookup_ingredient(ingredient_index);
                access.next_value_seed(DeserializeMemo {
                    memos,
                    ingredient,
                    struct_ingredient_index,
                })?;
            }

            Ok(())
        }
    }

    struct DeserializeMemo<'table, 'db> {
        memos: &'table mut MemoTableWithTypesMut<'db>,
        ingredient: &'db dyn Ingredient,
        struct_ingredient_index: IngredientIndex,
    }

    impl<'de> serde::de::DeserializeSeed<'de> for DeserializeMemo<'_, '_> {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            let Self {
                memos,
                ingredient,
                struct_ingredient_index,
            } = self;

            let deserializer = &mut <dyn erased_serde::Deserializer>::erase(deserializer);

            ingredient
                .deserialize_memo(memos, struct_ingredient_index, deserializer)
                .map_err(serde::de::Error::custom)
        }
    }
}
