use std::{
    any::{Any, TypeId},
    fmt::Debug,
    mem::ManuallyDrop,
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::{zalsa::MemoIngredientIndex, zalsa_local::QueryOrigin};

/// The "memo table" stores the memoized results of tracked function calls.
/// Every tracked function must take a salsa struct as its first argument
/// and memo tables are attached to those salsa structs as auxiliary data.
#[derive(Default)]
pub(crate) struct MemoTable {
    memos: boxcar::DefaultVec<MemoEntry>,
}

pub(crate) trait Memo: Any + Send + Sync + Debug {
    /// Returns the `origin` of this memo
    fn origin(&self) -> &QueryOrigin;
}

/// Wraps the data stored for a memoized entry.
/// This struct has a customized Drop that will
/// ensure that its `data` field is properly freed.
#[derive(Default)]
struct MemoEntry {
    data: AtomicPtr<DummyMemo>,
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
/// However, using `AtomicPtr` means we cannot use a `Arc<dyn Any>` or any other wide pointer.
/// Therefore, we hide the type by transmuting to `DummyMemo`; but we must then be very careful
/// when freeing `MemoEntryData` values to transmute things back. See the `Drop` impl for
/// [`MemoEntry`][] for details.
#[repr(C)]
pub(crate) struct MemoEntryData<M: ?Sized> {
    /// The `type_id` of the erased memo type `M`
    type_id: TypeId,

    /// A pointer to `std::mem::drop::<Box<M>>` for the erased memo type `M`
    to_dyn_fn: fn(Box<DummyMemo>) -> Box<MemoEntryData<dyn Memo>>,

    /// An `Box<M>` for the erased memo type `M`
    pub(crate) memo: M,
}

impl<'a, M: Memo> MemoEntryData<M> {
    pub(crate) fn new(memo: M) -> Self
    where
        M: 'a,
    {
        MemoEntryData {
            type_id: TypeId::of::<M>(),
            to_dyn_fn: MemoTable::to_dyn_fn::<M>(),
            memo,
        }
    }
}

/// Dummy placeholder type that we use when erasing the memo type `M` in [`MemoEntryData`][].
type DummyMemo = MemoEntryData<()>;

impl MemoTable {
    fn to_dummy<M: Memo>(memo: Box<MemoEntryData<M>>) -> Box<DummyMemo> {
        unsafe { std::mem::transmute::<Box<MemoEntryData<M>>, Box<DummyMemo>>(memo) }
    }

    unsafe fn from_dummy<M: Memo>(memo: Box<DummyMemo>) -> Box<MemoEntryData<M>> {
        unsafe { std::mem::transmute::<Box<DummyMemo>, Box<MemoEntryData<M>>>(memo) }
    }

    unsafe fn from_dummy_ptr<M: Memo>(memo: *const DummyMemo) -> *const MemoEntryData<M> {
        memo.cast()
    }

    unsafe fn from_dummy_mut<M: Memo>(memo: &mut DummyMemo) -> &mut MemoEntryData<M> {
        unsafe { std::mem::transmute::<&mut DummyMemo, &mut MemoEntryData<M>>(memo) }
    }

    fn to_dyn_fn<M: Memo>() -> fn(Box<DummyMemo>) -> Box<MemoEntryData<dyn Memo>> {
        let f: fn(Box<MemoEntryData<M>>) -> Box<MemoEntryData<dyn Memo>> = |x| x;

        unsafe {
            std::mem::transmute::<
                fn(Box<MemoEntryData<M>>) -> Box<MemoEntryData<dyn Memo>>,
                fn(Box<DummyMemo>) -> Box<MemoEntryData<dyn Memo>>,
            >(f)
        }
    }

    /// # Safety
    ///
    /// The caller needs to make sure to not drop the returned value until no more references into
    /// the database exist as there may be outstanding borrows into the `Arc` contents.
    pub(crate) unsafe fn insert<M: Memo>(
        &self,
        memo_ingredient_index: MemoIngredientIndex,
        new_memo: Box<MemoEntryData<M>>,
    ) -> Option<ManuallyDrop<Box<MemoEntryData<M>>>> {
        // If the memo slot is already occupied, it must already have the
        // right type info etc, and we only need the read-lock.
        let memo_entry = match self.memos.get(memo_ingredient_index.as_usize()) {
            Some(memo_entry) => memo_entry,
            None => self.create_entry_cold::<M>(memo_ingredient_index),
        };

        let old_memo = NonNull::new(memo_entry.data.swap(
            Box::into_raw(Self::to_dummy(new_memo)) as *mut _,
            Ordering::AcqRel,
        ))?;

        // TODO: Box is unsound here because it's aliased
        let old_memo = unsafe { Self::from_dummy(Box::from_raw(old_memo.as_ptr())) };

        assert_eq!(
            (*old_memo).type_id,
            TypeId::of::<M>(),
            "inconsistent type-id for `{memo_ingredient_index:?}`"
        );

        return Some(ManuallyDrop::new(old_memo));
    }

    /// # Safety
    ///
    /// The caller needs to make sure to not drop the returned value until no more references into
    /// the database exist as there may be outstanding borrows into the `Arc` contents.
    #[cold]
    #[inline(never)]
    unsafe fn create_entry_cold<M: Memo>(
        &self,
        memo_ingredient_index: MemoIngredientIndex,
    ) -> &MemoEntry {
        self.memos.get_or_default(memo_ingredient_index.as_usize())
    }

    pub(crate) fn get<M: Memo>(
        &self,
        memo_ingredient_index: MemoIngredientIndex,
    ) -> Option<*const MemoEntryData<M>> {
        let memo_entry = self.memos.get(memo_ingredient_index.as_usize())?;

        let memo_data = unsafe {
            Self::from_dummy_ptr(NonNull::new(memo_entry.data.load(Ordering::Acquire))?.as_ptr())
        };

        assert_eq!(
            unsafe { (*memo_data).type_id },
            TypeId::of::<M>(),
            "inconsistent type-id for `{memo_ingredient_index:?}`"
        );

        // SAFETY: type_id check asserted above
        Some(memo_data)
    }

    /// Calls `f` on the memo at `memo_ingredient_index` and replaces the memo with the result of `f`.
    /// If the memo is not present, `f` is not called.
    pub(crate) fn update_memo<M: Memo>(
        &mut self,
        memo_ingredient_index: MemoIngredientIndex,
        f: impl FnOnce(&mut M),
    ) {
        let Some(memo_entry) = self.memos.get_mut(memo_ingredient_index.as_usize()) else {
            return;
        };

        let Some(old_memo) = NonNull::new(*memo_entry.data.get_mut()) else {
            return;
        };

        let old_memo = unsafe { Self::from_dummy_mut(&mut *old_memo.as_ptr()) };

        assert_eq!(
            old_memo.type_id,
            TypeId::of::<M>(),
            "inconsistent type-id for `{memo_ingredient_index:?}`"
        );

        // arc-swap does not expose accessing the interior mutably at all unfortunately
        // https://github.com/vorner/arc-swap/issues/131
        // so we are required to allocate a new arc within `f` instead of being able
        // to swap out the interior
        // SAFETY: type_id check asserted above
        f(&mut old_memo.memo);
    }

    /// # Safety
    ///
    /// The caller needs to make sure to not drop the returned value until no more references into
    /// the database exist as there may be outstanding borrows into the `Arc` contents.
    pub(crate) unsafe fn into_memos(
        self,
    ) -> impl Iterator<
        Item = (
            MemoIngredientIndex,
            ManuallyDrop<Box<MemoEntryData<dyn Memo>>>,
        ),
    > {
        let mut i = 0;
        let mut memos = ManuallyDrop::new(self.memos.into_iter());

        std::iter::from_fn(move || {
            let mut memo = memos.next()?;

            i += 1;
            let data = NonNull::new(std::mem::replace(memo.data.get_mut(), ptr::null_mut()))?;
            let memo: Box<DummyMemo> = Box::from_raw(data.as_ptr());

            return Some((
                MemoIngredientIndex::from_usize(i - 1),
                ManuallyDrop::new((memo.to_dyn_fn)(memo)),
            ));
        })
    }
}

impl Drop for MemoEntry {
    fn drop(&mut self) {
        let memo = *self.data.get_mut();
        if !memo.is_null() {
            let to_dyn_fn = unsafe { (*memo).to_dyn_fn };
            let memo = unsafe { Box::from_raw(memo) };
            std::mem::drop(to_dyn_fn(memo))
        }
    }
}

impl std::fmt::Debug for MemoTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoTable").finish()
    }
}
