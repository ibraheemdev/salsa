use crossbeam_queue::SegQueue;

use super::{memo::BoxMemo, Configuration};

/// Stores the list of memos that have been deleted so they can be freed
/// once the next revision starts. See the comment on the field
/// `deleted_entries` of [`FunctionIngredient`][] for more details.
pub(super) struct DeletedEntries<C: Configuration> {
    seg_queue: SegQueue<BoxMemo<'static, C>>,
}

impl<C: Configuration> Default for DeletedEntries<C> {
    fn default() -> Self {
        Self {
            seg_queue: Default::default(),
        }
    }
}

impl<C: Configuration> DeletedEntries<C> {
    pub(super) fn push(&self, memo: BoxMemo<'_, C>) {
        let memo = unsafe { std::mem::transmute::<BoxMemo<'_, C>, BoxMemo<'static, C>>(memo) };
        self.seg_queue.push(memo);
    }
}
