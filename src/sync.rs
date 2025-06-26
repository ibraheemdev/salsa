pub use shim::*;

#[cfg(feature = "shuttle")]
pub mod shim {
    pub use shuttle::sync::*;
    pub use shuttle::{thread, thread_local};

    pub mod papaya {
        use std::hash::{BuildHasher, Hash};
        use std::marker::PhantomData;

        pub struct HashMap<K, V, S>(super::Mutex<std::collections::HashMap<K, V, S>>);

        impl<K, V, S: Default> Default for HashMap<K, V, S> {
            fn default() -> Self {
                Self(super::Mutex::default())
            }
        }

        pub struct LocalGuard<'a>(PhantomData<&'a ()>);

        impl<K, V, S> HashMap<K, V, S>
        where
            K: Eq + Hash,
            V: Clone,
            S: BuildHasher,
        {
            pub fn guard(&self) -> LocalGuard<'_> {
                LocalGuard(PhantomData)
            }

            pub fn get(&self, key: &K, _guard: &LocalGuard<'_>) -> Option<V> {
                self.0.lock().get(key).cloned()
            }

            pub fn insert(&self, key: K, value: V, _guard: &LocalGuard<'_>) {
                self.0.lock().insert(key, value);
            }
        }
    }

    /// A wrapper around shuttle's `Mutex` to mirror parking-lot's API.
    #[derive(Default, Debug)]
    pub struct Mutex<T>(shuttle::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub const fn new(value: T) -> Mutex<T> {
            Mutex(shuttle::sync::Mutex::new(value))
        }

        pub fn lock(&self) -> MutexGuard<'_, T> {
            self.0.lock().unwrap()
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap()
        }
    }

    /// A wrapper around shuttle's `RwLock` to mirror parking-lot's API.
    #[derive(Default, Debug)]
    pub struct RwLock<T>(shuttle::sync::RwLock<T>);

    impl<T> RwLock<T> {
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.0.read().unwrap()
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.0.write().unwrap()
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap()
        }
    }

    /// A wrapper around shuttle's `Condvar` to mirror parking-lot's API.
    #[derive(Default, Debug)]
    pub struct Condvar(shuttle::sync::Condvar);

    impl Condvar {
        // We cannot match parking-lot identically because shuttle's version takes ownership of the `MutexGuard`.
        pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
            self.0.wait(guard).unwrap()
        }

        pub fn notify_one(&self) {
            self.0.notify_one();
        }

        pub fn notify_all(&self) {
            self.0.notify_all();
        }
    }

    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;

    /// A polyfill for `std::sync::OnceLock`.
    pub struct OnceLock<T>(Mutex<bool>, UnsafeCell<MaybeUninit<T>>);

    impl<T> Default for OnceLock<T> {
        fn default() -> Self {
            OnceLock::new()
        }
    }

    impl<T> OnceLock<T> {
        pub const fn new() -> OnceLock<T> {
            OnceLock(Mutex::new(false), UnsafeCell::new(MaybeUninit::uninit()))
        }

        pub fn get(&self) -> Option<&T> {
            let initialized = self.0.lock();
            if *initialized {
                // SAFETY: The value is initialized and write-once.
                Some(unsafe { (*self.1.get()).assume_init_ref() })
            } else {
                None
            }
        }

        pub fn get_or_init<F>(&self, f: F) -> &T
        where
            F: FnOnce() -> T,
        {
            let _ = self.set_with(f);
            self.get().unwrap()
        }

        pub fn set(&self, value: T) -> Result<(), T> {
            self.set_with(|| value).map_err(|f| f())
        }

        fn set_with<F>(&self, f: F) -> Result<(), F>
        where
            F: FnOnce() -> T,
        {
            let mut initialized = self.0.lock();
            if *initialized {
                return Err(f);
            }

            // SAFETY: We hold the lock.
            unsafe { self.1.get().write(MaybeUninit::new(f())) }
            *initialized = true;

            Ok(())
        }
    }

    impl<T> From<T> for OnceLock<T> {
        fn from(value: T) -> OnceLock<T> {
            OnceLock(Mutex::new(true), UnsafeCell::new(MaybeUninit::new(value)))
        }
    }

    // SAFETY: Mirroring `std::sync::OnceLock`.
    unsafe impl<T: Send> Send for OnceLock<T> {}
    // SAFETY: Mirroring `std::sync::OnceLock`.
    unsafe impl<T: Sync + Send> Sync for OnceLock<T> {}
}

#[cfg(not(feature = "shuttle"))]
pub mod shim {
    pub use parking_lot::{Mutex, MutexGuard, RwLock};
    pub use std::sync::*;
    pub use std::{thread, thread_local};

    pub mod atomic {
        pub use portable_atomic::AtomicU64;
        pub use std::sync::atomic::*;
    }

    pub mod papaya {
        use std::hash::{BuildHasher, Hash};

        pub use papaya::LocalGuard;

        pub struct HashMap<K, V, S>(papaya::HashMap<K, V, S>);

        impl<K, V, S: Default> Default for HashMap<K, V, S> {
            fn default() -> Self {
                Self(
                    papaya::HashMap::builder()
                        .capacity(256) // A relatively large capacity to hopefully avoid resizing.
                        .resize_mode(papaya::ResizeMode::Blocking)
                        .hasher(S::default())
                        .build(),
                )
            }
        }

        impl<K, V, S> HashMap<K, V, S>
        where
            K: Eq + Hash,
            V: Clone,
            S: BuildHasher,
        {
            #[inline]
            pub fn guard(&self) -> LocalGuard<'_> {
                self.0.guard()
            }

            #[inline]
            pub fn get(&self, key: &K, guard: &LocalGuard<'_>) -> Option<V> {
                self.0.get(key, guard).cloned()
            }

            #[inline]
            pub fn insert(&self, key: K, value: V, guard: &LocalGuard<'_>) {
                self.0.insert(key, value, guard);
            }
        }
    }

    /// A wrapper around parking-lot's `Condvar` to mirror shuttle's API.
    pub struct Condvar(parking_lot::Condvar);

    // this is not derived because it confuses rust-analyzer ... https://github.com/rust-lang/rust-analyzer/issues/19755
    #[allow(clippy::derivable_impls)]
    impl Default for Condvar {
        fn default() -> Self {
            Self(Default::default())
        }
    }

    // this is not derived because it confuses rust-analyzer ... https://github.com/rust-lang/rust-analyzer/issues/19755
    impl std::fmt::Debug for Condvar {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_tuple("Condvar").field(&self.0).finish()
        }
    }

    impl Condvar {
        pub fn wait<'a, T>(&self, mut guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
            self.0.wait(&mut guard);
            guard
        }

        pub fn notify_one(&self) {
            self.0.notify_one();
        }

        pub fn notify_all(&self) {
            self.0.notify_all();
        }
    }
}
