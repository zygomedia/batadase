use culpa::throws;
use std::collections::HashMap;
use std::future::Future;

use super::{lmdb::{self, DbFlags}, DbName, RoTxn, RwTxn, Transaction, error::Error, Table};

pub struct Env {
	raw_env: *mut lmdb_sys::MDB_env,
	dbs: HashMap<&'static [u8], lmdb_sys::MDB_dbi>,
	write_sema: tokio::sync::Semaphore,
}

pub struct EnvBuilder {
	raw_env: *mut lmdb_sys::MDB_env,
	dbs: Vec<(&'static [u8], enumflags2::BitFlags<lmdb::DbFlags>)>,
}

unsafe impl Send for Env {}
unsafe impl Sync for Env {}
unsafe impl Send for EnvBuilder {}
unsafe impl Sync for EnvBuilder {}

pub trait WriteCallback<'tx, T>: FnOnce(&'tx RwTxn<'static>) -> Self::Fut {
    type Fut: Future<Output = T>;
}

impl<'tx, T, Out, F> WriteCallback<'tx, T> for F where
	Out: Future<Output = T>,
	F: FnOnce(&'tx RwTxn<'static>) -> Out,
{
    type Fut = Out;
}

fn complain_about_lock_hold(instant: std::time::Instant) {
	let lock_held = instant.elapsed().as_secs_f32();
	match lock_held {
		10.0.. => log::error!("async write lock held for {lock_held:.2} secs"),
		2.5.. => log::warn!("async write lock held for {lock_held:.2} secs"),
		0.25.. => log::info!("async write lock held for {lock_held:.2} secs"),
		_ => log::trace!("async write lock held for {lock_held:.2} secs"),
	}
}

impl Env {
	#[throws]
	pub fn builder() -> EnvBuilder {
		EnvBuilder { raw_env: lmdb::env_create()?, dbs: Vec::new() }
	}

	pub fn db(&self, name: &'static [u8]) -> Option<lmdb_sys::MDB_dbi> {
		self.dbs.get(name).copied()
	}

	pub fn reader_list(&self) {
		unsafe extern "C" fn msg(msg: *const libc::c_char, _: *mut libc::c_void) -> i32 {
			let cstr = unsafe { std::ffi::CStr::from_ptr(msg) };
			println!("{}", cstr.to_string_lossy());
			0
		}
		unsafe { lmdb_sys::mdb_reader_list(self.raw_env, Some(msg), std::ptr::null_mut()) };
	}

	// ????? rustc lint engine?
	#[expect(unused_braces)]
	#[throws] pub fn read_tx(&self) -> RoTxn<'_> { RoTxn { raw: lmdb::txn_begin(self.raw_env, lmdb_sys::MDB_RDONLY)?, env: self } }
	#[expect(unused_braces)]
	#[throws] pub(super) fn write_tx(&self) -> RwTxn<'_> { RwTxn { raw: lmdb::txn_begin(self.raw_env, 0)?, env: self } }

	#[throws]
	pub async fn write<Res, Job>(&'static self, job: Job) -> Res where
		Res: Send + 'static,
		Job: (FnOnce(&RwTxn<'_>) -> Res) + Send + 'static,
	{
		let _lock = self.write_sema.acquire().await.unwrap();
		let now = std::time::Instant::now();

		let res = tokio::task::spawn_blocking(move || {
			let tx = self.write_tx()?;
			let res = job(&tx);
			tx.commit()?;
			Result::<_, crate::Error>::Ok(res)
		}).await.expect("tokio spawn_blocking failed");
		drop(_lock);
		complain_about_lock_hold(now);
		res?
	}

	/// outer result is whether DB ops failed or not,
	/// inner result is whether the job failed or not
	/// the tx isn't committed if the job fails
	#[throws]
	pub async fn try_write<Res, Err, Job>(&'static self, job: Job) -> Result<Res, Err> where
		Res: Send + 'static,
		Job: (FnOnce(&RwTxn<'_>) -> Result<Res, Err>) + Send + 'static,
		Err: Send + 'static,
	{
		let _lock = self.write_sema.acquire().await.unwrap();
		let now = std::time::Instant::now();

		let res = tokio::task::spawn_blocking(move || {
			let tx = self.write_tx()?;
			let res = job(&tx);
			if res.is_ok() {
				tx.commit()?;
			} else {
				tx.abort();
			}
			Result::<_, crate::Error>::Ok(res)
		}).await.expect("tokio spawn_blocking failed");
		drop(_lock);
		complain_about_lock_hold(now);
		res?
	}

	#[throws]
	pub async fn write_async<Res>(&'static self, job: impl for <'tx> WriteCallback<'tx, Res>) -> Res {
		let _lock = self.write_sema.acquire().await.unwrap();
		let now = std::time::Instant::now();

		let res = {
			let tx = self.write_tx()?;
			let res = job(&tx).await;
			tx.commit()?;
			res
		};
		drop(_lock);
		complain_about_lock_hold(now);
		res
	}

	/// outer result is whether DB ops failed or not,
	/// inner result is whether the job failed or not
	/// the tx isn't committed if the job fails
	#[throws]
	pub async fn try_write_async<Res, Err>(&'static self, job: impl for <'tx> WriteCallback<'tx, Result<Res, Err>>) -> Result<Res, Err> {
		let _lock = self.write_sema.acquire().await.unwrap();
		let now = std::time::Instant::now();

		let res = {
			let tx: RwTxn<'static> = self.write_tx()?;
			let res = job(&tx).await;
			if res.is_ok() {
				tx.commit()?;
			} else {
				tx.abort();
			}
			res
		};
		drop(_lock);
		complain_about_lock_hold(now);
		res
	}

	// unclear if useful?
	/*
	#[throws]
	pub fn blocking_write<Res, F: (FnOnce(&RwTxn) -> Res)>(job: F) -> Res {
		let _lock = tokio::task::block_in_place(|| ENV.write_lock.lock());
		let now = std::time::Instant::now();
		let tx = ENV.write_tx()?;
		let res = job(&tx);
		tx.commit()?;
		drop(_lock);
		log::debug!("write lock held for {:.2} secs", now.elapsed().as_secs_f32());
		res
	}

	#[throws]
	pub fn blocking_try_write<Res, F: (FnOnce(&RwTxn) -> anyhow::Result<Res>)>(job: F) -> anyhow::Result<Res> {
		let _lock = tokio::task::block_in_place(|| ENV.write_lock.lock());
		let now = std::time::Instant::now();
		let tx = ENV.write_tx()?;
		let res = job(&tx);
		if res.is_ok() {
			tx.commit()?;
		} else {
			tx.abort();
		}
		drop(_lock);
		log::debug!("write lock held for {:.2} secs", now.elapsed().as_secs_f32());
		res
	}
	*/
}

impl EnvBuilder {
	#[throws]
	pub fn mapsize(self, size: usize) -> Self {
		lmdb::env_set_mapsize(self.raw_env, size)?;
		self
	}

	#[throws]
	pub fn maxreaders(self, readers: u32) -> Self {
		lmdb::env_set_maxreaders(self.raw_env, readers)?;
		self
	}

	#[must_use]
	pub fn with<N: DbName>(mut self) -> Self {
		self.dbs.push((N::NAME, N::flags() | N::Table::<'static, 'static, RwTxn>::flags()));
		self
	}

	#[throws]
	pub fn build(self, path: &std::ffi::CStr) -> Env {
		let flags =
			lmdb_sys::MDB_NOMETASYNC | // maybe lose last transaction in case of a crash
			lmdb_sys::MDB_NOTLS |      // don't use thread-local storage - read and write transactions can be on any thread, still at most 1 write tx
			lmdb_sys::MDB_NORDAHEAD;   // don't readahead - useful when datasets are bigger than ram (does nothing on Windows)

		lmdb::env_set_maxdbs(self.raw_env, self.dbs.len() as u32)?;
		
		// 0664 is permissions for db folder on Unix - read/write/not execute
		lmdb::env_open(self.raw_env, path, flags, 664)?;

		let mut env = Env { raw_env: self.raw_env, dbs: HashMap::new(), write_sema: tokio::sync::Semaphore::new(1) };
		let db_create_tx = RwTxn { raw: lmdb::txn_begin(self.raw_env, 0)?, env: &env };
		let mut dbs = HashMap::with_capacity(self.dbs.len());
		for (name, flags) in self.dbs {
			log::trace!("creating {}", unsafe { std::str::from_utf8_unchecked(name) });
			dbs.insert(name, lmdb::dbi_open(db_create_tx.raw(), name, flags | DbFlags::Create));
		}
		db_create_tx.commit()?;

		env.dbs = dbs;
		env
	}
}
