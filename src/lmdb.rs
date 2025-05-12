use super::{Transaction, RwTxn};
use std::convert::AsMut;
use culpa::throws;
pub use error::Error;
pub use lmdb_sys as sys;

pub mod error;

#[enumflags2::bitflags]
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbFlags {
	ReverseKey = sys::MDB_REVERSEKEY, // keys compared in reverse order
	IntegerKey = sys::MDB_INTEGERKEY, // keys are binary integers in native byte order (u32 [C unsigned int] or usize [C size_t]), all must be same size
	Create = sys::MDB_CREATE,         // create db if it doesn't exist, only write tx

	DupSort = sys::MDB_DUPSORT, // allow duplicate keys, stored in sorted order, limits the size of data to MDB_MAXKEYSIZE compile-time constant, default 511 bytes
		DupFixed = sys::MDB_DUPFIXED,     // all values are same size
		IntegerDup = sys::MDB_INTEGERDUP, // duplicate data items are binary integers
		ReverseDup = sys::MDB_REVERSEDUP, // duplicate data items should be compared in reverse order
}

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorOpFlags {
	GetCurrent = sys::MDB_GET_CURRENT, // Return key/data at current cursor position

	First = sys::MDB_FIRST, // Position at first key/data item
	Last = sys::MDB_LAST,   // Position at last key/data item

	Next = sys::MDB_NEXT, // Position at next data item
	Prev = sys::MDB_PREV, // Position at previous data item

	Set = sys::MDB_SET,            // Position at specified key
	SetKey = sys::MDB_SET_KEY,     // Position at specified key, return key + data
	SetRange = sys::MDB_SET_RANGE, // Position at first key greater than or equal to specified key.

	// ONLY DbFlags::DupFixed
		GetMultiple = sys::MDB_GET_MULTIPLE,   // Return key and up to a page of duplicate data items from current cursor position. Move cursor to prepare for CursorOpFlags::NextMultiple
		NextMultiple = sys::MDB_NEXT_MULTIPLE, // Return key and up to a page of duplicate data items from next cursor position. Move cursor to prepare for CursorOpFlags::NextMultiple

	// ONLY DbFlags::DupSort
		FirstDup = sys::MDB_FIRST_DUP, // Position at first data item of current key
		LastDup = sys::MDB_LAST_DUP,   // Position at last data item of current key

		NextDup = sys::MDB_NEXT_DUP,     // Position at next data item of current key
		NextNodup = sys::MDB_NEXT_NODUP, // Position at first data item of next key
		PrevDup = sys::MDB_PREV_DUP,     // Position at previous data item of current key
		PrevNodup = sys::MDB_PREV_NODUP, // Position at last data item of previous key

		GetBoth = sys::MDB_GET_BOTH,            // Position at key/data pair
		GetBothRange = sys::MDB_GET_BOTH_RANGE, // position at key, nearest data
}

#[enumflags2::bitflags]
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutFlags {
	NoDupData = sys::MDB_NODUPDATA, // ONLY for DbFlags::DupSort, do not enter duplicate data
	NoOverwrite = sys::MDB_NOOVERWRITE, // do not enter duplicate data or overwrite existing data, in case of Error::KeyExists - the data parameter will point to existing item
	Reserve = sys::MDB_RESERVE, // reserve space but do not write the data, caller expected to fill in the data before transaction ends
	Append = sys::MDB_APPEND, // append key/data to end of the database, allows fast bulk loading of keys in known sorted order, loading unsorted will cause Error::KeyExists
	AppendDup = sys::MDB_APPENDDUP, // as above, but for sorted dup data
}

#[repr(transparent)]
struct Val<'a>(sys::MDB_val, std::marker::PhantomData<&'a ()>);

impl<'a> Val<'a> {
	fn from_buf(mut buf: impl AsMut<[u8]> + 'a) -> Self {
		let buf = buf.as_mut();
		Self(sys::MDB_val { mv_size: buf.len(), mv_data: buf.as_mut_ptr().cast() }, std::marker::PhantomData)
	}

	fn new_outparam<'tx: 'a, 'env: 'tx>(_tx: &'tx impl Transaction<'env>) -> Self {
		Self(sys::MDB_val { mv_size: 0, mv_data: std::ptr::null_mut() }, std::marker::PhantomData)
	}

	fn as_slice(&self) -> &'a [u8] {
		unsafe { std::slice::from_raw_parts(self.mv_data.cast::<u8>(), self.mv_size) }
	}
}

impl std::ops::Deref for Val<'_> {
	type Target = sys::MDB_val;

	fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::DerefMut for Val<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

pub(super) struct Cursor<'tx, TX>(*mut sys::MDB_cursor, &'tx TX);
unsafe impl<TX> Send for Cursor<'_, TX> {}
unsafe impl<TX> Sync for Cursor<'_, TX> {}

impl<'tx, 'env: 'tx, TX> Cursor<'tx, TX> where
	TX: Transaction<'env>,
{
	#[throws]
	pub(super) fn open(tx: &'tx TX, dbi: sys::MDB_dbi) -> Self {
		let mut cursor = std::ptr::null_mut();
		error::handle_cursor_open_code(unsafe { sys::mdb_cursor_open(tx.raw(), dbi, &mut cursor) })?;
		Self(cursor, tx)
	}

	pub(super) fn get(&mut self, flags: CursorOpFlags) -> Option<(&'tx [u8], &'tx [u8])> {
		let mut key = Val::new_outparam(self.1);
		let mut value = Val::new_outparam(self.1);
		if !error::handle_cursor_get_code(unsafe { sys::mdb_cursor_get(self.0, &mut *key, &mut *value, flags as _) }) { return None }
		Some((
			key.as_slice(),
			value.as_slice(),
		))
	}

	// flags must not include CursorOpFlags::Set because that doesn't change key
	pub(super) fn get_with_key(&mut self, key_in: &mut [u8], flags: CursorOpFlags) -> Option<(&'tx [u8], &'tx [u8])> {
		let mut key = Val::new_outparam(self.1);
		key.mv_size = key_in.len();
		key.mv_data = key_in.as_mut_ptr().cast();
		let mut value = Val::new_outparam(self.1);
		if !error::handle_cursor_get_code(unsafe { sys::mdb_cursor_get(self.0, &mut *key, &mut *value, flags as _) }) { return None }
		Some((
			key.as_slice(),
			value.as_slice(),
		))
	}

	pub(super) fn get_with_u64_key(&mut self, flags: CursorOpFlags) -> Option<(u64, &'tx [u8])> {
		let mut key = Val::new_outparam(self.1);
		let mut value = Val::new_outparam(self.1);
		if !error::handle_cursor_get_code(unsafe { sys::mdb_cursor_get(self.0, &mut *key, &mut *value, flags as _) }) { return None }
		debug_assert!(key.mv_size == std::mem::size_of::<u64>());
		Some((
			u64::from_ne_bytes(unsafe { *key.mv_data.cast::<[u8; std::mem::size_of::<u64>()]>() }),
			value.as_slice(),
		))
	}
}

impl<TX> Drop for Cursor<'_, TX> {
	fn drop(&mut self) {
		unsafe { sys::mdb_cursor_close(self.0) };
	}
}

#[throws]
pub(super) fn put(tx: &RwTxn, dbi: sys::MDB_dbi, key: impl AsMut<[u8]>, val: impl AsMut<[u8]>, flags: enumflags2::BitFlags<PutFlags>) {
	error::handle_put_code(unsafe { sys::mdb_put(tx.raw(), dbi, &mut *Val::from_buf(key), &mut *Val::from_buf(val), flags.bits()) })?;
}

#[throws]
pub(super) fn del(tx: &RwTxn, dbi: sys::MDB_dbi, key: impl AsMut<[u8]>) -> bool {
	error::handle_del_code(unsafe { sys::mdb_del(tx.raw(), dbi, &mut *Val::from_buf(key), std::ptr::null_mut()) })?
}

#[throws]
pub(super) fn drop(tx: &RwTxn, dbi: sys::MDB_dbi) {
	error::handle_drop_code(unsafe { sys::mdb_drop(tx.raw(), dbi, 0) })?;
}

#[throws]
pub(super) fn get<'tx, 'env: 'tx>(tx: &'tx impl Transaction<'env>, dbi: sys::MDB_dbi, key: impl AsMut<[u8]>) -> Option<&'tx [u8]> {
	let mut value = Val::new_outparam(tx);
	if !error::handle_get_code(unsafe { sys::mdb_get(tx.raw(), dbi, &mut *Val::from_buf(key), &mut *value) })? { return None; }
	Some(value.as_slice())
}

#[throws]
pub(super) fn txn_begin(env: *mut sys::MDB_env, flags: u32) -> *mut sys::MDB_txn {
	let mut tx: *mut sys::MDB_txn = std::ptr::null_mut();
	error::handle_txn_begin_code(unsafe { sys::mdb_txn_begin(env, std::ptr::null_mut(), flags, &mut tx) })?;
	tx
}

#[throws]
pub(super) fn txn_commit(tx: *mut sys::MDB_txn) {
	error::handle_txn_commit_code(unsafe { sys::mdb_txn_commit(tx) })?;
}

#[throws]
pub(super) fn env_create() -> *mut sys::MDB_env {
	let mut env: *mut sys::MDB_env = std::ptr::null_mut();
	error::handle_env_create_code(unsafe { sys::mdb_env_create(&mut env) })?;
	env
}

#[throws]
pub(super) fn env_set_maxdbs(env: *mut sys::MDB_env, maxdbs: u32) {
	error::handle_env_set_maxdbs_code(unsafe { sys::mdb_env_set_maxdbs(env, maxdbs) })?;
}

#[throws]
pub(super) fn env_set_mapsize(env: *mut sys::MDB_env, mapsize: usize) {
	error::handle_env_set_mapsize_code(unsafe { sys::mdb_env_set_mapsize(env, mapsize) })?;
}

#[throws]
pub(super) fn env_set_maxreaders(env: *mut sys::MDB_env, maxreaders: u32) {
	error::handle_env_set_maxreaders_code(unsafe { sys::mdb_env_set_maxreaders(env, maxreaders) })?;
}

#[allow(unused_variables)]
#[throws]
pub(super) fn env_open(env: *mut sys::MDB_env, path: &std::ffi::CStr, flags: u32, mode: u32) {
	#[cfg(unix)] let mode = mode;
	#[cfg(windows)] let mode = 0;
	error::handle_env_open(unsafe { sys::mdb_env_open(env, path.as_ptr(), flags, mode) })?;
}

pub(super) fn dbi_open(tx: *mut sys::MDB_txn, name: &[u8], flags: enumflags2::BitFlags<DbFlags>) -> sys::MDB_dbi {
	let mut dbi: sys::MDB_dbi = 0;
	error::handle_dbi_open_code(unsafe { sys::mdb_dbi_open(tx, name.as_ptr().cast(), flags.bits(), &mut dbi) });
	dbi
}

#[throws]
pub(super) fn stat(txn: *mut sys::MDB_txn, dbi: sys::MDB_dbi) -> sys::MDB_stat {
	let mut stat: sys::MDB_stat = unsafe { std::mem::zeroed() };
	error::handle_stat_code(unsafe { sys::mdb_stat(txn, dbi, &mut stat) })?;
	stat
}

pub trait MdbValExt {
	#[expect(clippy::missing_safety_doc)]
	unsafe fn as_slice(&self) -> &[u8];
}

impl MdbValExt for lmdb_sys::MDB_val {
	unsafe fn as_slice(&self) -> &[u8] {
		unsafe { std::slice::from_raw_parts(self.mv_data.cast::<u8>(), self.mv_size) }
	}
}
