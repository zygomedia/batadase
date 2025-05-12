use crate::{Transaction, RwTxn, Table, RkyvSer, RkyvVal, RkyvDe, Error, lmdb};
use culpa::throws;
use enumflags2::BitFlag;
use std::marker::PhantomData;

pub struct AssocTable<'tx, TX, K, V> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<(K, V)>,
}

impl<'tx, 'env: 'tx, TX, K, V> Table<'tx, 'env, TX> for AssocTable<'tx, TX, K, V> where
	TX: Transaction<'env>,
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	V: rkyv::Archive,
	rkyv::Archived<K>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	rkyv::Archived<V>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>> + rkyv::Deserialize<V, RkyvDe> + 'tx,
{
	fn dbi(&self) -> lmdb_sys::MDB_dbi { self.dbi }
	fn txn(&self) -> &TX { self.tx }
	fn build(tx: &'tx TX, name: &'static [u8]) -> Self {
		Self::build(tx, tx.env().db(name).unwrap())
	}
}

fn archived_from_cursor_get<'tx, K, V>(get: Option<(&'tx [u8], &'tx [u8])>) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> where
	K: rkyv::Archive,
	V: rkyv::Archive,
	rkyv::Archived<K>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	rkyv::Archived<V>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	let (key_bytes, value_bytes) = get?;
	let key = match rkyv::access::<rkyv::Archived<K>, _>(key_bytes) {
		Ok(x) => x,
		Err(e) => { log::error!("Error deserializing key in cursor: {e:?}"); return None; }
	};
	let value = match rkyv::access::<rkyv::Archived<V>, _>(value_bytes) {
		Ok(x) => x,
		Err(e) => { log::error!("Error deserializing value in cursor: {e:?}"); return None; }
	};
	Some((key, value))
}

struct Cursor<'tx, TX, K, V>(lmdb::Cursor<'tx, TX>, lmdb::CursorOpFlags, PhantomData<(K, V)>);
impl<'tx, 'env: 'tx, TX, K, V> Iterator for Cursor<'tx, TX, K, V> where
	TX: Transaction<'env>,
	K: rkyv::Archive,
	V: rkyv::Archive,
	rkyv::Archived<K>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	rkyv::Archived<V>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	type Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>);

	fn next(&mut self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
		archived_from_cursor_get::<'tx, K, V>(self.0.get(self.1))
	}
}

// RwTxn only, so all methods mutate
impl<'tx, K, V> AssocTable<'tx, RwTxn<'tx>, K, V> where
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	V: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
{
	#[throws]
	pub fn put(&self, key: &K, value: &V) {
		let mut key_bytes = rkyv::to_bytes(key)?;
		let mut value_bytes = rkyv::to_bytes(value)?;
		lmdb::put(self.tx, self.dbi, &mut key_bytes, &mut value_bytes, lmdb::PutFlags::empty())?;
	}

	#[throws]
	pub fn put_no_overwrite(&self, key: &K, value: &V) {
		let mut key_bytes = rkyv::to_bytes(key)?;
		let mut value_bytes = rkyv::to_bytes(value)?;
		lmdb::put(self.tx, self.dbi, &mut key_bytes, &mut value_bytes, lmdb::PutFlags::NoOverwrite.into())?;
	}

	#[throws]
	pub fn delete(&self, key: &K) -> bool {
		let mut key_bytes = rkyv::to_bytes(key)?;
		lmdb::del(self.tx, self.dbi, &mut key_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)?; }
}

// both RoTxn and RwTxn, so all methods are read-only
impl<'tx, 'env: 'tx, TX, K, V> AssocTable<'tx, TX, K, V> where
	TX: Transaction<'env>,
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	V: rkyv::Archive,
	rkyv::Archived<K>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	rkyv::Archived<V>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>> + rkyv::Deserialize<V, RkyvDe> + 'tx,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		Self { tx, dbi, _pd: PhantomData }
	}

	#[throws]
	pub fn get(&self, key: &K) -> Option<&'tx rkyv::Archived<V>> {
		let mut key_bytes = rkyv::to_bytes(key)?;
		let Some(value_bytes) = lmdb::get(self.tx, self.dbi, &mut key_bytes)? else { return None; };
		Some(rkyv::access::<rkyv::Archived<V>, _>(value_bytes)?)
	}

	#[throws]
	pub fn get_unrkyv(&self, key: &K) -> Option<V> {
		let Some(archived) = self.get(key)? else { return None; };
		Some(rkyv::deserialize::<V, rkyv::rancor::Error>(archived)?)
	}

	#[throws]
	pub fn last(&self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
		let Some((key_bytes, value_bytes)) = lmdb::Cursor::open(self.tx, self.dbi)?.get(lmdb::CursorOpFlags::Last) else { return None; };
		Some((
			rkyv::access::<rkyv::Archived<K>, _>(key_bytes)?,
			rkyv::access::<rkyv::Archived<V>, _>(value_bytes)?,
		))
	}

	#[expect(clippy::iter_not_returning_iterator)]
	#[throws]
	pub fn iter(&self) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> + use<'tx, 'env, TX, K, V> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		Cursor::<TX, K, V>(lmdb::Cursor::open(self.tx, self.dbi)?, lmdb::CursorOpFlags::Next, PhantomData)
	}

	#[throws]
	pub fn iter_from(&self, key: &K) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> + use<'tx, 'env, TX, K, V> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		let mut key_bytes = rkyv::to_bytes(key)?;
		let mut cursor = lmdb::Cursor::open(self.tx, self.dbi)?;
		archived_from_cursor_get::<'tx, K, V>(cursor.get_with_key(&mut key_bytes, lmdb::CursorOpFlags::SetRange)).into_iter()
			.chain(Cursor::<TX, K, V>(cursor, lmdb::CursorOpFlags::Next, PhantomData))
	}

	#[throws]
	pub fn iter_rev(&self) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> + use<'tx, 'env, TX, K, V> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		Cursor::<TX, K, V>(lmdb::Cursor::open(self.tx, self.dbi)?, lmdb::CursorOpFlags::Prev, PhantomData)
	}

	#[throws]
	pub fn iter_rev_from(&self, key: &K) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> + use<'tx, 'env, TX, K, V> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		let mut key_bytes = rkyv::to_bytes(key)?;
		let mut cursor = lmdb::Cursor::open(self.tx, self.dbi)?;
		let _ = cursor.get_with_key(&mut key_bytes, lmdb::CursorOpFlags::SetRange);
		Cursor::<TX, K, V>(cursor, lmdb::CursorOpFlags::Prev, PhantomData)
	}
}
