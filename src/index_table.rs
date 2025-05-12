use crate::{Transaction, RwTxn, Table, RkyvSer, RkyvVal, Error, lmdb, DbFlags};
use culpa::throws;
use batadase_index::Index;
use enumflags2::BitFlag;
use std::marker::PhantomData;

pub struct IndexTable<'tx, TX, T> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<T>,
}

impl<'tx, 'env: 'tx, TX, T> Table<'tx, 'env, TX> for IndexTable<'tx, TX, T> where
	TX: Transaction<'env>,
	T: rkyv::Archive,
	rkyv::Archived<T>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	fn dbi(&self) -> lmdb_sys::MDB_dbi { self.dbi }
	fn txn(&self) -> &TX { self.tx }
	fn flags() -> enumflags2::BitFlags<DbFlags> { DbFlags::IntegerKey.into() }
	fn build(tx: &'tx TX, name: &'static [u8]) -> Self {
		Self::build(tx, tx.env().db(name).unwrap())
	}
}

impl<'tx, T> IndexTable<'tx, RwTxn<'tx>, T> where
	T: for <'a> rkyv::Serialize<RkyvSer<'a>>,
	rkyv::Archived<T>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	#[throws]
	pub fn put(&self, index: Index<T>, t: &T) {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t)?;
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes, lmdb::PutFlags::empty())?;
	}

	#[throws]
	pub fn put_no_overwrite(&self, index: Index<T>, t: &T) {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t)?;
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes, lmdb::PutFlags::NoOverwrite.into())?;
	}

	#[throws]
	pub fn put_last(&self, t: &T) -> Index<T> {
		let index = Index::from(self.last()?.map_or(0, |(x, _)| u64::from(x) + 1));
		self.put(index, t)?;
		index
	}

	#[throws]
	pub fn delete_index(&self, index: Index<T>) -> bool {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::del(self.tx, self.dbi, &mut index_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)?; }
}

impl<'tx, 'env: 'tx, TX, T> IndexTable<'tx, TX, T> where
	TX: Transaction<'env>,
	T: rkyv::Archive,
	rkyv::Archived<T>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		Self { tx, dbi, _pd: PhantomData }
	}

	#[throws]
	pub fn get(&self, index: Index<T>) -> Option<&'tx rkyv::Archived<T>> {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let Some(value_bytes) = lmdb::get(self.tx, self.dbi, &mut index_bytes)? else { return None; };
		Some(rkyv::access::<rkyv::Archived<T>, _>(value_bytes)?)
	}

	#[throws]
	pub fn last(&self) -> Option<(Index<T>, &'tx rkyv::Archived<T>)> {
		let Some((key_u64, value_bytes)) = lmdb::Cursor::open(self.tx, self.dbi)?.get_with_u64_key(lmdb::CursorOpFlags::Last) else { return None; };
		Some((Index::from(key_u64), rkyv::access::<rkyv::Archived<T>, _>(value_bytes)?))
	}

	#[expect(clippy::iter_not_returning_iterator)]
	#[throws]
	pub fn iter(&self) -> impl Iterator<Item = (Index<T>, &'tx rkyv::Archived<T>)> + use<'tx, 'env, TX, T> where
		rkyv::Archived<T>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	{
		struct Cursor<'tx, TX, T>(lmdb::Cursor<'tx, TX>, PhantomData<T>);

		impl<'tx, 'env: 'tx, TX, T> Iterator for Cursor<'tx, TX, T> where
			TX: Transaction<'env>,
			T: rkyv::Archive,
			rkyv::Archived<T>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
		{
			type Item = (Index<T>, &'tx rkyv::Archived<T>);

			fn next(&mut self) -> Option<(Index<T>, &'tx rkyv::Archived<T>)> {
				let (key_u64, value_bytes) = self.0.get_with_u64_key(lmdb::CursorOpFlags::Next)?;
				let key = Index::from(key_u64);
				let value = match rkyv::access::<rkyv::Archived<T>, _>(value_bytes) {
					Ok(x) => x,
					Err(e) => { log::error!("Error deserializing value in rev cursor: {e:?}"); return None; }
				};
				Some((key, value))
			}
		}

		Cursor::<TX, T>(lmdb::Cursor::open(self.tx, self.dbi)?, PhantomData)
	}
}
