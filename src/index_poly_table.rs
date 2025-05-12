use crate::{Transaction, RwTxn, Table, RkyvSer, RkyvVal, Error, lmdb, DbFlags};
use culpa::throws;
use enumflags2::BitFlag;
use batadase_index::Index;

pub struct IndexPolyTable<'tx, TX> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
}

impl<'tx, 'env: 'tx, TX> Table<'tx, 'env, TX> for IndexPolyTable<'tx, TX> where
	TX: Transaction<'env>,
{
	fn dbi(&self) -> lmdb_sys::MDB_dbi { self.dbi }
	fn txn(&self) -> &TX { self.tx }
	fn flags() -> enumflags2::BitFlags<DbFlags> { DbFlags::IntegerKey.into() }
	fn build(tx: &'tx TX, name: &'static [u8]) -> Self {
		Self::build(tx, tx.env().db(name).unwrap())
	}
}

impl<'tx> IndexPolyTable<'tx, RwTxn<'tx>> {
	#[throws]
	pub fn put<T>(&self, index: Index<T>, t: &T) where
		T: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	{
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t)?;
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes, lmdb::PutFlags::empty())?;
	}

	#[throws]
	pub fn put_no_overwrite<T>(&self, index: Index<T>, t: &T) where
		T: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	{
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t)?;
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes, lmdb::PutFlags::NoOverwrite.into())?;
	}

	#[throws]
	pub fn put_last<T>(&self, t: &T) -> Index<T> where
		T: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	{
		let index = Index::from(self.last_numeric_index()?.map_or(0, |x| x + 1));
		self.put(index, t)?;
		index
	}

	#[throws]
	pub fn delete_index<T>(&self, index: Index<T>) -> bool {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::del(self.tx, self.dbi, &mut index_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)?; }
}

impl<'tx, 'env: 'tx, TX> IndexPolyTable<'tx, TX> where
	TX: Transaction<'env>,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self { Self { tx, dbi } }

	#[throws]
	pub fn get<T>(&self, index: Index<T>) -> Option<&'tx rkyv::Archived<T>> where
		T: rkyv::Archive,
		rkyv::Archived<T>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	{
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let Some(value_bytes) = lmdb::get(self.tx, self.dbi, &mut index_bytes)? else { return None; };
		Some(rkyv::access::<rkyv::Archived<T>, _>(value_bytes)?)
	}

	#[throws]
	fn last_numeric_index(&self) -> Option<u64> {
		lmdb::Cursor::open(self.tx, self.dbi)?
			.get_with_u64_key(lmdb::CursorOpFlags::Last)
			.map(|(key, _)| key)
	}

	// Maybe you just can't iterate poly tables as you're really not meant to
	// if you want diagnostics or whatever then restore indices from the sources
	// #[throws]
	// pub fn iter(&self) -> impl Iterator<Item = (Index<T>, &rkyv::Archived<T>)> { ... }
}
