use culpa::{throw, throws};

#[derive(thiserror::Error, Debug)]
pub enum Error {
	#[error("the version of the LMDB library doesn't match the version that created the database environment")] VersionMismatch,
	#[error("the environment file headers are corrupted")] Corrupted,
	#[error("the directory specified by the path parameter doesn't exist")] DirDoesntExist,
	#[error("the user didn't have permission to access the environment files")] NoAccess,
	#[error("the environment was locked by another process")] EnvLocked,
	#[error("database create error")] CreateError(i32),
	#[error("an invalid parameter was specified")] InvalidParameter,
	#[error("a fatal error occurred earlier and the environment must be shut down")] Panic,
	#[error("another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well")] MapResized,
	#[error("a read-only transaction was requested and the reader lock table is full")] ReadersFull,
	#[error("no more disk space")] NoDiskSpace,
	#[error("database is full")] MapFull,
	#[error("too many tx dirty pags")] TxnFull,
	#[error("trying to write a readonly transaction")] TxnPerm,
	#[error("a low-level I/O error occured while writing")] Io,
	#[error("out of memory")] Oom,
	#[error("key already exists and overwrite isn't requested")] KeyExists,
	#[error("unsupported size of key/DB name/data, or wrong DUPFIXED size")] BadValSize,
	#[error("misc error {0}")] Misc(i32),
}

#[throws]
pub(crate) fn handle_del_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EACCES => throw!(Error::TxnPerm),
		libc::EINVAL => throw!(Error::InvalidParameter),
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_put_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_MAP_FULL => throw!(Error::MapFull),
		lmdb_sys::MDB_TXN_FULL => throw!(Error::TxnFull),
		lmdb_sys::MDB_KEYEXIST => throw!(Error::KeyExists),
		lmdb_sys::MDB_BAD_VALSIZE => throw!(Error::BadValSize),
		libc::EACCES => throw!(Error::TxnPerm),
		libc::EINVAL => throw!(Error::InvalidParameter),
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_drop_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_get_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EINVAL => throw!(Error::InvalidParameter),
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_cursor_open_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		libc::EINVAL => throw!(Error::InvalidParameter),
		code => throw!(Error::Misc(code)),
	}
}

pub(crate) fn handle_cursor_get_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EINVAL => panic!("cursor invalid parameter"),
		e => panic!("cursor misc error {e}"),
	}
}

#[throws]
pub(crate) fn handle_txn_begin_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_PANIC => throw!(Error::Panic),
		lmdb_sys::MDB_MAP_RESIZED => throw!(Error::MapResized),
		lmdb_sys::MDB_READERS_FULL => throw!(Error::ReadersFull),
		libc::ENOMEM => throw!(Error::Oom),
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_txn_commit_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		libc::EINVAL => throw!(Error::InvalidParameter),
		libc::ENOSPC => throw!(Error::NoDiskSpace),
		libc::EIO => throw!(Error::Io),
		libc::ENOMEM => throw!(Error::Oom),
		code => throw!(Error::Misc(code)),
	}
}

#[throws]
pub(crate) fn handle_env_create_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		e => throw!(Error::CreateError(e)),
	}
}

#[throws]
pub(crate) fn handle_env_set_maxdbs_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => throw!(Error::InvalidParameter),
	}
}

#[throws]
pub(crate) fn handle_env_set_mapsize_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => throw!(Error::InvalidParameter),
	}
}

#[throws]
pub(crate) fn handle_env_set_maxreaders_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => throw!(Error::InvalidParameter),
	}
}

#[throws]
pub(crate) fn handle_env_open(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_VERSION_MISMATCH => throw!(Error::VersionMismatch),
		lmdb_sys::MDB_INVALID => throw!(Error::Corrupted),
		libc::ENOENT | libc::ESRCH => throw!(Error::DirDoesntExist),
		libc::EACCES => throw!(Error::NoAccess),
		libc::EAGAIN => throw!(Error::EnvLocked),
		code => throw!(Error::Misc(code)),
	}
}

pub(crate) fn handle_dbi_open_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_NOTFOUND => panic!("not found"), // should be unreachable
		lmdb_sys::MDB_DBS_FULL => panic!("too many dbs"),
		e => panic!("misc error {e}"),
	}
}

#[throws]
pub(crate) fn handle_stat_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		libc::EINVAL => throw!(Error::InvalidParameter),
		code => throw!(Error::Misc(code)),
	}
}
