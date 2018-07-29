// Copyright (c) 2014 The btcsuite developers
// Copyright (c) 2015 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package badgerdb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/decred/dcrwallet/errors"
	"github.com/decred/dcrwallet/wallet/internal/walletdb"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

// convertErr wraps a driver-specific error with an error code.
func convertErr(err error) error {
	if err == nil {
		return nil
	}
	var kind errors.Kind
	switch err {
	case badger.ErrValueLogSize, badger.ErrValueThreshold, badger.ErrTxnTooBig, badger.ErrReadOnlyTxn, badger.ErrDiscardedTxn, badger.ErrEmptyKey, badger.ErrThresholdZero,
		badger.ErrRejected, badger.ErrInvalidRequest, badger.ErrManagedTxn, badger.ErrInvalidDump, badger.ErrZeroBandwidth, badger.ErrInvalidLoadingMode, badger.ErrWindowsNotSupported, badger.ErrReplayNeeded, badger.ErrTruncateNeeded:
		kind = errors.Invalid
	case badger.ErrKeyNotFound:
		kind = errors.NotExist
	case badger.ErrConflict, badger.ErrRetry, badger.ErrNoRewrite:
		kind = errors.IO
	}
	return errors.E(kind, err)
}

// transaction represents a database transaction.  It can either by read-only or
// read-write and implements the walletdb Tx interfaces.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	badgerTx *badger.Txn
	db       *badger.DB
	writable bool
	buckets  []*Bucket
	iterator *badger.Iterator
}

func (tx *transaction) ReadBucket(key []byte) walletdb.ReadBucket {
	//fmt.Println("Read Bucket")
	return tx.ReadWriteBucket(key)
}

func (tx *transaction) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	//fmt.Println("ReadWriteBucket")
	badgerBucket, err := newBucket(tx.badgerTx, key, tx)
	if err != nil {
		//TODO: Handle Error
		return nil
	}
	tx.buckets = append(tx.buckets, badgerBucket)
	return (*bucket)(badgerBucket)
}

func (tx *transaction) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	//fmt.Println("CreateTopLevelBucket:", string(key))
	badgerBucket, err := newBucket(tx.badgerTx, key, tx)
	if err != nil {
		return nil, err
	}
	tx.buckets = append(tx.buckets, badgerBucket)
	return (*bucket)(badgerBucket), nil
}

func (tx *transaction) DeleteTopLevelBucket(key []byte) error {
	//fmt.Println("DeleteTopLevelBucket")
	it := tx.badgerTx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		tx.badgerTx.Delete(it.Item().Key())
	}
	for i, _ := range tx.buckets {
		if bytes.Equal(tx.buckets[i].key, key) {
			tx.buckets = append(tx.buckets[:i], tx.buckets[i+1:]...)
			break
		}
	}
	return nil
}

// Commit commits all changes that have been made through the root bucket and
// all of its sub-buckets to persistent storage.
//
// This function is part of the walletdb.Tx interface implementation.
func (tx *transaction) Commit() error {
	if tx.iterator != nil {
		tx.iterator.Close()
	}

	writeable := tx.writable
	err := tx.badgerTx.Commit(nil)
	if err != nil {
		fmt.Println("Transaction commit error: ", err)
		return err
	}
	if true {
		return nil
	}
	tx.badgerTx = tx.db.NewTransaction(writeable)
	for _, b := range tx.buckets {
		b.SetTx(tx.badgerTx)
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 5
	tx.iterator = tx.badgerTx.NewIterator(opts)
	//fmt.Println("Commit Tx 2: ", &tx.badgerTx)
	// for _, b := range tx.buckets {
	// 	b.CloseCursor()
	// }
	//fmt.Println("Transaction commit successful")
	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the walletdb.Tx interface implementation.
func (tx *transaction) Rollback() error {
	writeable := tx.writable
	//	for _, b := range tx.buckets {
	//		b.CloseCursor()
	//	}
	if tx.iterator != nil {
		tx.iterator.Close()
	}
	tx.badgerTx.Discard()
	tx.badgerTx = tx.db.NewTransaction(writeable)
	for _, b := range tx.buckets {
		b.SetTx(tx.badgerTx)
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 5
	tx.iterator = tx.badgerTx.NewIterator(opts)
	//fmt.Println("Rollback 2:", &tx.badgerTx)
	return nil
}

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the walletdb Bucket interfaces.
type bucket Bucket

// Enforce bucket implements the walletdb Bucket interfaces.
var _ walletdb.ReadWriteBucket = (*bucket)(nil)

// NestedReadWriteBucket retrieves a nested bucket with the given key.  Returns
// nil if the bucket does not exist.
//
// This function is part of the walletdb.ReadWriteBucket interface implementation.
func (b *bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	badgerBucket := (*Bucket)(b).RetrieveBucket(key)
	if badgerBucket == nil {
		return nil
	}
	return (*bucket)(badgerBucket)
}

func (b *bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	return b.NestedReadWriteBucket(key)
}

// CreateBucket creates and returns a new nested bucket with the given key.
// Errors with code Exist if the bucket already exists, and Invalid if the key
// is empty or otherwise invalid for the driver.
//
//This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	//fmt.Println("CreateBucket:", string(key), "Parent:", string(b.key))
	bkt, err := (*Bucket)(b).Bucket(key, true)
	if err != nil {
		return nil, err
	}
	return (*bucket)(bkt), nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.  Errors with code Invalid if the key
// is empty or otherwise invalid for the driver.
//
//This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, error) {
	bkt, err := (*Bucket)(b).Bucket(key, false)
	if err != nil {
		return nil, err
	}
	return (*bucket)(bkt), nil
}

// DeleteNestedBucket removes a nested bucket with the given key.
//
//This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) DeleteNestedBucket(key []byte) error {
	//fmt.Println("DeleteNestedBucket")
	return (*Bucket)(b).DropBucket(key)
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This includes nested buckets, in which case the value is nil, but it does not
// include the key/value pairs within those nested buckets.
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	//fmt.Println("ForEach")
	return convertErr((*Bucket)(b).ForEach(fn))
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	return convertErr((*Bucket)(b).Put(key, value))
}

// Get returns the value for the given key.  Returns nil if the key does
// not exist in this bucket (or nested buckets).
//
// NOTE: The value returned by this function is only valid during a
// transaction.  Attempting to access it after a transaction has ended
// will likely result in an access violation.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	//fmt.Println("Get")
	return (*Bucket)(b).Get(key)
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	//fmt.Println("Delete")
	return convertErr((*Bucket)(b).Delete(key))
}

func (b *bucket) ReadCursor() walletdb.ReadCursor {
	return b.ReadWriteCursor()
}

// ReadWriteCursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs and nested buckets in forward or backward order.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	//fmt.Println("ReadWriteCursor")
	return (*cursor)((*Bucket)(b).BadgerCursor())
}

// cursor represents a cursor over key/value pairs and nested buckets of a
// bucket.
//
// Note that open cursors are not tracked on bucket changes and any
// modifications to the bucket, with the exception of cursor.Delete, invalidate
// the cursor. After invalidation, the cursor must be repositioned, or the keys
// and values returned may be unpredictable.
//type cursor badger.Iterator
type cursor Cursor

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Delete() error {
	//fmt.Println("Delete")
	if (*Cursor)(c).iterator.ValidForPrefix(c.key) {
		item := (*Cursor)(c).iterator.Item()
		return (*Cursor)(c).txn.Delete(item.Key())
	}
	return nil
}

// First positions the cursor at the first key/value pair and returns the pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) First() (key, value []byte) {
	//fmt.Println("First")
	for (*Cursor)(c).iterator.Rewind(); (*Cursor)(c).iterator.ValidForPrefix(c.key); (*Cursor)(c).iterator.Next() {
		item := (*Cursor)(c).iterator.Item()
		//fmt.Println("Item: ", item)
		val, err := item.Value()
		if err != nil {
			//TODO: handle error
			return nil, nil
		}
		return item.Key(), val
	}
	//No item found
	return nil, nil
}

// Last positions the cursor at the last key/value pair and returns the pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Last() (key, value []byte) {
	//fmt.Println("Last")
	var lastValidItem *badger.Item
	for (*Cursor)(c).iterator.Rewind(); (*Cursor)(c).iterator.ValidForPrefix(c.key); (*Cursor)(c).iterator.Next() {
		lastValidItem = (*Cursor)(c).iterator.Item()
	}
	if lastValidItem != nil {
		val, err := lastValidItem.Value()
		if err != nil {
			//TODO: handle error
			return nil, nil
		}
		return lastValidItem.Key(), val
	}

	return nil, nil
}

// Next moves the cursor one key/value pair forward and returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Next() (key, value []byte) {
	//fmt.Println("Next")
	for (*Cursor)(c).iterator.Next(); (*Cursor)(c).iterator.ValidForPrefix(c.key); (*Cursor)(c).iterator.Next() {
		item := (*Cursor)(c).iterator.Item()
		//fmt.Println("Item: ", item)
		val, err := item.Value()
		if err != nil {
			//TODO: handle error
			return nil, nil
		}
		return item.Key(), val
	}
	return nil, nil
}

// Prev moves the cursor one key/value pair backward and returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Prev() (key, value []byte) {
	fmt.Println("Prev")
	//Not Yet Implemented
	return nil, nil
}

// Seek positions the cursor at the passed seek key. If the key does not exist,
// the cursor is moved to the next key after seek. Returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) (key, value []byte) {
	//fmt.Println("Seek")
	prefix, err := addPrefix((*Cursor)(c).key, seek)
	if err != nil {
		fmt.Println("Seek err:", err)
		return nil, nil
	}
	(*Cursor)(c).iterator.Seek(prefix)
	if bytes.Equal(prefix, (*Cursor)(c).iterator.Item().Key()) {
		item := (*Cursor)(c).iterator.Item()
		val, err := item.Value()
		if err != nil {
			//TODO: handle error
			return nil, nil
		}
		return item.Key(), val
	}
	return nil, nil
}

// db represents a collection of namespaces which are persisted and implements
// the walletdb.Db interface.  All database access is performed through
// transactions which are obtained through the specific Namespace.
type db badger.DB

// Enforce db implements the walletdb.Db interface.
var _ walletdb.DB = (*db)(nil)

func (db *db) beginTx(writable bool) (*transaction, error) {
	tx := (*badger.DB)(db).NewTransaction(writable)
	tran := &transaction{badgerTx: tx, writable: writable, db: (*badger.DB)(db)}
	return tran, nil
}

func (db *db) BeginReadTx() (walletdb.ReadTx, error) {
	return db.beginTx(false)
}

func (db *db) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	return db.beginTx(true)
}

// Copy writes a copy of the database to the provided writer.  This call will
// start a read-only transaction to perform all operations.
//
// This function is part of the walletdb.Db interface implementation.
func (db *db) Copy(w io.Writer) error {
	fmt.Println("Copy Writter")
	return nil
}

// Close cleanly shuts down the database and syncs all data.
//
// This function is part of the walletdb.Db interface implementation.
func (db *db) Close() error {
	fmt.Println("Close DB!!!")
	if ticker != nil {
		ticker.Stop()
	}
	return convertErr((*badger.DB)(db).Close())
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	//fmt.Println("File Exists")
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

var ticker *time.Ticker

// openDB opens the database at the provided path.
func openDB(dbPath string, create bool) (walletdb.DB, error) {
	fmt.Println("Open DB called !!!")
	if !create && !fileExists(dbPath) {
		return nil, errors.E(errors.NotExist, "missing database file")
	}

	opts := badger.DefaultOptions
	opts.Dir = dbPath
	opts.ValueDir = dbPath
	opts.ValueLogLoadingMode = options.FileIO
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogFileSize = 64 << 20
	opts.MaxTableSize = 64 << 20
	opts.LevelOneSize = 256 << 16

	badgerDb, err := badger.Open(opts)

	go func() {
		ticker = time.NewTicker(5 * time.Minute)

		for range ticker.C {
		again:
			err := badgerDb.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	return (*db)(badgerDb), convertErr(err)
}
