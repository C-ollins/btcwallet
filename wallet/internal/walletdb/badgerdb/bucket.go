package badgerdb

import (
	"bytes"
	"fmt"

	"github.com/decred/dcrwallet/errors"
	"github.com/dgraph-io/badger"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 65378

	MaxPrefixSize = 50

	//Unique Identifier for a bucket
	MetaBucket = 5
)

type Bucket struct {
	key           []byte
	txn           *badger.Txn
	buckets       []*Bucket //sub buckets
	cursor        *Cursor
	dbTransaction *transaction
}

type Cursor struct {
	iterator *badger.Iterator
	txn      *badger.Txn
	key      []byte
}

func newBucket(tx *badger.Txn, badgerKey []byte, dbTx *transaction) (*Bucket, error) {
	prefix, err := createPrefix(badgerKey)
	if err != nil {
		fmt.Println("Prefix error: ", prefix)
		return nil, err
	}
	item, err := tx.Get(badgerKey)
	if err != nil {
		//Not Found
		err = tx.SetWithMeta(badgerKey, insertPrefixLength([]byte{}, len(badgerKey)), MetaBucket)
		if err != nil {
			fmt.Println("Unable to set with meta:", err, "key:", string(badgerKey))
			return nil, err
		}
		return &Bucket{txn: tx, key: prefix, dbTransaction: dbTx}, nil
	}
	if item.UserMeta() != MetaBucket {
		errors.E(errors.Invalid, "Key is not associated with a bucket: ", string(badgerKey))
	}
	return &Bucket{txn: tx, key: prefix, dbTransaction: dbTx}, nil
}

func trimByte(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		if b[i] != 0 {
			return b[i:]
		}
	}
	return []byte{}
}

func createPrefix(prefix []byte) ([]byte, error) {
	prefix = trimByte(prefix)
	//fmt.Println("Prefix: ", prefix)
	if len(prefix) > MaxPrefixSize {
		//TODO: handle long prefix here
		fmt.Println("Prefix is too long: ", len(prefix), "Max: ", MaxPrefixSize)
		return nil, nil
	}

	finalPrefix := make([]byte, MaxPrefixSize)
	finalPrefix = append(finalPrefix[len(prefix):MaxPrefixSize], prefix...)
	//fmt.Println("Final Prefix length: ", len(finalPrefix))
	//fmt.Println("Final Prefix: ", finalPrefix)
	return prefix, nil
}

func insertPrefixLength(val []byte, length int) []byte {
	result := make([]byte, 0)
	prefixBits := byte(length)
	result = append(result, prefixBits)
	result = append(result, val...)
	return result
}

func addPrefix(prefix []byte, key []byte) ([]byte, error) {
	if len(key) > MaxKeySize {
		//TODO: Handle long key here
		return nil, errors.E(errors.Invalid, "Key too long")
	}
	return append(prefix, key...), nil
}

//Change the transaction for bucket and sub buckets
func (b *Bucket) SetTx(tx *badger.Txn) {
	b.txn = tx
	for _, bkt := range b.buckets {
		bkt.SetTx(tx)
	}
}

func (b *Bucket) Iterator() *badger.Iterator {
	//Create a new Iterator
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 20
	it := b.txn.NewIterator(opts)
	b.dbTransaction.iterators = append(b.dbTransaction.iterators, it)
	return it
}

func (b *Bucket) BadgerCursor() *Cursor {
	cursor := &Cursor{iterator: b.Iterator(), txn: b.txn, key: b.key}
	return cursor
}

//Nested Bucket
func (b *Bucket) Bucket(key []byte, errorIfExists bool) (*Bucket, error) {
	if len(key) == 0 {
		//Empty Key
		return nil, errors.E(errors.Invalid, "Key is empty")
	}
	keyPrefix, err := addPrefix(b.key, key)
	if err != nil {
		fmt.Println("Error from adding prefix: ", err)
		return nil, err
	}
	//Convert len to MaxPrefixLen
	k, err := createPrefix(keyPrefix)
	if err != nil {
		fmt.Println("Error from creating prefix: ", err)
		return nil, err
	}
	item, err := b.txn.Get(k)
	if err != nil {
		//Key Not Found
		//fmt.Println("Creating New Bucket")
		bucket, err := newBucket(b.txn, k, b.dbTransaction)
		if err != nil {
			fmt.Println("Bucket not created:", err)
			return nil, err
		}
		//fmt.Println("Bucket:", string(k), "created")
		return bucket, nil
	}

	if item.UserMeta() == MetaBucket {
		if errorIfExists {
			return nil, errors.E(errors.Exist, "Bucket already exists")
		} else {
			bucket, err := newBucket(b.txn, k, b.dbTransaction)
			if err != nil {
				fmt.Println("Bucket not retrieve:", err)
				return nil, err
			}
			//fmt.Println("Bucket:", string(k), "retrieved")
			return bucket, nil
		}
	} else {
		return nil, errors.E(errors.Invalid, "Key is not associated with a bucket")
	}
}

func Dump(b *Bucket, key []byte) {
	it := b.Iterator()
	defer it.Close()
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		k := item.Key()
		val, _ := item.Value()
		prefixLength := int(val[0])
		if prefixLength == len(key) {
			fmt.Printf("key=%s, meta=%v, Key bytes %v: %v\n", k, item.UserMeta(), key, item.Key())
		}
	}
}

func (b *Bucket) RetrieveBucket(key []byte) *Bucket {
	if len(key) == 0 {
		fmt.Println("Retrieve Bucket empty key")
		//Empty Key
		return nil
	}

	k, err := addPrefix(b.key, key)
	if err != nil {
		fmt.Println("Retrieve bucket prefix error: ", err)
		return nil
	}
	keyPrefix, err := createPrefix(k)
	if err != nil {
		fmt.Println("Retrieve bucket prefix error: ", err)
		return nil
	}
	item, err := b.txn.Get(keyPrefix)
	if err != nil {
		//Bucket Not Found
		return nil
	}

	if item.UserMeta() == MetaBucket {
		//Retrieve bucket
		bucket, err := newBucket(b.txn, keyPrefix, b.dbTransaction)
		if err != nil {
			fmt.Println("Failed to create new bucket")
			return nil
		}
		b.buckets = append(b.buckets, bucket)
		return bucket
	} else {
		fmt.Println("Bucket key is not associated")
		//Key is not associated with a bucket
		return nil
	}
}

func (b *Bucket) DropBucket(key []byte) error {
	prefix, err := addPrefix(b.key, key)
	if err != nil {
		return err
	}
	it := b.Iterator()
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		b.txn.Delete(it.Item().Key())
	}
	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	if len(key) == 0 {
		//TODO: handle empty key
		return nil
	}
	k, err := addPrefix(b.key, key)
	if err != nil {
		fmt.Println("Get Failed to add prefix: ", err)
		//TODO: Handle error
	}

	item, err := b.txn.Get(k)
	if err != nil {
		//Not found
		return nil
	}
	val, err := item.Value()
	if err != nil {
		fmt.Println("Get Failed: ", err)
		//TODO: Handle error here
		return nil
	}
	return val[1:]
}

func (b *Bucket) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		fmt.Println("Put Key is empty")
		//Key empty
		return errors.E(errors.Invalid, "Key is empty")
	} else if len(key) > MaxKeySize {
		fmt.Println("Put Key is large")
		//Key too large
		return errors.E(errors.Invalid, "Key is empty")
	}
	k, err := addPrefix(b.key, key)
	if err != nil {
		fmt.Println("Put Key Failed to add prefix: ", err)
		return err
	}

	err = b.txn.Set(k, insertPrefixLength(value, len(b.key)))
	if err != nil {
		fmt.Println("Put Key Failed to put: ", err)
		return err
	}
	return err
}

func (b *Bucket) Delete(key []byte) error {
	if len(key) == 0 {
		//TODO: Handle empty key
		return nil
	}

	k, err := addPrefix(b.key, key)
	if err != nil {
		return err
	}

	return b.txn.Delete(k)
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	it := b.Iterator()
	defer it.Close()
	prefix := b.key
	it.Rewind()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		if bytes.Equal(item.Key(), prefix) {
			continue
		}
		v, err := item.Value()
		if err != nil {
			fmt.Println("Loop returning error")
			return err
		}
		prefixLength := int(v[0])
		if prefixLength == len(prefix) {
			if item.UserMeta() == MetaBucket {
				if err := fn(k[prefixLength:], nil); err != nil {
					return err
				}
			} else {
				if err := fn(k[prefixLength:], v[1:]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}