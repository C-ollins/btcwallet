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
		err = tx.SetWithMeta(badgerKey, nil, MetaBucket)
		if err != nil {
			fmt.Println("Unable to set with meta:", err, "key:", badgerKey)
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
	//Return an iterator if already created
	if b.dbTransaction.iterator != nil {
		//fmt.Printf("Iterator %s Taken\n", b.key)
		return b.dbTransaction.iterator
	}
	//fmt.Printf("Iterator %s Created\n", b.key)
	//Create a new Iterator
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 5
	it := b.txn.NewIterator(opts)
	b.dbTransaction.iterator = it
	return it
}

func (b *Bucket) BadgerCursor() *Cursor {
	if b.cursor != nil {
		//fmt.Printf("Cursor %s Taken\n", b.key)
		return b.cursor
	}

	cursor := &Cursor{iterator: b.Iterator(), txn: b.txn, key: b.key}
	b.cursor = cursor
	//fmt.Printf("Cursor %s Created\n", b.key)
	return cursor
}

func (b *Bucket) CloseCursor() {
	if b.cursor != nil {
		//fmt.Println("Closing Cursor", string(b.key))
		//b.cursor.iterator.Close()
		//fmt.Printf("Cursor %s Closed\n", b.key)
		b.cursor = nil
	}
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
			//fmt.Println("Retreiving Bucket", string(k))
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
	//it := b.Iterator()
	//defer it.Close()
	//it.Seek(k)
	// if it.Valid() {
	// 	// Return an error if there is an existing key.
	// 	if bytes.Equal(k, it.Item().Key()) {
	// 		fmt.Println("Bucket already exists:", string(it.Item().Key()))
	// 		//Key Already Exists
	// 		item := it.Item()
	// 		//If Item is a bucket
	// 		if item.UserMeta() == MetaBucket {
	// 			if errorIfExists {
	// 				return nil, errors.E(errors.Exist, "Bucket already exists")
	// 			}
	// 		} else {
	// 			return nil, errors.E(errors.Invalid, "Key is not associated with a bucket")
	// 		}
	// 	}
	// }
	// fmt.Println("Creating New Bucket")
	// bucket, err := newBucket(b.txn, k)
	// if err != nil {
	// 	fmt.Println("Bucket not created:", err)
	// 	return nil, err
	// }
	// fmt.Println("Bucket:", string(k), "created")
	//b.buckets = append(b.buckets, bucket)
	// return bucket, nil
}

func Dump(b *Bucket, key []byte) {
	it := b.txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		fmt.Printf("key=%s, meta=%v, Equal to %s: %v\n", k, item.UserMeta(), key, bytes.Equal(k, key))
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
		fmt.Println("RetrieveBucket not found: ", err, " ", string(keyPrefix))
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

	// it := b.Iterator()
	// defer it.Close()
	// it.Seek(k)
	// // Return nil if there isn't an existing key.
	// if bytes.Equal(k, it.Item().Key()) {
	// 	item := it.Item()
	// 	if item.UserMeta() == MetaBucket {
	// 		//Retrieve bucket
	// 		bucket, err := newBucket(b.txn, k)
	// 		if err != nil {
	// 			return nil
	// 		}
	// 		b.buckets = append(b.buckets, bucket)
	// 		return bucket
	// 	}
	// }
	// return nil
}

func (b *Bucket) DropBucket(key []byte) error {
	fmt.Println("Drop Bucket:", string(key))
	key, err := addPrefix(b.key, key)
	if err != nil {
		return err
	}
	item, err := b.txn.Get(key)
	if err != nil {
		return err
	}
	err = b.txn.Delete(item.Key())
	if err != nil {
		return err
	}
	// it := b.Iterator()
	// defer it.Close()
	// for it.Seek(key); it.ValidForPrefix(key); it.Next() {
	// 	item := it.Item()
	// 	err = b.txn.Delete(item.Key())
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	if len(key) == 0 {
		fmt.Println("Get Key is empty")
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
		//fmt.Println("Get Key not found: ", err, " ", string(k))
		//Not found
		return nil
	}
	// fmt.Println("Get Key len:",len(item.Key()), "Key: ",item.Key())
	// fmt.Println("Key: ", string(item.Key()), "Prefix: ", string(item.Key()[:MaxPrefixSize]))
	val, err := item.Value()
	if err != nil {
		fmt.Println("Get Failed: ", err)
		//TODO: Handle error here
		return nil
	}
	//fmt.Println("Get Key: ", b.dbTransaction.uuid, " ", string(k))
	// fmt.Println("Prefix: ", string(b.key),"Val:", val)
	// fmt.Println("Return Val:", )

	//First byte is prefix length
	return val[1:]
	// it := b.Iterator()
	// defer it.Close()
	// it.Seek(k)

	// // If our target node isn't the same key as what's passed in then return nil.
	// if !bytes.Equal(k, it.Item().Key()) {
	// 	return nil
	// }
	// val, err := it.Item().Value()
	// if err != nil {
	// 	//TODO: Handle error here
	// 	return nil
	// }
	// return val
}

func (b *Bucket) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		fmt.Println("Put Key is empty")
		//Key empty
		return nil
	} else if len(key) > MaxKeySize {
		fmt.Println("Put Key is large")
		//Key too large
		return nil
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
		if item.UserMeta() == MetaBucket {
			continue
		}
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return err
		}
		prefixLength := int(v[0])
		if err := fn(k[prefixLength:], v[1:]); err != nil {
			return err
		}
	}
	return nil
}