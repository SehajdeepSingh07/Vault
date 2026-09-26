package storage

import "sync"

type keyLocker struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyLocker() *keyLocker {
	return &keyLocker{locks: make(map[string]*keyLock)}
}

func (k *keyLocker) lock(key string) func() {
	k.mu.Lock()
	kl := k.locks[key]
	if kl == nil {
		kl = &keyLock{}
		k.locks[key] = kl
	}
	kl.refs++
	k.mu.Unlock()

	kl.mu.Lock()

	return func() {
		kl.mu.Unlock()
		k.mu.Lock()
		kl.refs--
		if kl.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
