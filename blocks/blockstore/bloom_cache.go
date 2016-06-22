package blockstore

import (
	"github.com/ipfs/go-ipfs/blocks"
	key "github.com/ipfs/go-ipfs/blocks/key"
	lru "gx/ipfs/QmVYxfoJQiZijTgPNHCHgHELvQpbsJNTg6Crmc3dQkj3yy/golang-lru"
	bloom "gx/ipfs/QmWQ2SJisXwcCLsUXLwYCKSfyExXjFRW2WbBH5sqCUnwX5/bbloom"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
)

// BloomCached returns Blockstore that caches Has requests using Bloom filter
// Size is size of bloom filter in bytes
func BloomCached(bs Blockstore, bloomSize, lruSize int) (*bloomcache, error) {
	bl, err := bloom.New(float64(bloomSize), float64(7))
	if err != nil {
		return nil, err
	}
	arc, err := lru.NewARC(lruSize)
	if err != nil {
		return nil, err
	}
	bc := &bloomcache{blockstore: bs, bloom: bl, arc: arc}
	bc.Invaludate()
	go bc.Rebuild()

	return bc, nil
}

type bloomcache struct {
	bloom  *bloom.Bloom
	active bool

	arc *lru.ARCCache
	// This chan is only used for testing to wait for bloom to enable
	rebuildChan chan struct{}
	blockstore  Blockstore
}

func (b *bloomcache) Invaludate() {
	b.rebuildChan = make(chan struct{})
	b.active = false
}

func (b *bloomcache) BloomActive() bool {
	return b.active
}

func (b *bloomcache) Rebuild() {
	ctx := context.TODO()
	evt := log.EventBegin(ctx, "bloomcache.Rebuild")
	defer evt.Done()

	ch, err := b.blockstore.AllKeysChan(ctx)
	if err != nil {
		log.Errorf("AllKeysChan failed in bloomcache rebuild with: %v", err)
	}
	for key := range ch {
		b.bloom.AddTS([]byte(key)) // Use binary key, the more compact the better
	}
	close(b.rebuildChan)
	b.active = true
}

func (b *bloomcache) DeleteBlock(k key.Key) error {
	err := b.blockstore.DeleteBlock(k)
	if err == nil {
		b.arc.Add(k, false)
	}
	return err
}

// return true means that there for sure is no such object
// return false means that there might be that object
func (b *bloomcache) hasNot(k key.Key) bool {
	if k == "" {
		return true
	}
	if b.active {
		blr := b.bloom.HasTS([]byte(k))
		if blr == false {
			return true
		}
	}
	return false
}

func (b *bloomcache) Has(k key.Key) (bool, error) {
	if b.hasNot(k) {
		return false, nil
	}
	if has, hit := b.arc.Get(k); hit {
		return has.(bool), nil
	}

	res, err := b.blockstore.Has(k)
	if err == nil {
		b.arc.Add(k, res)
	}
	return res, err
}

func (b *bloomcache) Get(k key.Key) (blocks.Block, error) {
	if b.hasNot(k) {
		return nil, ErrNotFound
	}
	if has, hit := b.arc.Get(k); hit && !(has.(bool)) {
		return nil, ErrNotFound
	}

	bl, err := b.blockstore.Get(k)
	if bl == nil && err == ErrNotFound {
		b.arc.Add(k, false)
	} else if bl != nil {
		b.arc.Add(k, true)
	}
	return bl, err
}

func (b *bloomcache) Put(bl blocks.Block) error {
	if has, hit := b.arc.Get(bl.Key()); hit && (has.(bool)) {
		return nil
	}
	err := b.blockstore.Put(bl)
	if err == nil {
		b.bloom.AddTS([]byte(bl.Key()))
		b.arc.Add(bl.Key(), true)
	}
	return err
}

func (b *bloomcache) PutMany(bs []blocks.Block) error {
	err := b.blockstore.PutMany(bs)
	if err == nil {
		for _, block := range bs {
			b.bloom.AddTS([]byte(block.Key()))
		}
	}
	return err
}

func (b *bloomcache) AllKeysChan(ctx context.Context) (<-chan key.Key, error) {
	return b.blockstore.AllKeysChan(ctx)
}

func (b *bloomcache) GCLock() Unlocker {
	return b.blockstore.(GCBlockstore).GCLock()
}

func (b *bloomcache) PinLock() Unlocker {
	return b.blockstore.(GCBlockstore).PinLock()
}

func (b *bloomcache) GCRequested() bool {
	return b.blockstore.(GCBlockstore).GCRequested()
}
