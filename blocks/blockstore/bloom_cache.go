package blockstore

import (
	"github.com/ipfs/go-ipfs/blocks"
	key "github.com/ipfs/go-ipfs/blocks/key"
	bloom "gx/ipfs/QmPZP5NhZSUPH1eYEaEEjZGrwy3fLPF9yDAN6h9zGAkXAk/bbloom"
	//"gx/ipfs/QmVYxfoJQiZijTgPNHCHgHELvQpbsJNTg6Crmc3dQkj3yy/golang-lru"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
)

// BloomCached returns Blockstore that caches Has requests using Bloom filter
// Size is size of bloom filter in bytes
func BloomCached(bs Blockstore, size int) *bloomcache {
	bl := bloom.New(float64(size), float64(7))
	bc := &bloomcache{blockstore: bs, bloom: bl}
	bc.Invaludate()
	go bc.Rebuild()

	return bc
}

type bloomcache struct {
	bloom       bloom.Bloom
	active      bool
	rebuildChan chan struct{}

	blockstore Blockstore
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
	return b.blockstore.DeleteBlock(k)
}

func (b *bloomcache) Has(k key.Key) (bool, error) {
	if b.active {
		blr := b.bloom.HasTS([]byte(k))
		if blr == false {
			return false, nil
		}
	}
	return b.blockstore.Has(k)
}

func (b *bloomcache) Get(k key.Key) (blocks.Block, error) {
	return b.blockstore.Get(k)
}

func (b *bloomcache) Put(bl blocks.Block) error {
	b.bloom.AddTS([]byte(bl.Key()))
	return b.blockstore.Put(bl)
}

func (b *bloomcache) PutMany(bs []blocks.Block) error {
	for _, block := range bs {
		b.bloom.AddTS([]byte(block.Key()))
	}
	return b.blockstore.PutMany(bs)
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
