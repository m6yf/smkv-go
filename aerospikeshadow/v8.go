package aerospikeshadow

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ClientV8 is a v8 Aerospike client with a shadow: writes Aerospike accepts
// are copied to SMKV, and a sample of reads, batch reads included, is
// compared with SMKV's rows. Scans and every other call go to Aerospike alone.
type ClientV8 struct {
	*as.Client
	shadow *Shadow
}

// WrapV8 gives client the shadow; a nil shadow passes every call through.
func WrapV8(client *as.Client, shadow *Shadow) *ClientV8 {
	return &ClientV8{Client: client, shadow: shadow}
}

func (c *ClientV8) Put(policy *as.WritePolicy, key *as.Key, bins as.BinMap) as.Error {
	err := c.Client.Put(policy, key, bins)
	if err == nil && c.shadow != nil {
		written := make([]bin, 0, len(bins))
		for name, value := range bins {
			written = append(written, bin{name, value})
		}
		c.shadow.write(key.Namespace(), key.SetName(), key.Value(), replacesV8(policy), written)
	}
	return err
}

func (c *ClientV8) PutBins(policy *as.WritePolicy, key *as.Key, bins ...*as.Bin) as.Error {
	err := c.Client.PutBins(policy, key, bins...)
	if err == nil && c.shadow != nil {
		written := make([]bin, len(bins))
		for i, b := range bins {
			written[i] = bin{b.Name, b.Value}
		}
		c.shadow.write(key.Namespace(), key.SetName(), key.Value(), replacesV8(policy), written)
	}
	return err
}

func (c *ClientV8) Delete(policy *as.WritePolicy, key *as.Key) (bool, as.Error) {
	existed, err := c.Client.Delete(policy, key)
	if err == nil {
		c.shadow.remove(key.Namespace(), key.SetName(), key.Value())
	}
	return existed, err
}

func (c *ClientV8) Get(policy *as.BasePolicy, key *as.Key, binNames ...string) (*as.Record, as.Error) {
	record, err := c.Client.Get(policy, key, binNames...)
	if c.shadow.sampled() {
		switch {
		case err == nil && record != nil:
			c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, record.Bins, true)
		case err != nil && err.Matches(types.KEY_NOT_FOUND_ERROR):
			c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, nil, false)
		}
	}
	return record, err
}

func (c *ClientV8) BatchGet(policy *as.BatchPolicy, keys []*as.Key, binNames ...string) ([]*as.Record, as.Error) {
	records, err := c.Client.BatchGet(policy, keys, binNames...)
	if err == nil && len(records) == len(keys) && c.shadow.sampled() {
		for i, key := range keys {
			if records[i] == nil {
				c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, nil, false)
			} else {
				c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, records[i].Bins, true)
			}
		}
	}
	return records, err
}

// replacesV8 reports whether a write under policy leaves the record holding
// only the bins written. A nil policy is the client's default, UPDATE.
func replacesV8(policy *as.WritePolicy) bool {
	if policy == nil {
		return false
	}
	switch policy.RecordExistsAction {
	case as.REPLACE, as.REPLACE_ONLY, as.CREATE_ONLY:
		return true
	}
	return false
}
