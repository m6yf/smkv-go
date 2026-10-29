package aerospikeshadow

import (
	"errors"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
)

// ClientV4 is a v4 Aerospike client with a shadow: writes Aerospike accepts
// are copied to SMKV, and a sample of reads is compared with SMKV's rows.
// Every other call goes to Aerospike alone.
type ClientV4 struct {
	*as.Client
	shadow *Shadow
}

// WrapV4 gives client the shadow; a nil shadow passes every call through.
func WrapV4(client *as.Client, shadow *Shadow) *ClientV4 {
	return &ClientV4{Client: client, shadow: shadow}
}

func (c *ClientV4) Put(policy *as.WritePolicy, key *as.Key, bins as.BinMap) error {
	err := c.Client.Put(policy, key, bins)
	if err == nil && c.shadow != nil {
		written := make([]bin, 0, len(bins))
		for name, value := range bins {
			written = append(written, bin{name, value})
		}
		c.shadow.write(key.Namespace(), key.SetName(), key.Value(), replacesV4(policy), written)
	}
	return err
}

func (c *ClientV4) PutBins(policy *as.WritePolicy, key *as.Key, bins ...*as.Bin) error {
	err := c.Client.PutBins(policy, key, bins...)
	if err == nil && c.shadow != nil {
		written := make([]bin, len(bins))
		for i, b := range bins {
			written[i] = bin{b.Name, b.Value}
		}
		c.shadow.write(key.Namespace(), key.SetName(), key.Value(), replacesV4(policy), written)
	}
	return err
}

func (c *ClientV4) Delete(policy *as.WritePolicy, key *as.Key) (bool, error) {
	existed, err := c.Client.Delete(policy, key)
	if err == nil {
		c.shadow.remove(key.Namespace(), key.SetName(), key.Value())
	}
	return existed, err
}

func (c *ClientV4) Get(policy *as.BasePolicy, key *as.Key, binNames ...string) (*as.Record, error) {
	record, err := c.Client.Get(policy, key, binNames...)
	if c.shadow.sampled() {
		switch {
		case err == nil && record != nil:
			c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, record.Bins, true)
		case notFoundV4(err):
			c.shadow.compare(key.Namespace(), key.SetName(), key.Value(), binNames, nil, false)
		}
	}
	return record, err
}

// replacesV4 reports whether a write under policy leaves the record holding
// only the bins written. A nil policy is the client's default, UPDATE.
func replacesV4(policy *as.WritePolicy) bool {
	if policy == nil {
		return false
	}
	switch policy.RecordExistsAction {
	case as.REPLACE, as.REPLACE_ONLY, as.CREATE_ONLY:
		return true
	}
	return false
}

func notFoundV4(err error) bool {
	var ae types.AerospikeError
	return errors.As(err, &ae) && ae.ResultCode() == types.KEY_NOT_FOUND_ERROR
}
