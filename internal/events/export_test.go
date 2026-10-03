package events

// SetBeforeCommit installs a hook run after a batch's records are flushed to
// Kafka and before its transaction ends. Returning an error makes Run return
// without ending the transaction; blocking holds the transaction open.
func SetBeforeCommit(d *Deriver, f func() error) { d.beforeCommit = f }

// SetOnEnd observes whether each transaction committed or aborted.
func SetOnEnd(d *Deriver, f func(committed bool)) { d.onEnd = f }
