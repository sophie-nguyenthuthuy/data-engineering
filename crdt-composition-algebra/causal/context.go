package causal

// Context is a compact causal context (dotted version vector).
// For each node, it tracks:
//   - max: the highest contiguous counter seen (all counters 1..max are known)
//   - dots: exceptional dots above max (gaps in the sequence)
//
// This compact representation lets us efficiently determine if one node's history
// subsumes another's, which drives the delta-state sync protocol.
type Context struct {
	// max[n] = highest contiguous counter for node n (i.e., 1..max[n] all seen)
	max map[NodeID]uint64
	// dots = exceptional seen events above their node's max
	dots DotSet
}

func NewContext() Context {
	return Context{
		max:  make(map[NodeID]uint64),
		dots: make(DotSet),
	}
}

// Next returns the next dot for a node and advances the context.
// The returned dot is the new event; the context is updated in place.
func (c *Context) Next(node NodeID) Dot {
	counter := c.max[node] + 1
	// Skip any exceptional dots to find the true next
	for c.dots.Contains(Dot{node, counter}) {
		delete(c.dots, Dot{node, counter})
		counter++
	}
	c.max[node] = counter
	return Dot{Node: node, Counter: counter}
}

// PeekNext returns the next dot for a node WITHOUT modifying the context.
// Use this when computing a delta — let the Join advance the state's cc.
func (c Context) PeekNext(node NodeID) Dot {
	counter := c.max[node] + 1
	for c.dots.Contains(Dot{node, counter}) {
		counter++
	}
	return Dot{Node: node, Counter: counter}
}

// MaxFor returns the max contiguous counter seen for node.
func (c Context) MaxFor(node NodeID) uint64 {
	return c.max[node]
}

// Contains returns true if this context has seen the given dot.
func (c Context) Contains(d Dot) bool {
	if c.max[d.Node] >= d.Counter {
		return true
	}
	return c.dots.Contains(d)
}

// Add adds a dot to this context and compresses if possible.
func (c *Context) Add(d Dot) {
	if c.max[d.Node] >= d.Counter {
		return // already known
	}
	c.dots[d] = struct{}{}
	c.compress(d.Node)
}

// compress fills in contiguous gaps from max upward.
func (c *Context) compress(node NodeID) {
	for {
		next := Dot{Node: node, Counter: c.max[node] + 1}
		if c.dots.Contains(next) {
			delete(c.dots, next)
			c.max[node]++
		} else {
			break
		}
	}
}

// Join merges two causal contexts (takes the union of all seen events).
func (c Context) Join(other Context) Context {
	result := NewContext()
	// Merge maxes
	allNodes := make(map[NodeID]struct{})
	for n := range c.max {
		allNodes[n] = struct{}{}
	}
	for n := range other.max {
		allNodes[n] = struct{}{}
	}
	for n := range allNodes {
		cm, om := c.max[n], other.max[n]
		if cm > om {
			result.max[n] = cm
		} else {
			result.max[n] = om
		}
	}
	// Merge exceptional dots
	for d := range c.dots {
		result.dots[d] = struct{}{}
	}
	for d := range other.dots {
		result.dots[d] = struct{}{}
	}
	// Compress all
	for n := range allNodes {
		result.compress(n)
	}
	return result
}

// LessEq returns true if every event in c is also in other.
// This is used to determine if a delta is needed.
func (c Context) LessEq(other Context) bool {
	// Check all dots in c are in other
	for d := range c.dots {
		if !other.Contains(d) {
			return false
		}
	}
	// Check all contiguous ranges
	for node, maxC := range c.max {
		for i := uint64(1); i <= maxC; i++ {
			if !other.Contains(Dot{Node: node, Counter: i}) {
				return false
			}
		}
	}
	return true
}

// Clone returns a deep copy.
func (c Context) Clone() Context {
	result := NewContext()
	for k, v := range c.max {
		result.max[k] = v
	}
	for k := range c.dots {
		result.dots[k] = struct{}{}
	}
	return result
}

// Dots returns all dots explicitly tracked (above max).
func (c Context) Dots() DotSet {
	return c.dots.Clone()
}

// AllDots returns all dots represented by this context (max range + exceptional).
// Mainly for testing; can be large.
func (c Context) AllDots() DotSet {
	result := c.dots.Clone()
	for node, m := range c.max {
		for i := uint64(1); i <= m; i++ {
			result[Dot{node, i}] = struct{}{}
		}
	}
	return result
}
