package redisfarm

// RedisFarm holds collection of RedisStore identified by code.
type RedisFarm struct {
	code map[string]Storer
	db   map[int]Storer
}

var _ StoreFarmer = (*RedisFarm)(nil)

// NewRedisFarm cretes instance of RedisFarm.
func NewRedisFarm() *RedisFarm {
	return &RedisFarm{code: make(map[string]Storer), db: make(map[int]Storer)}
}

// Add adds redis store identified by code or db.
func (rf *RedisFarm) Add(code string, s Storer) {
	rf.code[code] = s
	rf.db[s.DB()] = s
}

// ByCode returns Storer by code.
func (rf *RedisFarm) ByCode(code string) Storer {
	res, ok := rf.code[code]
	if !ok {
		return nil
	}
	return res
}

// ByCode returns Storer by database number.
func (rf *RedisFarm) ByDB(db int) Storer {
	res, ok := rf.db[db]
	if !ok {
		return nil
	}
	return res
}

// Codes returns registered Storer codes.
func (rf *RedisFarm) Codes() []string {
	var res []string
	for k := range rf.code {
		res = append(res, k)
	}
	return res
}
