package utils


// 10_000 station name with 
type CustomMap struct {
    data [][]interface{}
}

func NewCustomMap(initialSize int) *CustomMap {
    return &CustomMap{
        data: make([][]interface{}, 0, initialSize),
    }
}

// func Get(key []byte) (interface{}, bool) {
// }

// func (m *CustomMap) Put(key []byte, val interface{}) {
//     id := xxhash.Sum64(key)
    
//     if len()

//     m.data[id] = append(m.data[id], val)
// }
