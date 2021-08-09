package storage

type IDB interface {
	Set(k []byte, v []byte) error
	Delete(k []byte) error
	Flush() error
	Close() error
	FindById(id string) (map[string]interface{}, error)
}
