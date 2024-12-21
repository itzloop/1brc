package processors

type Processor interface {
    Process(p string) (result string, err error)
}
