package mock

type MockFramer struct {
	pos  int
	Data [][]byte
}

func (m *MockFramer) ReadBytesInternal() ([]byte, error) {
	if m.pos < len(m.Data) {
		m.pos = m.pos + 1
		return m.Data[m.pos-1], nil
	}
	return []byte{}, nil
}

func (*MockFramer) GetCustomPayload() map[string][]byte { return map[string][]byte{} }
func (*MockFramer) GetHeaderWarnings() []string         { return []string{} }
