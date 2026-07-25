package fiber

import (
	"testing"
)

func TestNewStack(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	if s == nil {
		t.Fatal("NewStack returned nil")
	}

	if s.Size() != DefaultStackSize {
		t.Errorf("Expected size %d, got %d", DefaultStackSize, s.Size())
	}

	if s.Used() != 0 {
		t.Errorf("Expected used 0, got %d", s.Used())
	}

	if s.Available() != DefaultStackSize {
		t.Errorf("Expected available %d, got %d", DefaultStackSize, s.Available())
	}
}

func TestStackMinMaxSize(t *testing.T) {
	t.Parallel()
	// Test minimum size
	s := NewStack(1024)
	if s.Size() != MinStackSize {
		t.Errorf("Expected min size %d, got %d", MinStackSize, s.Size())
	}

	// Test maximum size
	s = NewStack(10 * 1024 * 1024)
	if s.Size() != MaxStackSize {
		t.Errorf("Expected max size %d, got %d", MaxStackSize, s.Size())
	}

	// Test normal size
	s = NewStack(128 * 1024)
	if s.Size() != 128*1024 {
		t.Errorf("Expected size %d, got %d", 128*1024, s.Size())
	}
}

func TestStackPush(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	data := []byte("test data")
	err := s.Push(data)

	if err != nil {
		t.Errorf("Push failed: %v", err)
	}

	if s.Used() != len(data) {
		t.Errorf("Expected used %d, got %d", len(data), s.Used())
	}

	if s.Available() != DefaultStackSize-len(data) {
		t.Errorf("Expected available %d, got %d", DefaultStackSize-len(data), s.Available())
	}
}

func TestStackPop(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	original := []byte("test data")
	if err := s.Push(original); err != nil {
		t.Fatal(err)
	}

	popped, err := s.Pop(len(original))

	if err != nil {
		t.Errorf("Pop failed: %v", err)
	}

	if string(popped) != string(original) {
		t.Errorf("Expected '%s', got '%s'", string(original), string(popped))
	}

	if s.Used() != 0 {
		t.Errorf("Expected used 0, got %d", s.Used())
	}
}

func TestStackOverflow(t *testing.T) {
	t.Parallel()
	s := NewStack(MinStackSize)

	data := make([]byte, MinStackSize+100)
	err := s.Push(data)

	if err == nil {
		t.Error("Expected overflow error, got nil")
	}
}

func TestStackUnderflow(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	_, err := s.Pop(100)

	if err == nil {
		t.Error("Expected underflow error, got nil")
	}
}

func TestStackRejectsNegativePop(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)
	if _, err := s.Pop(-1); err == nil {
		t.Fatal("negative pop should return an error")
	}
	if got := s.Used(); got != 0 {
		t.Fatalf("negative pop changed stack usage to %d", got)
	}
}

func TestStackReset(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	data := []byte("test data")
	if err := s.Push(data); err != nil {
		t.Fatal(err)
	}

	s.Reset()

	if s.Used() != 0 {
		t.Errorf("Expected used 0 after reset, got %d", s.Used())
	}

	if s.Available() != DefaultStackSize {
		t.Errorf("Expected available %d after reset, got %d", DefaultStackSize, s.Available())
	}
}

func TestStackUsage(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)

	if s.Usage() != 0.0 {
		t.Errorf("Expected usage 0.0, got %f", s.Usage())
	}

	halfSize := s.Size() / 2
	data := make([]byte, halfSize)
	if err := s.Push(data); err != nil {
		t.Fatal(err)
	}

	expected := 0.5
	actual := s.Usage()
	if actual < expected-0.01 || actual > expected+0.01 {
		t.Errorf("Expected usage around 0.5, got %f", actual)
	}
}

func TestStackClone(t *testing.T) {
	t.Parallel()
	original := NewStack(DefaultStackSize)
	data := []byte("test data")
	if err := original.Push(data); err != nil {
		t.Fatal(err)
	}

	clone := original.Clone()

	if clone.Size() != original.Size() {
		t.Error("Clone should have same size")
	}

	if clone.Used() != original.Used() {
		t.Error("Clone should have same used space")
	}

	// Verify independent copies
	original.Reset()

	if clone.Used() == 0 {
		t.Error("Clone should not be affected by original reset")
	}
}

func TestStackGetInfo(t *testing.T) {
	t.Parallel()
	s := NewStack(DefaultStackSize)
	quarterSize := s.Size() / 4
	data := make([]byte, quarterSize)
	if err := s.Push(data); err != nil {
		t.Fatal(err)
	}

	info := s.GetInfo()

	if info.Size != s.Size() {
		t.Errorf("Expected size %d, got %d", s.Size(), info.Size)
	}

	if info.Used != quarterSize {
		t.Errorf("Expected used %d, got %d", quarterSize, info.Used)
	}

	expectedAvailable := s.Size() - quarterSize
	if info.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, info.Available)
	}

	expected := 0.25
	if info.Usage < expected-0.01 || info.Usage > expected+0.01 {
		t.Errorf("Expected usage around 0.25, got %f", info.Usage)
	}
}

func TestValidateStackSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    int
		expected int
	}{
		{1024, MinStackSize},
		{MinStackSize, MinStackSize},
		{MinStackSize - 1, MinStackSize},
		{DefaultStackSize, DefaultStackSize},
		{MaxStackSize, MaxStackSize},
		{MaxStackSize + 1000, MaxStackSize},
		{128 * 1024, 128 * 1024},
	}

	for _, tt := range tests {
		result := ValidateStackSize(tt.input)
		if result != tt.expected {
			t.Errorf("ValidateStackSize(%d) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}
