package task

import (
	"errors"
	"testing"

	"github.com/gdamore/tcell"
)

type heightTestScreen struct {
	tcell.Screen
	initErr error
	closed  bool
	quit    chan struct{}
}

func (s *heightTestScreen) Init() error {
	if s.initErr != nil {
		return s.initErr
	}
	s.quit = make(chan struct{})
	return nil
}

func (s *heightTestScreen) Fini() {
	close(s.quit) // Like tcell, cleanup requires successful initialization.
	s.closed = true
}

func (s *heightTestScreen) Size() (int, int) { return 80, 24 }

func TestScreenHeightInitFailure(t *testing.T) {
	wantErr := errors.New("terminal unavailable")
	screen := &heightTestScreen{initErr: wantErr}
	height, err := screenHeight(screen)
	if !errors.Is(err, wantErr) || height != 40 {
		t.Fatalf("got (%d, %v), want (40, %v)", height, err, wantErr)
	}
	if screen.closed {
		t.Fatal("finalized a screen whose initialization failed")
	}
}

func TestScreenHeightSuccess(t *testing.T) {
	screen := &heightTestScreen{}
	height, err := screenHeight(screen)
	if err != nil || height != 24 {
		t.Fatalf("got (%d, %v), want (24, nil)", height, err)
	}
	if !screen.closed {
		t.Fatal("initialized screen was not finalized")
	}
}
