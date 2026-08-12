// Scaffolded by genffi. Yours to edit.
//
// The factory `InitLibrary` calls, in the package next to the wrapper. genffi
// writes the shape — one getter per `@singleton`, which is what the generated
// `cInit` in `main/clib.go` requires — and stops there, because which
// implementation each one returns is yours to say.
//
// Written only when it is absent, so it is safe to edit: every later run finds
// it and leaves it alone. Delete it to get this scaffold back.
//
// The fingerprint below is how a later run tells that the definitions have moved
// on from what is here — it says so and still leaves the file alone. Keep the
// line to keep being told; remove it and nothing is reported.
//
// genffi:scaffold 003e2eddb25e

package clib

import (
	api "github.com/ldproxy/xtralink/ffi/api"
	impl "github.com/ldproxy/xtralink/ffi/impl"
)

type initStub struct{}

func (initStub) JobQueue() api.JobQueue { return impl.NewJobQueue() }

func (initStub) JobProcessors() api.JobProcessors { return impl.NewJobProcessors() }

func NewInit() initStub { return initStub{} }
