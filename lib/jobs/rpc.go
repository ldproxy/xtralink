package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ldproxy/xtralink/model"
)

// The job queue over JSON-RPC 2.0: RPCHandler exposes a Backend, and
// RemoteBackend (rpc_client.go) is a Backend that calls one. Method names
// are the Backend method names, params are positional in the order of the
// Go arguments.
//
// Only what a Runner needs to execute PartialJobs is exposed (s.
// rpcMethods). Producer and admin calls - PushJob, Cancel - stay with the
// process that owns the queue, so anyone who can reach the endpoint can
// work the queue, but not fill or dismiss it.

const (
	rpcVersion = "2.0"

	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcBackendError   = -32000

	maxRPCRequestBytes = 16 << 20
)

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage   `json:"id,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("jobs rpc error %d: %s", e.Code, e.Message)
}

type rpcMethod func(backend Backend, params []json.RawMessage) (any, error)

var rpcMethods = map[string]rpcMethod{
	"Take": func(b Backend, p []json.RawMessage) (any, error) {
		var kind, executor string
		if err := decodeParams(p, &kind, &executor); err != nil {
			return nil, err
		}
		return b.Take(kind, executor)
	},
	"Done": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		if err := decodeParams(p, &id); err != nil {
			return nil, err
		}
		return nil, b.Done(id)
	},
	"Error": func(b Backend, p []json.RawMessage) (any, error) {
		var id, message string
		var retry bool
		if err := decodeParams(p, &id, &message, &retry); err != nil {
			return nil, err
		}
		return nil, b.Error(id, message, retry)
	},
	"DoneJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		if err := decodeParams(p, &id); err != nil {
			return nil, err
		}
		return nil, b.DoneJob(id)
	},
	"PushPartialJob": func(b Backend, p []json.RawMessage) (any, error) {
		var partialJob model.PartialJob
		var untake bool
		if err := decodeParams(p, &partialJob, &untake); err != nil {
			return nil, err
		}
		return nil, b.PushPartialJob(&partialJob, untake)
	},
	"Heartbeat": func(b Backend, p []json.RawMessage) (any, error) {
		var ids []string
		if err := decodeParams(p, &ids); err != nil {
			return nil, err
		}
		return nil, b.Heartbeat(ids)
	},
	"StartJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		if err := decodeParams(p, &id); err != nil {
			return nil, err
		}
		return nil, b.StartJob(id)
	},
	"GetJobs": func(b Backend, p []json.RawMessage) (any, error) {
		return b.GetJobs()
	},
	"GetJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		if err := decodeParams(p, &id); err != nil {
			return nil, err
		}
		return b.GetJob(id)
	},
	"GetPartialJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		if err := decodeParams(p, &id); err != nil {
			return nil, err
		}
		return b.GetPartialJob(id)
	},
	"GetOpen": func(b Backend, p []json.RawMessage) (any, error) {
		var kind string
		if err := decodeParams(p, &kind); err != nil {
			return nil, err
		}
		return b.GetOpen(kind)
	},
	"GetTaken": func(b Backend, p []json.RawMessage) (any, error) {
		return b.GetTaken()
	},
	"GetFailed": func(b Backend, p []json.RawMessage) (any, error) {
		return b.GetFailed()
	},
	"SetProgressDetails": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		var details map[string]any
		if err := decodeParams(p, &id, &details); err != nil {
			return nil, err
		}
		return nil, b.SetProgressDetails(id, details)
	},
	"SetOutput": func(b Backend, p []json.RawMessage) (any, error) {
		var id, key string
		var value model.OutputValue
		if err := decodeParams(p, &id, &key, &value); err != nil {
			return nil, err
		}
		return nil, b.SetOutput(id, key, value)
	},
	"SetOutputs": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		var outputs map[string]any
		if err := decodeParams(p, &id, &outputs); err != nil {
			return nil, err
		}
		return nil, b.SetOutputs(id, outputs)
	},
	"InitJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		var totalDelta int
		var updates []model.ProgressUpdate
		if err := decodeParams(p, &id, &totalDelta, &updates); err != nil {
			return nil, err
		}
		return nil, b.InitJob(id, totalDelta, updates)
	},
	"UpdateJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		var currentDelta int
		var updates []model.ProgressUpdate
		if err := decodeParams(p, &id, &currentDelta, &updates); err != nil {
			return nil, err
		}
		return nil, b.UpdateJob(id, currentDelta, updates)
	},
	"UpdatePartialJob": func(b Backend, p []json.RawMessage) (any, error) {
		var id string
		var currentDelta int
		if err := decodeParams(p, &id, &currentDelta); err != nil {
			return nil, err
		}
		return nil, b.UpdatePartialJob(id, currentDelta)
	},
}

type invalidParamsError struct{ error }

func decodeParams(params []json.RawMessage, targets ...any) error {
	if len(params) != len(targets) {
		return invalidParamsError{fmt.Errorf("expected %d params, got %d", len(targets), len(params))}
	}
	for i, target := range targets {
		if err := json.Unmarshal(params[i], target); err != nil {
			return invalidParamsError{fmt.Errorf("param %d: %w", i, err)}
		}
	}
	return nil
}

// RPCHandler serves a Backend over JSON-RPC 2.0, either from raw request
// bytes (Handle, used by the FFI passthrough) or over HTTP.
type RPCHandler struct {
	Backend Backend
}

func NewRPCHandler(backend Backend) *RPCHandler {
	return &RPCHandler{Backend: backend}
}

func (h *RPCHandler) Handle(request []byte) []byte {
	response, _ := json.Marshal(h.handle(request))
	return response
}

// RPCUnavailable answers request with an error, for a process that has no
// queue to serve.
func RPCUnavailable(request []byte, reason string) []byte {
	var parsed rpcRequest
	_ = json.Unmarshal(request, &parsed)
	response, _ := json.Marshal(rpcFailure(parsed.ID, rpcBackendError, reason))
	return response
}

func (h *RPCHandler) handle(raw []byte) rpcResponse {
	var request rpcRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return rpcFailure(nil, rpcParseError, err.Error())
	}
	if request.JSONRPC != rpcVersion || request.Method == "" {
		return rpcFailure(request.ID, rpcInvalidRequest, "not a JSON-RPC 2.0 request")
	}

	method, ok := rpcMethods[request.Method]
	if !ok {
		return rpcFailure(request.ID, rpcMethodNotFound, "method not available: "+request.Method)
	}

	result, err := method(h.Backend, request.Params)
	var invalidParams invalidParamsError
	switch {
	case errors.As(err, &invalidParams):
		return rpcFailure(request.ID, rpcInvalidParams, err.Error())
	case err != nil:
		return rpcFailure(request.ID, rpcBackendError, err.Error())
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return rpcFailure(request.ID, rpcBackendError, "encoding result: "+err.Error())
	}
	return rpcResponse{JSONRPC: rpcVersion, Result: encoded, ID: responseID(request.ID)}
}

func rpcFailure(id json.RawMessage, code int, message string) rpcResponse {
	return rpcResponse{JSONRPC: rpcVersion, Error: &RPCError{Code: code, Message: message}, ID: responseID(id)}
}

func responseID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

func (h *RPCHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	request, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRPCRequestBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(h.Handle(request))
}
