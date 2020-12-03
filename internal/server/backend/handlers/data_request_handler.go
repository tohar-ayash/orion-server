package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// dataRequestHandler handles query and transaction associated
// the user's data/state
type dataRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewDataRequestHandler returns handler capable to serve incoming data requests
func NewDataRequestHandler(db backend.DB, logger *logger.SugarLogger) http.Handler {
	handler := &dataRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetData, handler.dataQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostDataTx, handler.dataTransaction).Methods(http.MethodPost)

	return handler
}

func (d *dataRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dataRequestHandler) dataQuery(response http.ResponseWriter, request *http.Request) {
	queryEnv, respondedErr := extractDataQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, status := VerifyRequestSignature(d.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, status, err)
		return
	}

	dbName := queryEnv.GetPayload().GetDBName()
	if !d.db.IsDBExists(dbName) {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{
			ErrMsg: "error db '" + dbName + "' doesn't exist",
		})
		return
	}

	data, err := d.db.GetData(dbName, queryEnv.GetPayload().GetUserID(), queryEnv.GetPayload().GetKey())
	if err != nil {
		var status int

		switch err.(type) {
		case *backend.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&ResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (d *dataRequestHandler) dataTransaction(response http.ResponseWriter, request *http.Request) {
	requestData := json.NewDecoder(request.Body)
	requestData.DisallowUnknownFields()

	txEnv := &types.DataTxEnvelope{}
	if err := requestData.Decode(txEnv); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	if txEnv.Payload == nil {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserID == "" {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(d.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &ResponseErr{err.Error()})
		return
	}

	d.txHandler.handleTransaction(response, txEnv)
}
