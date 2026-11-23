package smuggle

// Code 是稳定的业务码，用于定位解析与裁决中的异常类别。
type Code string

const (
	CodeOK               Code = "OK"
	CodeCLConflict       Code = "E_CL_CONFLICT"
	CodeCLTEConflict     Code = "E_CL_TE_CONFLICT"
	CodeCLMalformed      Code = "E_CL_MALFORMED"
	CodeTEUnknown        Code = "E_TE_UNKNOWN"
	CodeTEOrder          Code = "E_TE_ORDER"
	CodeChunkMalformed   Code = "E_CHUNK_MALFORMED"
	CodeChunkObsFold     Code = "E_CHUNK_OBSFOLD"
	CodeHeaderMalformed  Code = "E_HEADER_MALFORMED"
	CodeStartMalformed   Code = "E_STARTLINE_MALFORMED"
	CodeBodyTruncated    Code = "E_BODY_TRUNCATED"
	CodeRequestBudget    Code = "E_REQUEST_BUDGET"
	CodeBatchContention  Code = "E_BATCH_CONTENTION"
	CodeSmuggleAmbiguity Code = "E_SMUGGLE_AMBIGUITY"
	CodeSnapshotVersion  Code = "E_SNAPSHOT_VERSION"
)
