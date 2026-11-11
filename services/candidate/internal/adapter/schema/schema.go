// Package schema implements app.SchemaValidator with the contracts/events
// JSON Schemas (pkg/event/eventschema).
package schema

import "github.com/nuroqzavitex/smarthire/pkg/event/eventschema"

// Validator implements app.SchemaValidator. Its errors may echo document
// values; callers must not log them.
type Validator struct{}

// ValidateEvent validates a serialized envelope of eventType.
func (Validator) ValidateEvent(eventType string, doc []byte) error {
	return eventschema.Validate(eventType, doc)
}

// ValidateParsedCV validates a ParsedCV document against
// cv.parsed $defs.ParsedCV.
func (Validator) ValidateParsedCV(doc []byte) error {
	return eventschema.ValidateDef("cv.parsed", "ParsedCV", doc)
}
