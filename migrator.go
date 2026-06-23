// Copyright 2026 Andres Morey
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beehive

import "encoding/json"

// Migrator upgrades a kind's stored Spec/Status JSON to the shape this build
// expects, at the rawToTyped decode boundary. Beehive stores Spec and Status as
// opaque JSON and records the schema version each blob was written at (per
// column); on read, a blob older than this build's current version is converted
// up before it is unmarshalled, so a consumer can evolve its Spec/Status structs
// without breaking decode of rows written by an earlier build.
//
// Spec and Status carry independent versions and convert independently — a
// status-only write re-stamps only the status version. A current version of 0
// means "not versioned": that blob is never converted (the kind hasn't opted in
// for it). Conversion is lazy: bytes are upgraded on read and re-stamped only
// when the blob is next written, never by a bulk row rewrite.
//
// Register a Migrator per kind via WithMigrator passed to Register.
type Migrator interface {
	// SchemaVersionSpec is the spec schema version this build writes (and
	// converts up to). 0 means spec is not versioned for this kind.
	SchemaVersionSpec() int
	// SchemaVersionStatus is the status schema version this build writes (and
	// converts up to). 0 means status is not versioned for this kind.
	SchemaVersionStatus() int
	// ConvertSpec upgrades spec bytes written at version from to the current spec
	// version. It is called only when 0 <= from < SchemaVersionSpec(); from == 0 is
	// the unversioned baseline (a row written before this kind opted into
	// versioning), so once a migrator is enabled the converter must handle it.
	ConvertSpec(from int, raw json.RawMessage) (json.RawMessage, error)
	// ConvertStatus upgrades status bytes written at version from to the current
	// status version. It is called only when 0 <= from < SchemaVersionStatus();
	// from == 0 is the unversioned baseline the converter must handle (see
	// ConvertSpec).
	ConvertStatus(from int, raw json.RawMessage) (json.RawMessage, error)
}

// migratorSpecVersion is the spec schema version a write should stamp: the
// migrator's current spec version, or 0 when the kind has no migrator. Keeping
// the nil check here lets the write paths stamp unconditionally.
func migratorSpecVersion(m Migrator) int {
	if m == nil {
		return 0
	}
	return m.SchemaVersionSpec()
}

// migratorStatusVersion is migratorSpecVersion for the status column.
func migratorStatusVersion(m Migrator) int {
	if m == nil {
		return 0
	}
	return m.SchemaVersionStatus()
}
