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

// trigger is one feed declared by WithTriggerByID or WithTriggerByName: a
// channel of addresses the app resolves within the kind and requeues. Exactly
// one of the two channels is set, and which one also selects the resolution.
type trigger struct {
	r     *reconciler
	ids   <-chan ObjectID
	names <-chan string
}
