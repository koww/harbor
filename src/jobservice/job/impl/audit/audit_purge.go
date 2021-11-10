// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package audit

import (
	"time"

	"github.com/goharbor/harbor/src/common"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/lib/config"
	"github.com/goharbor/harbor/src/pkg/audit"
)

// Job is a sample to show how to implement a job.
type AuditPurge struct {
	auditMgr audit.Manager
}

// MaxFails is implementation of same method in Interface.
func (ap *AuditPurge) MaxFails() uint {
	return 3
}

// MaxCurrency is implementation of same method in Interface.
func (ap *AuditPurge) MaxCurrency() uint {
	return 1
}

// ShouldRetry ...
func (ap *AuditPurge) ShouldRetry() bool {
	return true
}

// Validate is implementation of same method in Interface.
func (ap *AuditPurge) Validate(params job.Parameters) error {
	return nil
}

// Run the audit purge logic to purge audit older than given period of time
func (ap *AuditPurge) Run(ctx job.Context, params job.Parameters) error {
	logger := ctx.GetLogger()

	logger.Info("Audit Purge job starting")
	cfg, err := config.GetSystemCfg(ctx.SystemContext())
	if err != nil {
		logger.Errorf("Error occurred getting config: %v", err)
		return err
	}
	retention := cfg[common.AuditRetentionLimit].(uint)
	if retention == 0 {
		logger.Info("Audit Purge job exited without operations")
		return nil
	}

	num, err := ap.auditMgr.Purge(ctx.SystemContext(), getPurgeTimestamp(retention))
	if err != nil {
		logger.Errorf("Audit Purge job failed with error: %v", err)
		return err
	}
	logger.Infof("Audit Purge job exited with %v audits purged", num)
	return nil
}

func getPurgeTimestamp(retention uint) time.Time {
	year, month, day := time.Now().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.Now().UTC().Location()).Add(time.Hour * 24 * time.Duration(retention))
}
