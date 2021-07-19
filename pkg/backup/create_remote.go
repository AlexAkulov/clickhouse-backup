package backup

import "fmt"

func (b *Backuper) CreateToRemote(backupName, tablePattern, diffFrom string, schemaOnly, rbac, backupConfig bool, version string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := CreateBackup(b.cfg, backupName, tablePattern, schemaOnly, rbac, backupConfig, version); err != nil {
		return err
	}
	if err := b.Upload(backupName, tablePattern, diffFrom, schemaOnly); err != nil {
		return err
	}
	if err := RemoveOldBackupsLocal(b.cfg, false); err != nil {
		return fmt.Errorf("can't remove old local backups: %v", err)
	}
	return nil
}
