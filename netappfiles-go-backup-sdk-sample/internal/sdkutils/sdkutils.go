// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// This package centralizes any function that directly
// using any of the Azure's (with exception of authentication related ones)
// available SDK packages.

package sdkutils

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"netappfiles-go-backup-sdk-sample/internal/iam"
	"netappfiles-go-backup-sdk-sample/internal/uri"
	"netappfiles-go-backup-sdk-sample/internal/utils"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/netapp/mgmt/netapp"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/resources"
	"github.com/Azure/go-autorest/autorest/to"
)

const (
	userAgent = "anf-go-sdk-sample-agent"
	nfsv3     = "NFSv3"
	nfsv41    = "NFSv4.1"
	cifs      = "CIFS"
)

var (
	validProtocols = []string{nfsv3, nfsv41, cifs}
)

func validateANFServiceLevel(serviceLevel string) (validatedServiceLevel netapp.ServiceLevel, err error) {
	var svcLevel netapp.ServiceLevel

	switch strings.ToLower(serviceLevel) {
	case "ultra":
		svcLevel = netapp.ServiceLevelUltra
	case "premium":
		svcLevel = netapp.ServiceLevelPremium
	case "standard":
		svcLevel = netapp.ServiceLevelStandard
	default:
		return "", fmt.Errorf("invalid service level, supported service levels are: %v", netapp.PossibleServiceLevelValues())
	}

	return svcLevel, nil
}

func getResourcesClient() (resources.Client, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return resources.Client{}, err
	}

	client := resources.NewClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getAccountsClient() (netapp.AccountsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.AccountsClient{}, err
	}

	client := netapp.NewAccountsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getAccountBackupsClient() (netapp.AccountBackupsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.AccountBackupsClient{}, err
	}

	client := netapp.NewAccountBackupsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getPoolsClient() (netapp.PoolsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.PoolsClient{}, err
	}

	client := netapp.NewPoolsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getVolumesClient() (netapp.VolumesClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.VolumesClient{}, err
	}

	client := netapp.NewVolumesClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getSnapshotsClient() (netapp.SnapshotsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.SnapshotsClient{}, err
	}

	client := netapp.NewSnapshotsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getSnapshotPoliciesClient() (netapp.SnapshotPoliciesClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.SnapshotPoliciesClient{}, err
	}

	client := netapp.NewSnapshotPoliciesClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getBackupPoliciesClient() (netapp.BackupPoliciesClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.BackupPoliciesClient{}, err
	}

	client := netapp.NewBackupPoliciesClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getBackupsClient() (netapp.BackupsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.BackupsClient{}, err
	}

	client := netapp.NewBackupsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

func getVaultsClient() (netapp.VaultsClient, error) {
	authorizer, subscriptionID, err := iam.GetAuthorizer()
	if err != nil {
		return netapp.VaultsClient{}, err
	}

	client := netapp.NewVaultsClient(subscriptionID)
	client.Authorizer = authorizer
	client.AddToUserAgent(userAgent)

	return client, nil
}

// GetResourceByID gets a generic resource
func GetResourceByID(ctx context.Context, resourceID, APIVersion string) (resources.GenericResource, error) {
	resourcesClient, err := getResourcesClient()
	if err != nil {
		return resources.GenericResource{}, err
	}

	parentResource := ""
	resourceGroup := uri.GetResourceGroup(resourceID)
	resourceProvider := uri.GetResourceValue(resourceID, "providers")
	resourceName := uri.GetResourceName(resourceID)
	resourceType := uri.GetResourceValue(resourceID, resourceProvider)

	if strings.Contains(resourceID, "/subnets/") {
		parentResourceName := uri.GetResourceValue(resourceID, resourceType)
		parentResource = fmt.Sprintf("%v/%v", resourceType, parentResourceName)
		resourceType = "subnets"
	}

	return resourcesClient.Get(
		ctx,
		resourceGroup,
		resourceProvider,
		parentResource,
		resourceType,
		resourceName,
		APIVersion,
	)
}

// CreateANFAccount creates an ANF Account resource
func CreateANFAccount(ctx context.Context, location, resourceGroupName, accountName string, activeDirectories []netapp.ActiveDirectory, tags map[string]*string) (netapp.Account, error) {
	accountClient, err := getAccountsClient()
	if err != nil {
		return netapp.Account{}, err
	}

	accountProperties := netapp.AccountProperties{}

	if activeDirectories != nil {
		accountProperties = netapp.AccountProperties{
			ActiveDirectories: &activeDirectories,
		}
	}

	future, err := accountClient.CreateOrUpdate(
		ctx,
		netapp.Account{
			Location:          to.StringPtr(location),
			Tags:              tags,
			AccountProperties: &accountProperties,
		},
		resourceGroupName,
		accountName,
	)
	if err != nil {
		return netapp.Account{}, fmt.Errorf("cannot create account: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, accountClient.Client)
	if err != nil {
		return netapp.Account{}, fmt.Errorf("cannot get the account create or update future response: %v", err)
	}

	return future.Result(accountClient)
}

// CreateANFCapacityPool creates an ANF Capacity Pool within ANF Account
func CreateANFCapacityPool(ctx context.Context, location, resourceGroupName, accountName, poolName, serviceLevel string, sizeBytes int64, tags map[string]*string) (netapp.CapacityPool, error) {
	poolClient, err := getPoolsClient()
	if err != nil {
		return netapp.CapacityPool{}, err
	}

	svcLevel, err := validateANFServiceLevel(serviceLevel)
	if err != nil {
		return netapp.CapacityPool{}, err
	}

	future, err := poolClient.CreateOrUpdate(
		ctx,
		netapp.CapacityPool{
			Location: to.StringPtr(location),
			Tags:     tags,
			PoolProperties: &netapp.PoolProperties{
				ServiceLevel: svcLevel,
				Size:         to.Int64Ptr(sizeBytes),
			},
		},
		resourceGroupName,
		accountName,
		poolName,
	)

	if err != nil {
		return netapp.CapacityPool{}, fmt.Errorf("cannot create pool: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, poolClient.Client)
	if err != nil {
		return netapp.CapacityPool{}, fmt.Errorf("cannot get the pool create or update future response: %v", err)
	}

	return future.Result(poolClient)
}

// CreateANFVolume creates an ANF volume within a Capacity Pool
func CreateANFVolume(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, serviceLevel, subnetID, snapshotID string, backupID string, protocolTypes []string, volumeUsageQuota int64, unixReadOnly, unixReadWrite bool, tags map[string]*string, dataProtectionObject netapp.VolumePropertiesDataProtection) (netapp.Volume, error) {
	if len(protocolTypes) > 2 {
		return netapp.Volume{}, fmt.Errorf("maximum of two protocol types are supported")
	}

	if len(protocolTypes) > 1 && utils.Contains(protocolTypes, "NFSv4.1") {
		return netapp.Volume{}, fmt.Errorf("only cifs/nfsv3 protocol types are supported as dual protocol")
	}

	if snapshotID != "" && backupID != "" {
		return netapp.Volume{}, fmt.Errorf("volume cannot be created from both snapshotID and backupID")
	}

	_, found := utils.FindInSlice(validProtocols, protocolTypes[0])
	if !found {
		return netapp.Volume{}, fmt.Errorf("invalid protocol type, valid protocol types are: %v", validProtocols)
	}

	svcLevel, err := validateANFServiceLevel(serviceLevel)
	if err != nil {
		return netapp.Volume{}, err
	}

	volumeClient, err := getVolumesClient()
	if err != nil {
		return netapp.Volume{}, err
	}

	exportPolicy := netapp.VolumePropertiesExportPolicy{}

	if _, found := utils.FindInSlice(protocolTypes, cifs); !found {
		exportPolicy = netapp.VolumePropertiesExportPolicy{
			Rules: &[]netapp.ExportPolicyRule{
				{
					AllowedClients: to.StringPtr("0.0.0.0/0"),
					Cifs:           to.BoolPtr(map[bool]bool{true: true, false: false}[protocolTypes[0] == cifs]),
					Nfsv3:          to.BoolPtr(map[bool]bool{true: true, false: false}[protocolTypes[0] == nfsv3]),
					Nfsv41:         to.BoolPtr(map[bool]bool{true: true, false: false}[protocolTypes[0] == nfsv41]),
					RuleIndex:      to.Int32Ptr(1),
					UnixReadOnly:   to.BoolPtr(unixReadOnly),
					UnixReadWrite:  to.BoolPtr(unixReadWrite),
				},
			},
		}
	}

	volumeProperties := netapp.VolumeProperties{
		SnapshotID:     map[bool]*string{true: to.StringPtr(snapshotID), false: nil}[snapshotID != ""],
		BackupID:       map[bool]*string{true: to.StringPtr(backupID), false: nil}[backupID != ""],
		ExportPolicy:   map[bool]*netapp.VolumePropertiesExportPolicy{true: &exportPolicy, false: nil}[protocolTypes[0] != cifs],
		ProtocolTypes:  &protocolTypes,
		ServiceLevel:   svcLevel,
		SubnetID:       to.StringPtr(subnetID),
		UsageThreshold: to.Int64Ptr(volumeUsageQuota),
		CreationToken:  to.StringPtr(volumeName),
		DataProtection: &dataProtectionObject,
	}

	future, err := volumeClient.CreateOrUpdate(
		ctx,
		netapp.Volume{
			Location:         to.StringPtr(location),
			Tags:             tags,
			VolumeProperties: &volumeProperties,
		},
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
	)

	if err != nil {
		return netapp.Volume{}, fmt.Errorf("cannot create volume: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, volumeClient.Client)
	if err != nil {
		return netapp.Volume{}, fmt.Errorf("cannot get the volume create or update future response: %v", err)
	}

	return future.Result(volumeClient)
}

// UpdateANFVolume update an ANF volume
func UpdateANFVolume(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName string, volumePropertiesPatch netapp.VolumePatchProperties, tags map[string]*string) (netapp.VolumesUpdateFuture, error) {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return netapp.VolumesUpdateFuture{}, err
	}

	volume, err := volumeClient.Update(
		ctx,
		netapp.VolumePatch{
			Location:              to.StringPtr(location),
			Tags:                  tags,
			VolumePatchProperties: &volumePropertiesPatch,
		},
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
	)

	if err != nil {
		return netapp.VolumesUpdateFuture{}, fmt.Errorf("cannot update volume: %v", err)
	}

	return volume, nil
}

// AuthorizeReplication - authorizes volume replication
func AuthorizeReplication(ctx context.Context, resourceGroupName, accountName, poolName, volumeName, remoteVolumeResourceID string) error {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumeClient.AuthorizeReplication(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		netapp.AuthorizeRequest{
			RemoteVolumeResourceID: to.StringPtr(remoteVolumeResourceID),
		},
	)

	if err != nil {
		return fmt.Errorf("cannot authorize volume replication: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, volumeClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get authorize volume replication future response: %v", err)
	}

	return nil
}

// DeleteANFVolumeReplication - authorizes volume replication
func DeleteANFVolumeReplication(ctx context.Context, resourceGroupName, accountName, poolName, volumeName string) error {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumeClient.DeleteReplication(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete volume replication: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, volumeClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get delete volume replication future response: %v", err)
	}

	return nil
}

// CreateANFSnapshot creates a Snapshot from an ANF volume
func CreateANFSnapshot(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, snapshotName string, tags map[string]*string) (netapp.Snapshot, error) {
	snapshotClient, err := getSnapshotsClient()
	if err != nil {
		return netapp.Snapshot{}, err
	}

	future, err := snapshotClient.Create(
		ctx,
		netapp.Snapshot{
			Location: to.StringPtr(location),
		},
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		snapshotName,
	)

	if err != nil {
		return netapp.Snapshot{}, fmt.Errorf("cannot create snapshot: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, snapshotClient.Client)
	if err != nil {
		return netapp.Snapshot{}, fmt.Errorf("cannot get the snapshot create or update future response: %v", err)
	}

	return future.Result(snapshotClient)
}

// DeleteANFSnapshot deletes a Snapshot from an ANF volume
func DeleteANFSnapshot(ctx context.Context, resourceGroupName, accountName, poolName, volumeName, snapshotName string) error {
	snapshotClient, err := getSnapshotsClient()
	if err != nil {
		return err
	}

	future, err := snapshotClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		snapshotName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete snapshot: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, snapshotClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the snapshot delete future response: %v", err)
	}

	return nil
}

// CreateANFSnapshotPolicy creates a Snapshot Policy to be used on volumes
func CreateANFSnapshotPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, policy netapp.SnapshotPolicy) (netapp.SnapshotPolicy, error) {
	snapshotPolicyClient, err := getSnapshotPoliciesClient()
	if err != nil {
		return netapp.SnapshotPolicy{}, err
	}

	snapshotPolicy, err := snapshotPolicyClient.Create(
		ctx,
		policy,
		resourceGroupName,
		accountName,
		policyName,
	)

	if err != nil {
		return netapp.SnapshotPolicy{}, fmt.Errorf("cannot create snapshot policy: %v", err)
	}

	return snapshotPolicy, nil
}

// UpdateANFSnapshotPolicy update an ANF volume
func UpdateANFSnapshotPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, snapshotPolicyPatch netapp.SnapshotPolicyPatch) (netapp.SnapshotPoliciesUpdateFuture, error) {
	snapshotPolicyClient, err := getSnapshotPoliciesClient()
	if err != nil {
		return netapp.SnapshotPoliciesUpdateFuture{}, err
	}

	snapshotPolicy, err := snapshotPolicyClient.Update(
		ctx,
		snapshotPolicyPatch,
		resourceGroupName,
		accountName,
		policyName,
	)

	if err != nil {
		return netapp.SnapshotPoliciesUpdateFuture{}, fmt.Errorf("cannot update snapshot policy: %v", err)
	}

	return snapshotPolicy, nil
}

// CreateANFBackupPolicy creates a Snapshot Policy to be used on volumes
func CreateANFBackupPolicy(ctx context.Context, location, resourceGroupName, accountName, policyName string, policyProperties netapp.BackupPolicyProperties) (netapp.BackupPolicy, error) {
	backupPolicyClient, err := getBackupPoliciesClient()
	if err != nil {
		return netapp.BackupPolicy{}, err
	}

	backupPolicyBody := netapp.BackupPolicy{
		Location:               to.StringPtr(location),
		BackupPolicyProperties: &policyProperties,
	}

	future, err := backupPolicyClient.Create(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		backupPolicyBody,
	)

	if err != nil {
		return netapp.BackupPolicy{}, fmt.Errorf("cannot create backup policy: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, backupPolicyClient.Client)
	if err != nil {
		return netapp.BackupPolicy{}, fmt.Errorf("cannot get the backup create or update future response: %v", err)
	}

	backupPolicy, _ := future.Result(backupPolicyClient)

	return backupPolicy, nil
}

// CreateANFBackup creates an adhoc backup from volume
func CreateANFBackup(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, backupName, backupLabel string) (netapp.Backup, error) {
	backupsClient, err := getBackupsClient()
	if err != nil {
		return netapp.Backup{}, err
	}

	backupBody := netapp.Backup{
		Location: to.StringPtr(location),
		BackupProperties: &netapp.BackupProperties{
			Label: to.StringPtr(backupLabel),
		},
	}

	future, err := backupsClient.Create(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		backupName,
		backupBody,
	)

	if err != nil {
		return netapp.Backup{}, fmt.Errorf("cannot create backup: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, backupsClient.Client)
	if err != nil {
		return netapp.Backup{}, fmt.Errorf("cannot get the backup create or update future response: %v", err)
	}

	backup, _ := future.Result(backupsClient)

	return backup, nil
}

// GetANFVault gets an netappAccount/vaults resource
func GetANFVaultList(ctx context.Context, resourceGroupName, accountName string) (netapp.VaultList, error) {
	vaultsClient, err := getVaultsClient()
	if err != nil {
		return netapp.VaultList{}, err
	}

	vaults, err := vaultsClient.List(ctx,
		resourceGroupName,
		accountName,
	)

	if err != nil {
		return netapp.VaultList{}, fmt.Errorf("cannot retrieve list of vaults from account %v: %v", accountName, err)
	}

	return vaults, nil
}

// UpdateANFBackupPolicy update an ANF Backup Policy
func UpdateANFBackupPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, backupPolicyPatch netapp.BackupPolicyPatch) (netapp.BackupPolicy, error) {
	backupPoliciesClient, err := getBackupPoliciesClient()
	if err != nil {
		return netapp.BackupPolicy{}, err
	}

	future, err := backupPoliciesClient.Update(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		backupPolicyPatch,
	)

	if err != nil {
		return netapp.BackupPolicy{}, fmt.Errorf("cannot update snapshot policy: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, backupPoliciesClient.Client)
	if err != nil {
		return netapp.BackupPolicy{}, fmt.Errorf("cannot get the snapshot create or update future response: %v", err)
	}

	return future.Result(backupPoliciesClient)
}

// DeleteANFVolume deletes a volume
func DeleteANFVolume(ctx context.Context, resourceGroupName, accountName, poolName, volumeName string) error {
	volumesClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumesClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete volume: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, volumesClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the volume delete future response: %v", err)
	}

	return nil
}

// DeleteANFCapacityPool deletes a capacity pool
func DeleteANFCapacityPool(ctx context.Context, resourceGroupName, accountName, poolName string) error {
	poolsClient, err := getPoolsClient()
	if err != nil {
		return err
	}

	future, err := poolsClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete capacity pool: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, poolsClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the capacity pool delete future response: %v", err)
	}

	return nil
}

// DeleteANFSnapshotPolicy deletes a snapshot policy
func DeleteANFSnapshotPolicy(ctx context.Context, resourceGroupName, accountName, policyName string) error {
	snapshotPolicyClient, err := getSnapshotPoliciesClient()
	if err != nil {
		return err
	}

	future, err := snapshotPolicyClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete snapshot policy: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, snapshotPolicyClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the snapshot policy delete future response: %v", err)
	}

	return nil
}

// DeleteANFBackupPolicy deletes a backup policy
func DeleteANFBackupPolicy(ctx context.Context, resourceGroupName, accountName, policyName string) error {
	backupPolicyClient, err := getBackupPoliciesClient()
	if err != nil {
		return err
	}

	future, err := backupPolicyClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete backup policy: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, backupPolicyClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the backup policy delete future response: %v", err)
	}

	return nil
}

// DeleteANFBackups deletes backups, if shouldRemoveAllRegionalBackups is true, all regional backups are deleted
func DeleteANFBackups(ctx context.Context, parentResourceID string, waitForSnapmirror, deleteLastBackup bool) error {
	var err error

	// Checking if Volume is enabled for backups, if not, exit
	if uri.IsANFVolume(parentResourceID) {
		volumesClient, err := getVolumesClient()
		if err != nil {
			return err
		}

		volume, err := volumesClient.Get(
			ctx,
			uri.GetResourceGroup(parentResourceID),
			uri.GetANFAccount(parentResourceID),
			uri.GetANFCapacityPool(parentResourceID),
			uri.GetANFVolume(parentResourceID),
		)

		if err != nil {
			return err
		}

		if volume.DataProtection.Backup == nil {
			return nil
		}
	}

	// Geting backup client depending on which level
	// we are targeting (volume or account level)
	backupsClient := netapp.BackupsClient{}
	accountBackupsClient := netapp.AccountBackupsClient{}

	if uri.IsANFVolume(parentResourceID) {
		backupsClient, err = getBackupsClient()
	} else {
		accountBackupsClient, err = getAccountBackupsClient()
	}

	if err != nil {
		return err
	}

	// wait until initial backup shows up on get list of backups
	// this is due to initial backup always require a snapmirror to
	// be created.
	if waitForSnapmirror {
		err = waitForANFSnapmirrorAvailable(
			ctx,
			parentResourceID,
			15,
			40,
		)

		if err != nil {
			return fmt.Errorf("an error ocurred while waiting for snapmirror: %v", err)
		}
	}

	backupList := netapp.BackupsList{}

	if uri.IsANFVolume(parentResourceID) {
		backupList, _ = backupsClient.List(
			ctx,
			uri.GetResourceGroup(parentResourceID),
			uri.GetANFAccount(parentResourceID),
			uri.GetANFCapacityPool(parentResourceID),
			uri.GetANFVolume(parentResourceID),
		)
	} else {
		backupList, _ = accountBackupsClient.List(
			ctx,
			uri.GetResourceGroup(parentResourceID),
			uri.GetANFAccount(parentResourceID),
		)
	}

	// Converting backupList into a slice
	backups := make([]netapp.Backup, 0)
	for _, item := range *backupList.Value {
		backups = append(backups, item)
	}

	// Sorting backups by date ascending.
	// Backups needs to be deleted from newest to oldest.
	sort.SliceStable(backups, func(i, j int) bool {
		return backups[i].CreationDate.ToTime().Before(backups[j].CreationDate.ToTime())
	})

	utils.ConsoleOutput("\tBackup deletion order:")
	for i, backup := range backups {
		message := fmt.Sprintf("\t\t%v - BackupName: %v, CreationDate: %v", i, *backup.Name, backup.CreationDate)

		if i == len(backups)-1 {
			if !deleteLastBackup {
				message = fmt.Sprintf("\t\t%v - BackupName: %v, CreationDate: %v (deferred to account level removal)", i, *backup.Name, backup.CreationDate)
			}
		}

		utils.ConsoleOutput(message)
	}

	for i, backup := range backups {
		// Checking if we should delete last backup, the last one cannot be deleted
		// from the Volume, only from account level
		if i == len(backups)-1 {
			if !deleteLastBackup {
				break
			}
		}

		utils.ConsoleOutput(fmt.Sprintf("\t\tDeleting backup %v", *backup.Name))

		futureBackupsClient := netapp.BackupsDeleteFuture{}
		futureAccountBackupsClient := netapp.AccountBackupsDeleteFuture{}

		if uri.IsANFVolume(parentResourceID) {
			futureBackupsClient, err = backupsClient.Delete(
				ctx,
				uri.GetResourceGroup(*backup.ID),
				uri.GetANFAccount(*backup.ID),
				uri.GetANFCapacityPool(*backup.ID),
				uri.GetANFVolume(*backup.ID),
				uri.GetANFBackup(*backup.ID),
			)
		} else {
			// Making sure we're deleting only backups tied to this account since
			// all backups for a region can be shown in any account for the same
			// region
			if strings.ToLower(uri.GetANFAccount(*backup.ID)) == strings.ToLower(uri.GetANFAccount(parentResourceID)) {
				futureAccountBackupsClient, err = accountBackupsClient.Delete(
					ctx,
					uri.GetResourceGroup(*backup.ID),
					uri.GetANFAccount(*backup.ID),
					uri.GetANFAccountBackup(*backup.ID),
				)
			} else {
				continue
			}
		}

		if err != nil {
			return fmt.Errorf("cannot delete backup: %v", err)
		}

		if uri.IsANFVolume(parentResourceID) {
			err = futureBackupsClient.WaitForCompletionRef(ctx, backupsClient.Client)
		} else {
			err = futureAccountBackupsClient.WaitForCompletionRef(ctx, accountBackupsClient.Client)
		}

		if err != nil {
			return fmt.Errorf("cannot get the backup delete future response: %v", err)
		}

		err = WaitForANFResource(ctx, *backup.ID, 30, 100, false, true)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("an error ocurred while waiting for backup complete deletion: %v", err))
		}
		utils.ConsoleOutput("\t\tBackup deleted")
	}

	return nil
}

// DeleteANFAccount deletes an account
func DeleteANFAccount(ctx context.Context, resourceGroupName, accountName string) error {
	accountsClient, err := getAccountsClient()
	if err != nil {
		return err
	}

	future, err := accountsClient.Delete(
		ctx,
		resourceGroupName,
		accountName,
	)

	if err != nil {
		return fmt.Errorf("cannot delete account: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, accountsClient.Client)
	if err != nil {
		return fmt.Errorf("cannot get the account delete future response: %v", err)
	}

	return nil
}

// WaitForANFResource waits for a specified resource to be fully ready following a creation operation or for it to be
// not found if waitForNoAnfResource is true.
// This is due to a known issue related to ARM Cache where the state of the resource is still cached within ARM infrastructure
// reporting that it still exists so looping into a get process will return 404 as soon as the cached state expires
func WaitForANFResource(ctx context.Context, resourceID string, intervalInSec int, retries int, checkForReplication, waitForNoAnfResource bool) error {
	var err error
	resourceIdIdentified := false

	for i := 0; i < retries; i++ {
		time.Sleep(time.Duration(intervalInSec) * time.Second)
		if uri.IsANFSnapshot(resourceID) {
			resourceIdIdentified = true
			client, _ := getSnapshotsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFCapacityPool(resourceID),
				uri.GetANFVolume(resourceID),
				uri.GetANFSnapshot(resourceID),
			)
		} else if uri.IsANFBackup(resourceID) {
			resourceIdIdentified = true
			client, _ := getBackupsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFCapacityPool(resourceID),
				uri.GetANFVolume(resourceID),
				uri.GetANFBackup(resourceID),
			)
		} else if uri.IsANFAccountBackup(resourceID) {
			resourceIdIdentified = true
			client, _ := getAccountBackupsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFAccountBackup(resourceID),
			)
		} else if uri.IsANFVolume(resourceID) {
			resourceIdIdentified = true
			client, _ := getVolumesClient()
			if !checkForReplication {
				resourceIdIdentified = true
				_, err = client.Get(
					ctx,
					uri.GetResourceGroup(resourceID),
					uri.GetANFAccount(resourceID),
					uri.GetANFCapacityPool(resourceID),
					uri.GetANFVolume(resourceID),
				)
			} else {
				resourceIdIdentified = true
				_, err = client.ReplicationStatusMethod(
					ctx,
					uri.GetResourceGroup(resourceID),
					uri.GetANFAccount(resourceID),
					uri.GetANFCapacityPool(resourceID),
					uri.GetANFVolume(resourceID),
				)
			}
		} else if uri.IsANFCapacityPool(resourceID) {
			resourceIdIdentified = true
			client, _ := getPoolsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFCapacityPool(resourceID),
			)
		} else if uri.IsANFSnapshotPolicy(resourceID) {
			resourceIdIdentified = true
			client, _ := getSnapshotPoliciesClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFSnapshotPolicy(resourceID),
			)
		} else if uri.IsANFBackupPolicy(resourceID) {
			resourceIdIdentified = true
			client, _ := getBackupPoliciesClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFBackupPolicy(resourceID),
			)
		} else if uri.IsANFAccount(resourceID) {
			resourceIdIdentified = true
			client, _ := getAccountsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
			)
		}

		if resourceIdIdentified == false {
			return fmt.Errorf("resource id %v is not properly handled by WaitForANFResource function.", resourceID)
		}

		if waitForNoAnfResource {
			// Error is expected in this case
			if err != nil {
				return nil
			}
		} else {
			// We exit when there is no error
			if err == nil {
				return nil
			}
		}
	}

	return fmt.Errorf("wait not exited within expected parameters, number of retries %v and waitForNoAnfResource %v, error: %v", retries, waitForNoAnfResource, err)
}

// WaitForANFBackupCompletion waits for the specified backup to be completed.
func WaitForANFBackupCompletion(ctx context.Context, backupID string, intervalInSec int, retries int) error {
	var err error

	if !uri.IsANFBackup(backupID) {
		return fmt.Errorf("resource id is not a backup resource: %v", backupID)
	}

	backupsClient, err := getBackupsClient()
	if err != nil {
		return fmt.Errorf("an error ocurred while getting BackupsClient: %v", err)
	}

	backup := netapp.Backup{}

	for i := 0; i < retries; i++ {
		time.Sleep(time.Duration(intervalInSec) * time.Second)

		backup, _ = backupsClient.Get(
			ctx,
			uri.GetResourceGroup(backupID),
			uri.GetANFAccount(backupID),
			uri.GetANFCapacityPool(backupID),
			uri.GetANFVolume(backupID),
			uri.GetANFBackup(backupID),
		)

		// In this case, we exit when provisioning state is Succeeded
		if *backup.ProvisioningState == "Succeeded" {
			return nil
		}
	}

	return fmt.Errorf("backup didn't complete after number of retries %v, and wait time between retry %v", retries, intervalInSec)
}

// waitForANFSnapmirror waits until snapmirror is available from a get
func waitForANFSnapmirrorAvailable(ctx context.Context, parentResourceID string, intervalInSec int, retries int) error {
	var err error

	if !(uri.IsANFVolume(parentResourceID) || uri.IsANFAccount(parentResourceID)) {
		return fmt.Errorf("resource id %v is not a volume or account type", parentResourceID)
	}

	backupsClient := netapp.BackupsClient{}
	accountBackupsClient := netapp.AccountBackupsClient{}

	if uri.IsANFVolume(parentResourceID) {
		backupsClient, err = getBackupsClient()
	} else {
		accountBackupsClient, err = getAccountBackupsClient()
	}

	if err != nil {
		return err
	}

	for i := 0; i < retries; i++ {
		time.Sleep(time.Duration(intervalInSec) * time.Second)

		backupList := netapp.BackupsList{}

		if uri.IsANFVolume(parentResourceID) {
			backupList, _ = backupsClient.List(
				ctx,
				uri.GetResourceGroup(parentResourceID),
				uri.GetANFAccount(parentResourceID),
				uri.GetANFCapacityPool(parentResourceID),
				uri.GetANFVolume(parentResourceID),
			)
		} else {
			backupList, _ = accountBackupsClient.List(
				ctx,
				uri.GetResourceGroup(parentResourceID),
				uri.GetANFAccount(parentResourceID),
			)
		}

		// Exiting if no backups are defined since this will not generate
		// the snapmirror initial snapshot used by backups
		if len(*backupList.Value) == 0 {
			return nil
		}

		for _, item := range *backupList.Value {
			if strings.Contains(*item.Name, "snapmirror") {
				return nil
			}
		}
	}

	return fmt.Errorf("wait exceed # of attempts to get snapmirror, number of retries %v and interval in seconds %v", retries, intervalInSec)
}
