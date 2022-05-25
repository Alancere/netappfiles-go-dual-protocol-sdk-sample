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
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"netappfiles-go-backup-sdk-sample/internal/uri"
	"netappfiles-go-backup-sdk-sample/internal/utils"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/netapp/armnetapp"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
)

const (
	userAgent = "anf-go-sdk-sample-agent"
	nfsv3     = "NFSv3"
	nfsv41    = "NFSv4.1"
	cifs      = "CIFS"
)

var (
	validProtocols = []string{nfsv3, nfsv41, cifs}
	SubscriptionID string
)

func init() {
	SubscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	if SubscriptionID == "" {
		log.Fatal("The environment variable AZURE_SUBSCRIPTION_ID does not exist, please set it.")
	}
}

func validateANFServiceLevel(serviceLevel string) (validatedServiceLevel armnetapp.ServiceLevel, err error) {
	var svcLevel armnetapp.ServiceLevel

	switch strings.ToLower(serviceLevel) {
	case "ultra":
		svcLevel = armnetapp.ServiceLevelUltra
	case "premium":
		svcLevel = armnetapp.ServiceLevelPremium
	case "standard":
		svcLevel = armnetapp.ServiceLevelStandard
	default:
		return "", fmt.Errorf("invalid service level, supported service levels are: %v", armnetapp.PossibleServiceLevelValues())
	}

	return svcLevel, nil
}

func getResourcesClient() (*armresources.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armresources.NewClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getAccountsClient() (*armnetapp.AccountsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewAccountsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getAccountBackupsClient() (*armnetapp.AccountBackupsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewAccountBackupsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getPoolsClient() (*armnetapp.PoolsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewPoolsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getVolumesClient() (*armnetapp.VolumesClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewVolumesClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getSnapshotsClient() (*armnetapp.SnapshotsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewSnapshotsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getSnapshotPoliciesClient() (*armnetapp.SnapshotPoliciesClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewSnapshotPoliciesClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getBackupPoliciesClient() (*armnetapp.BackupPoliciesClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewBackupPoliciesClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getBackupsClient() (*armnetapp.BackupsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	client, err := armnetapp.NewBackupsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getVaultsClient() (*armnetapp.VaultsClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := armnetapp.NewVaultsClient(SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetResourceByID gets a generic resource
func GetResourceByID(ctx context.Context, resourceID, APIVersion string) (armresources.GenericResource, error) {
	resourcesClient, err := getResourcesClient()
	if err != nil {
		return armresources.GenericResource{}, err
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
	resp, err := resourcesClient.Get(
		ctx,
		resourceGroup,
		resourceProvider,
		parentResource,
		resourceType,
		resourceName,
		APIVersion,
		nil,
	)
	return resp.GenericResource, err
}

// CreateANFAccount creates an ANF Account resource
func CreateANFAccount(ctx context.Context, location, resourceGroupName, accountName string, activeDirectories []*armnetapp.ActiveDirectory, tags map[string]*string) (*armnetapp.Account, error) {
	accountClient, err := getAccountsClient()
	if err != nil {
		return nil, err
	}

	accountProperties := armnetapp.AccountProperties{}

	if activeDirectories != nil {
		accountProperties = armnetapp.AccountProperties{
			ActiveDirectories: activeDirectories,
		}
	}

	future, err := accountClient.BeginCreateOrUpdate(
		ctx,
		resourceGroupName,
		accountName,
		armnetapp.Account{
			Location:   to.Ptr(location),
			Tags:       tags,
			Properties: &accountProperties,
		},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create account: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &resp.Account, nil
}

// CreateANFCapacityPool creates an ANF Capacity Pool within ANF Account
func CreateANFCapacityPool(ctx context.Context, location, resourceGroupName, accountName, poolName, serviceLevel string, sizeBytes int64, tags map[string]*string) (*armnetapp.CapacityPool, error) {
	poolClient, err := getPoolsClient()
	if err != nil {
		return nil, err
	}

	svcLevel, err := validateANFServiceLevel(serviceLevel)
	if err != nil {
		return nil, err
	}

	future, err := poolClient.BeginCreateOrUpdate(
		ctx, resourceGroupName,
		accountName,
		poolName,
		armnetapp.CapacityPool{
			Location: to.Ptr(location),
			Tags:     tags,
			Properties: &armnetapp.PoolProperties{
				ServiceLevel: &svcLevel,
				Size:         to.Ptr[int64](sizeBytes),
			},
		},
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot create pool: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &resp.CapacityPool, nil
}

// CreateANFVolume creates an ANF volume within a Capacity Pool
func CreateANFVolume(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, serviceLevel, subnetID, snapshotID string, backupID string, protocolTypes []string, volumeUsageQuota int64, unixReadOnly, unixReadWrite bool, tags map[string]*string, dataProtectionObject armnetapp.VolumePropertiesDataProtection) (*armnetapp.Volume, error) {
	if len(protocolTypes) > 2 {
		return nil, fmt.Errorf("maximum of two protocol types are supported")
	}

	if len(protocolTypes) > 1 && utils.Contains(protocolTypes, "NFSv4.1") {
		return nil, fmt.Errorf("only cifs/nfsv3 protocol types are supported as dual protocol")
	}

	if snapshotID != "" && backupID != "" {
		return nil, fmt.Errorf("volume cannot be created from both snapshotID and backupID")
	}

	_, found := utils.FindInSlice(validProtocols, protocolTypes[0])
	if !found {
		return nil, fmt.Errorf("invalid protocol type, valid protocol types are: %v", validProtocols)
	}

	svcLevel, err := validateANFServiceLevel(serviceLevel)
	if err != nil {
		return nil, err
	}

	volumeClient, err := getVolumesClient()
	if err != nil {
		return nil, err
	}

	exportPolicy := armnetapp.VolumePropertiesExportPolicy{}

	if _, found := utils.FindInSlice(protocolTypes, cifs); !found {
		exportPolicy = armnetapp.VolumePropertiesExportPolicy{
			Rules: []*armnetapp.ExportPolicyRule{
				{
					AllowedClients: to.Ptr("0.0.0.0/0"),
					Cifs:           to.Ptr(map[bool]bool{true: true, false: false}[protocolTypes[0] == cifs]),
					Nfsv3:          to.Ptr(map[bool]bool{true: true, false: false}[protocolTypes[0] == nfsv3]),
					Nfsv41:         to.Ptr(map[bool]bool{true: true, false: false}[protocolTypes[0] == nfsv41]),
					RuleIndex:      to.Ptr[int32](1),
					UnixReadOnly:   to.Ptr(unixReadOnly),
					UnixReadWrite:  to.Ptr(unixReadWrite),
				},
			},
		}
	}

	protocolTypeSlice := make([]*string, len(protocolTypes))
	for i, protocolType := range protocolTypes {
		protocolTypeSlice[i] = &protocolType
	}

	volumeProperties := armnetapp.VolumeProperties{
		SnapshotID:     map[bool]*string{true: to.Ptr(snapshotID), false: nil}[snapshotID != ""],
		BackupID:       map[bool]*string{true: to.Ptr(backupID), false: nil}[backupID != ""],
		ExportPolicy:   map[bool]*armnetapp.VolumePropertiesExportPolicy{true: &exportPolicy, false: nil}[protocolTypes[0] != cifs],
		ProtocolTypes:  protocolTypeSlice,
		ServiceLevel:   &svcLevel,
		SubnetID:       to.Ptr(subnetID),
		UsageThreshold: to.Ptr[int64](volumeUsageQuota),
		CreationToken:  to.Ptr(volumeName),
		DataProtection: &dataProtectionObject,
	}

	future, err := volumeClient.BeginCreateOrUpdate(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		armnetapp.Volume{
			Location:   to.Ptr(location),
			Tags:       tags,
			Properties: &volumeProperties,
		},
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot create volume: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &resp.Volume, nil
}

// UpdateANFVolume update an ANF volume
func UpdateANFVolume(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName string, volumePropertiesPatch armnetapp.VolumePatchProperties, tags map[string]*string) (*armnetapp.Volume, error) {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return nil, err
	}

	volume, err := volumeClient.BeginUpdate(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		armnetapp.VolumePatch{
			Location:   to.Ptr(location),
			Tags:       tags,
			Properties: &volumePropertiesPatch,
		},
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot update volume: %v", err)
	}

	resp, err := volume.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &resp.Volume, nil
}

// AuthorizeReplication - authorizes volume replication
func AuthorizeReplication(ctx context.Context, resourceGroupName, accountName, poolName, volumeName, remoteVolumeResourceID string) error {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumeClient.BeginAuthorizeReplication(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		armnetapp.AuthorizeRequest{
			RemoteVolumeResourceID: to.Ptr(remoteVolumeResourceID),
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("cannot authorize volume replication: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// DeleteANFVolumeReplication - authorizes volume replication
func DeleteANFVolumeReplication(ctx context.Context, resourceGroupName, accountName, poolName, volumeName string) error {
	volumeClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumeClient.BeginDeleteReplication(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		nil,
	)
	if err != nil {
		return fmt.Errorf("cannot delete volume replication: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}

// CreateANFSnapshot creates a Snapshot from an ANF volume
func CreateANFSnapshot(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, snapshotName string, tags map[string]*string) (*armnetapp.Snapshot, error) {
	snapshotClient, err := getSnapshotsClient()
	if err != nil {
		return nil, err
	}

	future, err := snapshotClient.BeginCreate(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		snapshotName,
		armnetapp.Snapshot{
			Location: to.Ptr(location),
		},
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot create snapshot: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &resp.Snapshot, nil
}

// DeleteANFSnapshot deletes a Snapshot from an ANF volume
func DeleteANFSnapshot(ctx context.Context, resourceGroupName, accountName, poolName, volumeName, snapshotName string) error {
	snapshotClient, err := getSnapshotsClient()
	if err != nil {
		return err
	}

	future, err := snapshotClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		snapshotName,
		nil,
	)
	if err != nil {
		return fmt.Errorf("cannot delete snapshot: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot get the snapshot delete future response: %v", err)
	}

	return nil
}

// CreateANFSnapshotPolicy creates a Snapshot Policy to be used on volumes
func CreateANFSnapshotPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, policy armnetapp.SnapshotPolicy) (*armnetapp.SnapshotPolicy, error) {
	snapshotPolicyClient, err := getSnapshotPoliciesClient()
	if err != nil {
		return nil, err
	}

	snapshotPolicy, err := snapshotPolicyClient.Create(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		policy,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot create snapshot policy: %v", err)
	}

	return &snapshotPolicy.SnapshotPolicy, nil
}

// UpdateANFSnapshotPolicy update an ANF volume
func UpdateANFSnapshotPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, snapshotPolicyPatch armnetapp.SnapshotPolicyPatch) (*armnetapp.SnapshotPolicy, error) {
	snapshotPolicyClient, err := getSnapshotPoliciesClient()
	if err != nil {
		return nil, err
	}

	pollerResp, err := snapshotPolicyClient.BeginUpdate(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		snapshotPolicyPatch,
		nil,
	)
	if err != nil {
		return nil, err
	}

	snapshotPolicy, err := pollerResp.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot update snapshot policy: %v", err)
	}

	return &snapshotPolicy.SnapshotPolicy, nil
}

// CreateANFBackupPolicy creates a Snapshot Policy to be used on volumes
func CreateANFBackupPolicy(ctx context.Context, location, resourceGroupName, accountName, policyName string, policyProperties armnetapp.BackupPolicyProperties) (*armnetapp.BackupPolicy, error) {
	backupPolicyClient, err := getBackupPoliciesClient()
	if err != nil {
		return nil, err
	}

	backupPolicyBody := armnetapp.BackupPolicy{
		Location:   to.Ptr(location),
		Properties: &policyProperties,
	}

	future, err := backupPolicyClient.BeginCreate(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		backupPolicyBody,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot create backup policy: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot get the backup create or update future response: %v", err)
	}
	return &resp.BackupPolicy, nil
}

// CreateANFBackup creates an adhoc backup from volume
func CreateANFBackup(ctx context.Context, location, resourceGroupName, accountName, poolName, volumeName, backupName, backupLabel string) (*armnetapp.Backup, error) {
	backupsClient, err := getBackupsClient()
	if err != nil {
		return nil, err
	}

	backupBody := armnetapp.Backup{
		Location: to.Ptr(location),
		Properties: &armnetapp.BackupProperties{
			Label: to.Ptr(backupLabel),
		},
	}

	future, err := backupsClient.BeginCreate(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		backupName,
		backupBody,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create backup: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot get the backup create or update future response: %v", err)
	}

	return &resp.Backup, nil
}

// GetANFVault gets an netappAccount/vaults resource
func GetANFVaultList(ctx context.Context, resourceGroupName, accountName string) ([]*armnetapp.Vault, error) {
	vaultsClient, err := getVaultsClient()
	if err != nil {
		return nil, err
	}

	vaults := []*armnetapp.Vault{}
	pagerList := vaultsClient.NewListPager(
		resourceGroupName,
		accountName,
		nil,
	)
	for pagerList.More() {
		result, err := pagerList.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot retrieve list of vaults from account %v: %v", accountName, err)
		}
		vaults = append(vaults, result.Value...)
	}

	return vaults, nil
}

// UpdateANFBackupPolicy update an ANF Backup Policy
func UpdateANFBackupPolicy(ctx context.Context, resourceGroupName, accountName, policyName string, backupPolicyPatch armnetapp.BackupPolicyPatch) (*armnetapp.BackupPolicy, error) {
	backupPoliciesClient, err := getBackupPoliciesClient()
	if err != nil {
		return nil, err
	}

	future, err := backupPoliciesClient.BeginUpdate(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		backupPolicyPatch,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot update snapshot policy: %v", err)
	}

	resp, err := future.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot get the snapshot create or update future response: %v", err)
	}

	return &resp.BackupPolicy, nil
}

// DeleteANFVolume deletes a volume
func DeleteANFVolume(ctx context.Context, resourceGroupName, accountName, poolName, volumeName string) error {
	volumesClient, err := getVolumesClient()
	if err != nil {
		return err
	}

	future, err := volumesClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		volumeName,
		nil,
	)
	if err != nil {
		return fmt.Errorf("cannot delete volume: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
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

	future, err := poolsClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		poolName,
		nil,
	)

	if err != nil {
		return fmt.Errorf("cannot delete capacity pool: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
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

	future, err := snapshotPolicyClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		nil,
	)

	if err != nil {
		return fmt.Errorf("cannot delete snapshot policy: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
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

	future, err := backupPolicyClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		policyName,
		nil,
	)

	if err != nil {
		return fmt.Errorf("cannot delete backup policy: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
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
			nil,
		)

		if err != nil {
			return err
		}
		if volume.Properties.DataProtection.Backup == nil {
			return nil
		}
	}

	// Geting backup client depending on which level
	// we are targeting (volume or account level)
	backupsClient := &armnetapp.BackupsClient{}
	accountBackupsClient := &armnetapp.AccountBackupsClient{}

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

	backups := make([]*armnetapp.Backup, 0)

	if uri.IsANFVolume(parentResourceID) {
		pager := backupsClient.NewListPager(
			uri.GetResourceGroup(parentResourceID),
			uri.GetANFAccount(parentResourceID),
			uri.GetANFCapacityPool(parentResourceID),
			uri.GetANFVolume(parentResourceID),
			nil,
		)
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return err
			}
			backups = append(backups, resp.BackupsList.Value...)
		}
	} else {
		pager := accountBackupsClient.NewListPager(
			uri.GetResourceGroup(parentResourceID),
			uri.GetANFAccount(parentResourceID),
			nil,
		)
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return err
			}
			backups = append(backups, resp.BackupsList.Value...)
		}
	}

	// Sorting backups by date ascending.
	// Backups needs to be deleted from newest to oldest.
	sort.SliceStable(backups, func(i, j int) bool {
		return backups[i].Properties.CreationDate.Before(*backups[j].Properties.CreationDate)
	})

	utils.ConsoleOutput("\tBackup deletion order:")
	for i, backup := range backups {
		message := fmt.Sprintf("\t\t%v - BackupName: %v, CreationDate: %v", i, *backup.Name, backup.Properties.CreationDate)

		if i == len(backups)-1 {
			if !deleteLastBackup {
				message = fmt.Sprintf("\t\t%v - BackupName: %v, CreationDate: %v (deferred to account level removal)", i, *backup.Name, backup.Properties.CreationDate)
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

		if uri.IsANFVolume(parentResourceID) {
			futureBackups, err := backupsClient.BeginDelete(
				ctx,
				uri.GetResourceGroup(*backup.ID),
				uri.GetANFAccount(*backup.ID),
				uri.GetANFCapacityPool(*backup.ID),
				uri.GetANFVolume(*backup.ID),
				uri.GetANFBackup(*backup.ID),
				nil,
			)
			if err != nil {
				return fmt.Errorf("cannot delete backup: %v", err)
			}
			_, err = futureBackups.PollUntilDone(ctx, nil)
			if err != nil {
				return fmt.Errorf("cannot get the backup delete future response: %v", err)
			}
		} else {
			// Making sure we're deleting only backups tied to this account since
			// all backups for a region can be shown in any account for the same
			// region
			if strings.EqualFold(strings.ToLower(uri.GetANFAccount(*backup.ID)), strings.ToLower(uri.GetANFAccount(parentResourceID))) {
				futureAccountBackups, err := accountBackupsClient.BeginDelete(
					ctx,
					uri.GetResourceGroup(*backup.ID),
					uri.GetANFAccount(*backup.ID),
					uri.GetANFAccountBackup(*backup.ID),
					nil,
				)
				if err != nil {
					return fmt.Errorf("cannot delete backup: %v", err)
				}
				_, err = futureAccountBackups.PollUntilDone(ctx, nil)
				if err != nil {
					return fmt.Errorf("cannot get the backup delete future response: %v", err)
				}
			} else {
				continue
			}
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

	future, err := accountsClient.BeginDelete(
		ctx,
		resourceGroupName,
		accountName,
		nil,
	)
	if err != nil {
		return fmt.Errorf("cannot delete account: %v", err)
	}

	_, err = future.PollUntilDone(ctx, nil)
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
				nil,
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
				nil,
			)
		} else if uri.IsANFAccountBackup(resourceID) {
			resourceIdIdentified = true
			client, _ := getAccountBackupsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFAccountBackup(resourceID),
				nil,
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
					nil,
				)
			} else {
				resourceIdIdentified = true
				_, err = client.ReplicationStatus(
					ctx,
					uri.GetResourceGroup(resourceID),
					uri.GetANFAccount(resourceID),
					uri.GetANFCapacityPool(resourceID),
					uri.GetANFVolume(resourceID),
					nil,
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
				nil,
			)
		} else if uri.IsANFSnapshotPolicy(resourceID) {
			resourceIdIdentified = true
			client, _ := getSnapshotPoliciesClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFSnapshotPolicy(resourceID),
				nil,
			)
		} else if uri.IsANFBackupPolicy(resourceID) {
			resourceIdIdentified = true
			client, _ := getBackupPoliciesClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				uri.GetANFBackupPolicy(resourceID),
				nil,
			)
		} else if uri.IsANFAccount(resourceID) {
			resourceIdIdentified = true
			client, _ := getAccountsClient()
			_, err = client.Get(
				ctx,
				uri.GetResourceGroup(resourceID),
				uri.GetANFAccount(resourceID),
				nil,
			)
		}

		if !resourceIdIdentified {
			return fmt.Errorf("resource id %v is not properly handled by WaitForANFResource function", resourceID)
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

	for i := 0; i < retries; i++ {
		time.Sleep(time.Duration(intervalInSec) * time.Second)

		backup, _ := backupsClient.Get(
			ctx,
			uri.GetResourceGroup(backupID),
			uri.GetANFAccount(backupID),
			uri.GetANFCapacityPool(backupID),
			uri.GetANFVolume(backupID),
			uri.GetANFBackup(backupID),
			nil,
		)

		// In this case, we exit when provisioning state is Succeeded
		if *backup.Properties.ProvisioningState == "Succeeded" {
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

	backupsClient := &armnetapp.BackupsClient{}
	accountBackupsClient := &armnetapp.AccountBackupsClient{}

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

		backupList := []*armnetapp.Backup{}

		if uri.IsANFVolume(parentResourceID) {
			pagerList := backupsClient.NewListPager(
				uri.GetResourceGroup(parentResourceID),
				uri.GetANFAccount(parentResourceID),
				uri.GetANFCapacityPool(parentResourceID),
				uri.GetANFVolume(parentResourceID),
				nil,
			)
			for pagerList.More() {
				resp, _ := pagerList.NextPage(ctx)
				backupList = append(backupList, resp.Value...)
			}
		} else {
			pagerList := accountBackupsClient.NewListPager(
				uri.GetResourceGroup(parentResourceID),
				uri.GetANFAccount(parentResourceID),
				nil,
			)
			for pagerList.More() {
				resp, _ := pagerList.NextPage(ctx)
				backupList = append(backupList, resp.Value...)
			}
		}

		// Exiting if no backups are defined since this will not generate
		// the snapmirror initial snapshot used by backups
		if len(backupList) == 0 {
			return nil
		}

		for _, item := range backupList {
			if strings.Contains(*item.Name, "snapmirror") {
				return nil
			}
		}
	}

	return fmt.Errorf("wait exceed # of attempts to get snapmirror, number of retries %v and interval in seconds %v", retries, intervalInSec)
}
