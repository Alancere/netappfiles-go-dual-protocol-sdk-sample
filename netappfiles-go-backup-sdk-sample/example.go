// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// This sample code showcases how to create and use ANF Snapshot policies.
// For this to happen this code also creates Account, Capacity Pool, and
// Volumes.
// Clean up process (not enabled by default) is made in reverse order,
// this operation is not taking place if there is an execution failure,
// you will need to clean it up manually in this case.

// This package uses go-haikunator package (https://github.com/yelinaung/go-haikunator)
// port from Python's haikunator module and therefore used here just for sample simplification,
// this doesn't mean that it is endorsed/thouroughly tested by any means, use at own risk.
// Feel free to provide your own names on variables using it.

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Azure-Samples/netappfiles-go-backup-sdk-sample/netappfiles-go-backup-sdk-sample/internal/sdkutils"
	"github.com/Azure-Samples/netappfiles-go-backup-sdk-sample/netappfiles-go-backup-sdk-sample/internal/utils"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/netapp/mgmt/netapp"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/yelinaung/go-haikunator"
)

const (
	virtualNetworksAPIVersion string = "2019-09-01"
)

var (
	shouldCleanUp bool = false

	// Important - change ANF related variables below to appropriate values related to your environment
	// Share ANF properties related
	capacityPoolSizeBytes int64 = 4398046511104 // 4TiB (minimum capacity pool size)
	volumeSizeBytes       int64 = 107374182400  // 100GiB (minimum volume size)
	protocolTypes               = []string{"NFSv3"}
	sampleTags                  = map[string]*string{
		"Author":  to.StringPtr("ANF Go Snapshot Policy SDK Sample"),
		"Service": to.StringPtr("Azure Netapp Files"),
	}

	// ANF Resource Properties
	location              = "eastus"
	resourceGroupName     = "anf01-rg"
	vnetresourceGroupName = "anf01-rg"
	vnetName              = "vnet-01"
	subnetName            = "anf-sn"
	anfAccountName        = haikunator.New(time.Now().UTC().UnixNano()).Haikunate()
	snapshotPolicyName    = "snapshotpolicy01"
	backupPolicyName      = "backuppolicy01"
	capacityPoolName      = "Pool01"
	serviceLevel          = "Standard"
	volumeName            = fmt.Sprintf("NFSv3-Vol-%v-%v", anfAccountName, capacityPoolName)

	// Some other variables used throughout the course of the code execution - no need to change it
	exitCode         int
	volumeID         string
	capacityPoolID   string
	accountID        string
	snapshotPolicyID string
	backupPolicyID   string
)

func main() {

	cntx := context.Background()

	// Cleanup and exit handling
	defer func() { exit(cntx); os.Exit(exitCode) }()

	utils.PrintHeader("Azure NetAppFiles Go Snapshot Policy SDK Sample - Sample application that enables Snaphost Policy on an NFSv3 volume.")

	// Getting subscription ID from authentication file
	config, err := utils.ReadAzureBasicInfoJSON(os.Getenv("AZURE_AUTH_LOCATION"))
	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred getting non-sensitive info from AzureAuthFile: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}

	// Checking if subnet exists before any other operation starts
	subnetID := fmt.Sprintf("/subscriptions/%v/resourceGroups/%v/providers/Microsoft.Network/virtualNetworks/%v/subnets/%v",
		*config.SubscriptionID,
		vnetresourceGroupName,
		vnetName,
		subnetName,
	)

	utils.ConsoleOutput(fmt.Sprintf("Checking if vnet/subnet %v exists.", subnetID))

	_, err = sdkutils.GetResourceByID(cntx, subnetID, virtualNetworksAPIVersion)
	if err != nil {
		if string(err.Error()) == "NotFound" {
			utils.ConsoleOutput(fmt.Sprintf("error: subnet %v not found: %v", subnetID, err))
		} else {
			utils.ConsoleOutput(fmt.Sprintf("error: an error ocurred trying to check if %v subnet exists: %v", subnetID, err))
		}
		exitCode = 1
		shouldCleanUp = false
		return
	}

	//------------------
	// Account creation
	//------------------
	utils.ConsoleOutput(fmt.Sprintf("Creating Azure NetApp Files account %v...", anfAccountName))

	account, err := sdkutils.CreateANFAccount(cntx, location, resourceGroupName, anfAccountName, nil, sampleTags)
	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while creating account: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}
	accountID = *account.ID
	utils.ConsoleOutput(fmt.Sprintf("Account successfully created, resource id: %v", accountID))

	//-----------------------
	// Capacity pool creation
	//-----------------------
	utils.ConsoleOutput(fmt.Sprintf("Creating Capacity Pool %v...", capacityPoolName))
	capacityPool, err := sdkutils.CreateANFCapacityPool(
		cntx,
		location,
		resourceGroupName,
		anfAccountName,
		capacityPoolName,
		serviceLevel,
		capacityPoolSizeBytes,
		sampleTags,
	)
	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while creating capacity pool: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}
	capacityPoolID = *capacityPool.ID
	utils.ConsoleOutput(fmt.Sprintf("Capacity Pool successfully created, resource id: %v", capacityPoolID))

	//-------------------------
	// Snapshot policy creation
	//-------------------------

	// Creating Snapshot Policy - using arbitrary values
	utils.ConsoleOutput(fmt.Sprintf("Creating Snapshot Policy %v...", snapshotPolicyName))

	// Everyday at 22:00
	dailySchedule := netapp.DailySchedule{
		Hour:            to.Int32Ptr(22),
		Minute:          to.Int32Ptr(0),
		SnapshotsToKeep: to.Int32Ptr(5),
	}

	// Everyweek on Friday at 23:00
	weeklySchedule := netapp.WeeklySchedule{
		Day:             to.StringPtr("Friday"),
		Hour:            to.Int32Ptr(23),
		Minute:          to.Int32Ptr(0),
		SnapshotsToKeep: to.Int32Ptr(5),
	}

	// Monthly on specific days (01, 15 and 25) at 08:00 AM
	monthlySchedule := netapp.MonthlySchedule{
		DaysOfMonth:     to.StringPtr("1,15,25"),
		Hour:            to.Int32Ptr(8),
		Minute:          to.Int32Ptr(0),
		SnapshotsToKeep: to.Int32Ptr(5),
	}

	// Policy body, putting everything together
	snapshotPolicyBody := netapp.SnapshotPolicy{
		Location: to.StringPtr(location),
		Name:     to.StringPtr(snapshotPolicyName),
		SnapshotPolicyProperties: &netapp.SnapshotPolicyProperties{
			DailySchedule:   &dailySchedule,
			WeeklySchedule:  &weeklySchedule,
			MonthlySchedule: &monthlySchedule,
			Enabled:         to.BoolPtr(true),
		},
		Tags: sampleTags,
	}

	// Create the snapshot policy resource
	snapshotPolicy, err := sdkutils.CreateANFSnapshotPolicy(
		cntx,
		resourceGroupName,
		anfAccountName,
		snapshotPolicyName,
		snapshotPolicyBody,
	)

	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while creating snapshot policy: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}

	snapshotPolicyID = *snapshotPolicy.ID
	utils.ConsoleOutput(fmt.Sprintf("Snapshot Policy successfully created, resource id: %v", snapshotPolicyID))

	//-------------------------
	// Backup policy creation
	//-------------------------

	// Creating Backup Policy - using arbitrary values
	// Note that Backup policy depends on snapshot policies and it cannot be used with hourly snapshots
	utils.ConsoleOutput(fmt.Sprintf("Creating Backup Policy %v...", backupPolicyName))

	backupPolicyProperties := netapp.BackupPolicyProperties{
		DailyBackupsToKeep:   to.Int32Ptr(10),
		WeeklyBackupsToKeep:  to.Int32Ptr(10),
		MonthlyBackupsToKeep: to.Int32Ptr(10),
	}

	backupPolicy, err := sdkutils.CreateANFBackupPolicy(
		cntx,
		resourceGroupName,
		anfAccountName,
		backupPolicyName,
		backupPolicyProperties,
	)

	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while creating backup policy: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}

	backupPolicyID = *backupPolicy.ID
	utils.ConsoleOutput(fmt.Sprintf("Backup Policy successfully created, resource id: %v", backupPolicyID))

	//----------------
	// Volume creation
	//----------------
	utils.ConsoleOutput(fmt.Sprintf("Creating NFSv3 Volume %v with Snapshot Policy %v attached...", volumeName, snapshotPolicyName))

	// Build data protection object with snapshot and backup properties
	dataProtectionObject := netapp.VolumePropertiesDataProtection{
		Snapshot: &netapp.VolumeSnapshotProperties{
			SnapshotPolicyID: to.StringPtr(snapshotPolicyID),
		},

		Backup: &netapp.VolumeBackupProperties{
			BackupPolicyID: to.StringPtr(backupPolicyID),
		},
	}

	volume, err := sdkutils.CreateANFVolume(
		cntx,
		location,
		resourceGroupName,
		anfAccountName,
		capacityPoolName,
		volumeName,
		serviceLevel,
		subnetID,
		"",
		protocolTypes,
		volumeSizeBytes,
		false,
		true,
		sampleTags,
		dataProtectionObject,
	)

	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while creating volume: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}

	volumeID = *volume.ID
	utils.ConsoleOutput(fmt.Sprintf("Volume successfully created, resource id: %v", volumeID))

	utils.ConsoleOutput("Waiting for volume to be ready...")
	err = sdkutils.WaitForANFResource(cntx, volumeID, 60, 50, false)
	if err != nil {
		utils.ConsoleOutput(fmt.Sprintf("an error ocurred while waiting for volume: %v", err))
		exitCode = 1
		shouldCleanUp = false
		return
	}

	// //------------------------
	// // Snapshot Policy updates
	// //------------------------
	// utils.ConsoleOutput(fmt.Sprintf("Updating snapshot policy %v...", snapshotPolicyName))

	// // Updating number of snapshots to keep for hourly schedule
	// newHourlySchedule := *snapshotPolicy.SnapshotPolicyProperties.HourlySchedule
	// newHourlySchedule.SnapshotsToKeep = to.Int32Ptr(10)

	// // Creating a patch object
	// snapshotPolicyPatch := netapp.SnapshotPolicyPatch{
	// 	Location: to.StringPtr(location),
	// 	SnapshotPolicyProperties: &netapp.SnapshotPolicyProperties{
	// 		HourlySchedule: &newHourlySchedule,
	// 	},
	// }

	// // Executing the update
	// _, err = sdkutils.UpdateANFSnapshotPolicy(
	// 	cntx,
	// 	resourceGroupName,
	// 	anfAccountName,
	// 	snapshotPolicyName,
	// 	snapshotPolicyPatch,
	// )

	// if err != nil {
	// 	utils.ConsoleOutput(fmt.Sprintf("an error ocurred while updating snapshot policy: %v", err))
	// 	exitCode = 1
	// 	shouldCleanUp = false
	// 	return
	// }

	// utils.ConsoleOutput("Wait a few seconds for snapshot policy to complete update operation before deleting resources...")
	// time.Sleep(time.Duration(5) * time.Second)
}

func exit(cntx context.Context) {
	utils.ConsoleOutput("Exiting")

	// In order to enable clean up, change the shouldCleanUp variable in the var() section
	// to true. Notice that if there is an error while executing the main parts of this
	// code, clean up will need to be done manually.
	// Since resource deletions cannot happen if there is a child resource, we will perform the
	// clean up in the following order: Volume -> Capacity Pool -> Snapshot Policy -> Account
	if shouldCleanUp {
		utils.ConsoleOutput("\tPerforming clean up")

		resourceGroupName := resourceGroupName
		accountName := anfAccountName
		poolName := capacityPoolName
		volumeName := volumeName

		// Volume deletion
		utils.ConsoleOutput(fmt.Sprintf("\tRemoving %v volume...", volumeID))
		err := sdkutils.DeleteANFVolume(
			cntx,
			resourceGroupName,
			accountName,
			poolName,
			volumeName,
		)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while deleting volume: %v", err))
			exitCode = 1
			return
		}
		err = sdkutils.WaitForNoANFResource(cntx, volumeID, 60, 50, false)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while waiting for volume complete deletion: %v", err))
			exitCode = 1
			shouldCleanUp = false
			return
		}
		utils.ConsoleOutput("\tVolume successfully deleted")

		// Pool Cleanup
		utils.ConsoleOutput(fmt.Sprintf("\tCleaning up capacity pool %v...", capacityPoolID))
		err = sdkutils.DeleteANFCapacityPool(
			cntx,
			resourceGroupName,
			accountName,
			poolName,
		)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while deleting capacity pool: %v", err))
			exitCode = 1
			return
		}
		err = sdkutils.WaitForNoANFResource(cntx, capacityPoolID, 60, 50, false)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while waiting for capacity complete deletion: %v", err))
			exitCode = 1
			shouldCleanUp = false
			return
		}
		utils.ConsoleOutput("\tCapacity pool successfully deleted")

		// Snapshot Policy Cleanup
		utils.ConsoleOutput(fmt.Sprintf("\tCleaning up snapshot policy %v...", snapshotPolicyID))
		err = sdkutils.DeleteANFSnapshotPolicy(
			cntx,
			resourceGroupName,
			accountName,
			snapshotPolicyName,
		)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while deleting snapshot policy: %v", err))
			exitCode = 1
			return
		}
		err = sdkutils.WaitForNoANFResource(cntx, snapshotPolicyID, 60, 50, false)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while waiting for snapshot policy complete deletion: %v", err))
			exitCode = 1
			shouldCleanUp = false
			return
		}
		utils.ConsoleOutput("\tSnapshot policy successfully deleted")

		// Account Cleanup
		utils.ConsoleOutput(fmt.Sprintf("\tCleaning up account %v...", accountID))
		err = sdkutils.DeleteANFAccount(
			cntx,
			resourceGroupName,
			accountName,
		)
		if err != nil {
			utils.ConsoleOutput(fmt.Sprintf("an error ocurred while deleting account: %v", err))
			exitCode = 1
			return
		}
		utils.ConsoleOutput("\tAccount successfully deleted")
		utils.ConsoleOutput("\tCleanup completed!")
	}
}
