//
//  File.swift
//  Blackbird
//
//  Created by Hannes Nagel on 4/20/25.
//

import Foundation
import CloudKit

// Helper extension to convert between Blackbird.Row and CKRecord
extension CKRecord {
    internal func toBlackbirdRow() -> Blackbird.Row {
        var row: Blackbird.Row = [:]
        //print("DEBUG: Converting CKRecord to Blackbird.Row, keys: \(self.allKeys().joined(separator: ", "))")
        
        for key in self.allKeys() {
            let value = self[key]
            //print("DEBUG: Converting key \(key) with value of type: \(type(of: value))")
            
            if let stringValue = value as? String {
                row[key] = .text(stringValue)
                //print("DEBUG: Converted \(key) to .text: \(stringValue)")
            } else if let intValue = value as? Int64 {
                row[key] = .integer(intValue)
                //print("DEBUG: Converted \(key) to .integer: \(intValue)")
            } else if let doubleValue = value as? Double {
                row[key] = .double(doubleValue)
                //print("DEBUG: Converted \(key) to .double: \(doubleValue)")
            } else if let dataValue = value as? Data {
                row[key] = .data(dataValue)
                //print("DEBUG: Converted \(key) to .data (size: \(dataValue.count) bytes)")
            } else if value == nil {
                row[key] = .null
                //print("DEBUG: Converted \(key) to .null")
            }
            // Handle more specific CloudKit types
            else if let dateValue = value as? Date {
                // Store dates as ISO8601 string for better compatibility
                let formatter = ISO8601DateFormatter()
                formatter.formatOptions = [.withInternetDateTime]
                let dateString = formatter.string(from: dateValue)
                row[key] = .text(dateString)
                //print("DEBUG: Converted \(key) (Date) to .text: \(dateString)")
            } else if let assetValue = value as? CKAsset, let fileURL = assetValue.fileURL {
                do {
                    let data = try Data(contentsOf: fileURL)
                    row[key] = .data(data)
                    //print("DEBUG: Converted \(key) (CKAsset) to .data (size: \(data.count) bytes)")
                } catch {
                    print("ERROR: Error converting CKAsset to Data: \(error)")
                    row[key] = .null
                    //print("DEBUG: Failed to convert CKAsset, using .null instead")
                }
            } else {
                //print("DEBUG: Unknown value type for key \(key): \(type(of: value)), not converted")
            }
        }
        
        //print("DEBUG: Final Blackbird.Row has \(row.count) keys: \(row.keys.joined(separator: ", "))")
        return row
    }
}

extension Blackbird.Row {
    internal func updateCKRecord(_ record: CKRecord) {
        //print("DEBUG: Updating CKRecord from Blackbird.Row, row keys: \(self.keys.joined(separator: ", "))")
        //print("DEBUG: CKRecord before update, keys: \(record.allKeys().joined(separator: ", "))")
        
        for (key, value) in self {
            if key == "id" || key == "_sync_status" {
                // Skip internal columns that shouldn't be synced to CloudKit
                //print("DEBUG: Skipping internal key: \(key)")
                continue
            }
            
            //print("DEBUG: Processing key \(key) with value type: \(type(of: value))")
            
            switch value {
            case .null:
                record[key] = nil
                //print("DEBUG: Set \(key) to nil in CKRecord")
            case .integer(let intValue):
                record[key] = intValue
                //print("DEBUG: Set \(key) to integer: \(intValue) in CKRecord")
            case .double(let doubleValue):
                record[key] = doubleValue
                //print("DEBUG: Set \(key) to double: \(doubleValue) in CKRecord")
            case .text(let textValue):
                // Check if this might be a stored Date value
                if let date = Self.parseDate(textValue) {
                    record[key] = date
                    //print("DEBUG: Set \(key) to Date: \(date) in CKRecord (converted from ISO string)")
                } else {
                    record[key] = textValue
                    //print("DEBUG: Set \(key) to text: \(textValue) in CKRecord")
                }
            case .data(let dataValue):
            //storing all data as CKAsset
                do {
                    let tempDir = FileManager.default.temporaryDirectory
                    let fileName = UUID().uuidString
                    let fileURL = tempDir.appendingPathComponent(fileName)
                    try dataValue.write(to: fileURL)
                    let asset = CKAsset(fileURL: fileURL)
                    record[key] = asset
                    //print("DEBUG: Set \(key) to CKAsset (large data: \(dataValue.count) bytes) in CKRecord")
                } catch {
                    print("ERROR: Error creating CKAsset: \(error)")
                    record[key] = nil
                    //print("DEBUG: Failed to create CKAsset for \(key), set to nil instead")
                }
            }
        }
        
        //print("DEBUG: CKRecord after update, keys: \(record.allKeys().joined(separator: ", "))")
    }
    
    // Helper to parse ISO8601 date strings
    private static func parseDate(_ string: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: string)
    }
}

extension Blackbird.Database: CKSyncEngineDelegate {
    // MARK: - CKSyncEngineDelegate Implementation
    
    public func handleEvent(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
        //print("DEBUG: Handling CloudKit sync event: \(String(describing: type(of: event)))")
        
        switch event {
        case .stateUpdate(let stateUpdate):
            //print("DEBUG: State update received")
            stateSerialization.value = stateUpdate.stateSerialization
            // Save state to disk for persistent databases
            saveStateSerialization()
        case .accountChange(let accountChange):
            //print("DEBUG: CloudKit account change: \(accountChange.changeType)")
            // Handle account change if needed (e.g., user signed out)
            // Check if there's no account or user is signed out
            if changeRequiresStopSync(accountChange.changeType) {
                // User signed out - consider stopping sync or clearing local database
                //print("DEBUG: CloudKit account no longer available - stop syncing?")
//                stopCloudKitSync()
            }
        case .fetchedDatabaseChanges(let fetchedDatabaseChanges):
            //print("DEBUG: Fetched database changes with \(fetchedDatabaseChanges.deletions.count) deletions")
            for deletion in fetchedDatabaseChanges.deletions {
                let schemeToDelete = deletion.zoneID.zoneName
                //print("DEBUG: Deleting table for zone: \(schemeToDelete)")
                do {
                    try await execute("DROP TABLE IF EXISTS `\(schemeToDelete)`")
                    //print("DEBUG: Deleted table \(schemeToDelete) after CloudKit zone deletion")
                } catch {
                    print("ERROR: Error deleting table \(schemeToDelete): \(error.localizedDescription)")
                }
            }
        case .fetchedRecordZoneChanges(let fetchedRecordZoneChanges):
            //print("DEBUG: Fetched record zone changes: \(fetchedRecordZoneChanges.modifications.count) modifications, \(fetchedRecordZoneChanges.deletions.count) deletions")
            await processFetchedRecordZoneChanges(fetchedRecordZoneChanges)
        case .sentDatabaseChanges(_):
            //print("DEBUG: Database changes synced to CloudKit")
            return
        case .sentRecordZoneChanges(let sentRecordZoneChanges):
            //print("DEBUG: Record changes sent to CloudKit: \(sentRecordZoneChanges.savedRecords.count) saved records, \(sentRecordZoneChanges.deletedRecordIDs.count) deleted records")
            await processSuccessfullyUploadedRecords(sentRecordZoneChanges)
        case .willFetchChanges(_):
            //print("DEBUG: About to fetch changes from CloudKit")
            return
        case .willFetchRecordZoneChanges(_):
            //print("DEBUG: About to fetch record zone changes for zones")
            return
        case .didFetchRecordZoneChanges(_):
            //print("DEBUG: Finished fetching record zone changes for zones")
            return
        case .didFetchChanges(_):
            //print("DEBUG: Finished fetching all changes from CloudKit")
            return
        case .willSendChanges(_):
            //print("DEBUG: About to send local changes to CloudKit")
            return
        case .didSendChanges(_):
            //print("DEBUG: Finished sending local changes to CloudKit")
            return
        @unknown default:
            //print("DEBUG: Unknown CloudKit sync event")
            return
        }
    }
    
    // Helper function to check if an account change should stop sync
    private func changeRequiresStopSync(_ changeType: CKSyncEngine.Event.AccountChange.ChangeType) -> Bool {
        // This function exists to handle different ways account changes might be reported
        // Different iOS versions may have different case names
        String(describing: changeType).contains("no") || 
        String(describing: changeType).contains("unavailable") ||
        String(describing: changeType).contains("deleted")
    }
    
    private func processFetchedRecordZoneChanges(_ changes: CKSyncEngine.Event.FetchedRecordZoneChanges) async {
        //print("DEBUG: Processing fetched changes: \(changes.modifications.count) modifications, \(changes.deletions.count) deletions")
        
        do {
            try await transaction { core in
                // Process modifications
                for modification in changes.modifications {
                    let tableName = modification.record.recordType
                    let recordID = modification.record.recordID.recordName
                    
                    // Skip internal tables that shouldn't be synced
                    if tableName == "_cloudkit_deletions" {
                        //print("DEBUG: Skipping processing record for internal table: \(tableName)")
                        continue
                    }
                    
                    // Ensure _sync_status column exists
                    try await ensureSyncStatusColumnExists(for: tableName)
                    
                    //print("DEBUG: Processing record: \(recordID) in table: \(tableName), changedKeys: \(modification.record.changedKeys())")
                    
                    // Convert the CKRecord to a Blackbird.Row
                    let row = modification.record.toBlackbirdRow()
                    //print("DEBUG: Converted to row with keys: \(row.keys.joined(separator: ", "))")
                    
                    // Check if the table exists and create it if necessary
                    try await self.createTableIfNeeded(for: tableName, record: modification.record)
                    
                    // Check if the record exists
                    let existingRows = try await query("SELECT id FROM `\(tableName)` WHERE id = ?", recordID)
                    //print("DEBUG: Record exists in database: \(existingRows.isEmpty ? "NO" : "YES")")
                    
                    if existingRows.isEmpty {
                        // Record doesn't exist - INSERT
                        var columns = ["id"] + Array(row.keys)
                        columns = Array(Set(columns)) // Remove duplicates if any
                        
                        let placeholders = Array(repeating: "?", count: columns.count).joined(separator: ", ")
                        var values: [Sendable] = [recordID]
                        
                        for column in columns.dropFirst() { // Skip 'id' as we already added it
                            if let value = row[column] {
                                // Convert Blackbird.Value to a type that SQLite can handle
                                switch value {
                                case .integer(let intVal):
                                    //print("DEBUG: Adding column \(column) with INTEGER value: \(intVal)")
                                    values.append(intVal)
                                case .double(let doubleVal):
                                    //print("DEBUG: Adding column \(column) with DOUBLE value: \(doubleVal)")
                                    values.append(doubleVal)
                                case .text(let textVal):
                                    //print("DEBUG: Adding column \(column) with TEXT value: \(textVal)")
                                    values.append(textVal)
                                case .data(let dataVal):
                                    //print("DEBUG: Adding column \(column) with DATA value (size: \(dataVal.count))")
                                    values.append(dataVal)
                                case .null:
                                    //print("DEBUG: Adding column \(column) with NULL value")
                                    values.append(NSNull())
                                }
                            }
                        }
                        
                        let columnsStr = columns.map { "`\($0)`" }.joined(separator: ", ")
                        let query = "INSERT INTO `\(tableName)` (\(columnsStr)) VALUES (\(placeholders))"
                        //print("DEBUG: Executing query: \(query)")
                        
                        do {
                            try await self.query(query, arguments: values)
                            //print("DEBUG: Inserted record \(recordID) in table \(tableName)")
                        } catch {
                            print("ERROR: Error inserting record: \(error.localizedDescription)")
                            print("ERROR: Query was: \(query)")
                            print("ERROR: Values were: \(values)")
                            print("ERROR: Attempting fallback insert...")
                            
                            // Try a fallback approach - insert with minimal fields
                            try await self.query("INSERT INTO `\(tableName)` (id, _sync_status) VALUES (?, 0)", recordID)
                            //print("DEBUG: Inserted record with minimal fields")
                            
                            // Now try updating each field separately
                            for (column, value) in row {
                                do {
                                    let updateQuery = "UPDATE `\(tableName)` SET `\(column)` = ? WHERE id = ?"
                                    
                                    switch value {
                                    case .integer(let intVal):
                                        try await self.query(updateQuery, intVal, recordID)
                                    case .double(let doubleVal):
                                        try await self.query(updateQuery, doubleVal, recordID)
                                    case .text(let textVal):
                                        try await self.query(updateQuery, textVal, recordID)
                                    case .data(let dataVal):
                                        try await self.query(updateQuery, dataVal, recordID)
                                    case .null:
                                        try await self.query(updateQuery, NSNull(), recordID)
                                    }
                                    //print("DEBUG: Updated field \(column) separately")
                                } catch {
                                    print("WARNING: Could not update field \(column): \(error.localizedDescription)")
                                }
                            }
                        }
                    } else {
                        // Record exists - UPDATE
                        // Use either the changed keys or all keys if changedKeys is empty
                        var keysToUpdate = modification.record.changedKeys()
                        if keysToUpdate.isEmpty {
                            // No changed keys specified - check all keys from the record
                            //print("DEBUG: No changed keys specified, checking all fields for changes")
                            keysToUpdate = modification.record.allKeys()
                        }
                        
                        //print("DEBUG: Keys to check for update: \(keysToUpdate.joined(separator: ", "))")
                        
                        if !keysToUpdate.isEmpty {
                            // Get current values from database for comparison
                            let currentValues = try await query("SELECT * FROM `\(tableName)` WHERE id = ?", recordID).first
                            
                            var actualChangedKeys: [String] = []
                            var parameters: [Sendable] = []
                            
                            for key in keysToUpdate {
                                if key == "id" || key == "_sync_status" { continue } // Skip special fields
                                
                                if let newValue = row[key], let currentRow = currentValues {
                                    let currentValue = currentRow[key]
                                    //print("DEBUG: Comparing key \(key): cloudKit=\(newValue), local=\(String(describing: currentValue))")
                                    
                                    // Check if values are different (indicating a needed update)
                                    var needsUpdate = true
                                    
                                    if let currentVal = currentValue {
                                        switch (newValue, currentVal) {
                                        case (.integer(let newInt), .integer(let currentInt)) where newInt == currentInt:
                                            needsUpdate = false
                                        case (.double(let newDouble), .double(let currentDouble)) where newDouble == currentDouble:
                                            needsUpdate = false
                                        case (.text(let newText), .text(let currentText)) where newText == currentText:
                                            needsUpdate = false
                                        case (.data(let newData), .data(let currentData)) where newData == currentData:
                                            needsUpdate = false
                                        case (.null, .null):
                                            needsUpdate = false
                                        default:
                                            // Values are different types or values
                                            needsUpdate = true
                                        }
                                    }
                                    
                                    if needsUpdate {
                                        //print("DEBUG: Key \(key) needs to be updated")
                                        actualChangedKeys.append(key)
                                        
                                        // Add parameter for SQL UPDATE
                                        switch newValue {
                                        case .integer(let intVal):
                                            //print("DEBUG: Update key \(key) with INTEGER value: \(intVal)")
                                            parameters.append(intVal)
                                        case .double(let doubleVal):
                                            //print("DEBUG: Update key \(key) with DOUBLE value: \(doubleVal)")
                                            parameters.append(doubleVal)
                                        case .text(let textVal):
                                            //print("DEBUG: Update key \(key) with TEXT value: \(textVal)")
                                            parameters.append(textVal)
                                        case .data(let dataVal):
                                            //print("DEBUG: Update key \(key) with DATA value (size: \(dataVal.count))")
                                            parameters.append(dataVal)
                                        case .null:
                                            //print("DEBUG: Update key \(key) with NULL value")
                                            parameters.append(NSNull())
                                        }
                                    } else {
                                        //print("DEBUG: Key \(key) has not changed, skipping")
                                    }
                                } else if let newValue = row[key] {
                                    // No current value, but we have a new one to set
                                    //print("DEBUG: Key \(key) is new or was null, updating")
                                    actualChangedKeys.append(key)
                                    
                                    // Add parameter for SQL UPDATE
                                    switch newValue {
                                    case .integer(let intVal):
                                        //print("DEBUG: Update key \(key) with INTEGER value: \(intVal)")
                                        parameters.append(intVal)
                                    case .double(let doubleVal):
                                        //print("DEBUG: Update key \(key) with DOUBLE value: \(doubleVal)")
                                        parameters.append(doubleVal)
                                    case .text(let textVal):
                                        //print("DEBUG: Update key \(key) with TEXT value: \(textVal)")
                                        parameters.append(textVal)
                                    case .data(let dataVal):
                                        //print("DEBUG: Update key \(key) with DATA value (size: \(dataVal.count))")
                                        parameters.append(dataVal)
                                    case .null:
                                        //print("DEBUG: Update key \(key) with NULL value")
                                        parameters.append(NSNull())
                                    }
                                }
                            }
                            
                            if !actualChangedKeys.isEmpty {
                                let setClause = actualChangedKeys.map { "`\($0)` = ?" }.joined(separator: ", ") + ", `_sync_status` = 0"
                                
                                // Add the record ID for the WHERE clause
                                let query = "UPDATE `\(tableName)` SET \(setClause) WHERE id = ?"
                                //print("DEBUG: Executing query: \(query)")
                                try await self.query(query, arguments: parameters + [recordID])
                                //print("DEBUG: Updated record \(recordID) in table \(tableName) with \(actualChangedKeys.count) changed fields")
                            } else {
                                //print("DEBUG: No actual changes detected for record \(recordID), skipping update")
                                // Just make sure the sync status is reset
                                try await self.query("UPDATE `\(tableName)` SET `_sync_status` = 0 WHERE id = ?", recordID)
                            }
                        }
                    }
                }
                
                // Process deletions
                for deletion in changes.deletions {
                    let tableName = deletion.recordType
                    let recordID = deletion.recordID.recordName
                    
                    // Skip internal tables that shouldn't be synced
                    if tableName == "_cloudkit_deletions" {
                        //print("DEBUG: Skipping deletion for internal table: \(tableName)")
                        continue
                    }
                    
                    //print("DEBUG: Deleting record \(recordID) from table \(tableName)")
                    try await query("DELETE FROM `\(tableName)` WHERE id = ?", recordID)
                    //print("DEBUG: Deleted record \(recordID) from table \(tableName)")
                }
            }
        } catch {
            print("ERROR: Error processing CloudKit changes: \(error.localizedDescription)")
            print("ERROR: Detailed error: \(error)")
        }
    }
    
    private func processSuccessfullyUploadedRecords(_ changes: CKSyncEngine.Event.SentRecordZoneChanges) async {
        //print("DEBUG: Processing successfully uploaded records: \(changes.savedRecords.count) saved, \(changes.deletedRecordIDs.count) deleted")
        
        do {
            for savedRecord in changes.savedRecords {
                let tableName = savedRecord.recordType
                let recordID = savedRecord.recordID.recordName
                
                // Skip internal tables that shouldn't be synced
                if tableName == "_cloudkit_deletions" {
                    //print("DEBUG: Skipping sync status update for internal table: \(tableName)")
                    continue
                }
                
                // Ensure _sync_status column exists
                try await ensureSyncStatusColumnExists(for: tableName)
                
                // Mark the record as successfully synced (status = 0)
                let query = "UPDATE `\(tableName)` SET _sync_status = 0 WHERE id = ?"
                //print("DEBUG: Executing query: \(query)")
                try await self.query(query, recordID)
                //print("DEBUG: Marked record \(recordID) in \(tableName) as synced")
            }
            
            // Also log deleted record IDs
            for deletedRecordID in changes.deletedRecordIDs {
                //print("DEBUG: Record \(deletedRecordID.recordName) was successfully deleted from CloudKit")
            }
        } catch {
            print("ERROR: Error updating sync status after upload: \(error.localizedDescription)")
            print("ERROR: Detailed error: \(error)")
        }
    }
    
    // Helper function to create a table if needed based on a CKRecord
    private func createTableIfNeeded(for tableName: String, record: CKRecord) async throws {
        // Skip internal tables that shouldn't be synced
        if tableName == "_cloudkit_deletions" {
            //print("DEBUG: Skipping internal table creation for: \(tableName)")
            return
        }
        
        // Check if table exists
        let tableExists = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", tableName).count > 0
        
        if !tableExists {
            //print("DEBUG: Creating new table \(tableName) from CloudKit record")
            
            // Create table based on record structure
            var columnDefs = ["id TEXT PRIMARY KEY NOT NULL", "_sync_status INTEGER NOT NULL DEFAULT 0"]
            
            for key in record.allKeys() {
                // Skip ID as we already defined it
                if key == "id" { continue }
                
                let value = record[key]
                var columnType = "TEXT"
                let nullable = "NULL" // Default to nullable
                
                if value is Int64 || value is Int {
                    columnType = "INTEGER"
                } else if value is Double || value is Float {
                    columnType = "REAL"
                } else if value is Data || value is CKAsset {
                    columnType = "BLOB"
                } else if value is Date {
                    // Store dates as TEXT (ISO8601 string)
                    columnType = "TEXT"
                } else if value is String {
                    columnType = "TEXT"
                }
                
                columnDefs.append("`\(key)` \(columnType) \(nullable)")
                //print("DEBUG: Added column \(key) with type \(columnType) to new table definition")
            }
            
            let createTableSQL = "CREATE TABLE IF NOT EXISTS `\(tableName)` (\(columnDefs.joined(separator: ", ")))"
            //print("DEBUG: Creating table with SQL: \(createTableSQL)")
            try await execute(createTableSQL)
            //print("DEBUG: Created new table \(tableName) with columns: \(columnDefs.joined(separator: ", "))")
            
            // Make sure deletion tracking table exists
            let deletionTableExists = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name='_cloudkit_deletions'").count > 0
            if !deletionTableExists {
                try await execute(Blackbird.Table.createCloudKitDeletionsTableStatement())
                //print("DEBUG: Created _cloudkit_deletions table to track deletions")
            }
            
            // Create triggers for CloudKit sync
            try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: tableName))
            try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: tableName))
            try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: tableName))
            
            //print("DEBUG: Added all sync triggers to table \(tableName)")
        } else {
            // Table exists - ensure it has a _sync_status column
            let hasStatusColumn = try await query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
                row["name"]?.stringValue == "_sync_status"
            })
            
            if !hasStatusColumn {
                //print("DEBUG: Adding _sync_status column to existing table \(tableName)")
                try await execute("ALTER TABLE `\(tableName)` ADD COLUMN _sync_status INTEGER NOT NULL DEFAULT 0")
                
                // Create triggers for CloudKit sync
                try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: tableName))
                try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: tableName))
                try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: tableName))
            }
            
            // Check for new columns that need to be added
            let existingColumns = try await query("PRAGMA table_info(`\(tableName)`)").map { row -> String? in
                return row["name"]?.stringValue
            }.compactMap { $0 }
            
            //print("DEBUG: Existing columns in table \(tableName): \(existingColumns.joined(separator: ", "))")
            
            var columnsAdded = 0
            
            for key in record.allKeys() {
                if key == "id" || key == "_sync_status" || existingColumns.contains(key) { continue }
                
                let value = record[key]
                var columnType = "TEXT"
                
                if value is Int64 || value is Int {
                    columnType = "INTEGER"
                } else if value is Double || value is Float {
                    columnType = "REAL"
                } else if value is Data || value is CKAsset {
                    columnType = "BLOB"
                }
                
                //print("DEBUG: Adding missing column \(key) with type \(columnType) to table \(tableName)")
                try await execute("ALTER TABLE `\(tableName)` ADD COLUMN `\(key)` \(columnType) NULL")
                columnsAdded += 1
            }
            
            if columnsAdded > 0 {
                //print("DEBUG: Added \(columnsAdded) new columns to table \(tableName)")
            }
        }
    }
    
    public func nextRecordZoneChangeBatch(_ context: CKSyncEngine.SendChangesContext, syncEngine: CKSyncEngine) async -> CKSyncEngine.RecordZoneChangeBatch? {
        let scope = context.options.scope
        let changes = syncEngine.state.pendingRecordZoneChanges.filter { scope.contains($0) }
        
        //print("DEBUG: nextRecordZoneChangeBatch called with \(changes.count) pending changes")
        for change in changes {
            //print("DEBUG: Pending change: \(change)")
        }
        
        if changes.isEmpty {
            //print("DEBUG: No pending changes, returning nil batch")
            return nil
        }
        
        // Create a record provider that doesn't throw
        let recordProvider: @Sendable (CKRecord.ID) async -> CKRecord? = { recordID in
            do {
                //print("DEBUG: Creating/updating CKRecord for ID: \(recordID.recordName) in zone: \(recordID.zoneID.zoneName)")
                
                let tableName = recordID.zoneID.zoneName
                
                // Check if table exists, create it if it doesn't
                let tableExists = try await self.query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", tableName).count > 0
                if !tableExists {
                    //print("DEBUG: Table \(tableName) does not exist yet, will be created when receiving data")
                    syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                    return nil
                }
                
                // Check if the table has id column
                let hasIDColumn = try await self.query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
                    row["name"]?.stringValue == "id"
                })
                
                if !hasIDColumn {
                    //print("DEBUG: Table \(tableName) has no id column, cannot sync")
                    syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                    return nil
                }
                
                // Try to find the corresponding entity in the database
                let results = try await self.query("SELECT * FROM `\(tableName)` WHERE id = ?", recordID.recordName)
                //print("DEBUG: Found \(results.count) matching rows in database")
                
                if let row = results.first {
                    //print("DEBUG: Row found with keys: \(row.keys.joined(separator: ", "))")
                    
                    // Reset the record's sync status to 0 (synced)
                    try? await self.query("UPDATE `\(tableName)` SET _sync_status = 0 WHERE id = ?", recordID.recordName)
                    
                    // Try to fetch the existing record from CloudKit first
                    let container = CKContainer(identifier: self.containerIdentifier)
                    let database = container.privateCloudDatabase
                    
                    do {
                        //print("DEBUG: Attempting to fetch existing record from CloudKit...")
                        let existingRecord = try await database.record(for: recordID)
                        //print("DEBUG: Successfully fetched existing record with keys: \(existingRecord.allKeys().joined(separator: ", "))")
                        
                        // Update the existing record with values from the database
                        row.updateCKRecord(existingRecord)
                        
                        //print("DEBUG: Updated existing CKRecord with new values")
                        return existingRecord
                    } catch {
                        //print("DEBUG: Failed to fetch existing record: \(error.localizedDescription), creating new one")
                        
                        // Record doesn't exist in CloudKit yet, create a new one
                        let record = CKRecord(recordType: tableName, recordID: recordID)
                        
                        // Entity exists locally, populate the CKRecord with values from the database
                        row.updateCKRecord(record)
                        
                        //print("DEBUG: New CKRecord created with keys: \(record.allKeys().joined(separator: ", "))")
                        return record
                    }
                } else {
                    // Entity doesn't exist anymore, remove the pending change
                    //print("DEBUG: Record \(recordID.recordName) no longer exists, removing pending change")
                    syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                    return nil
                }
            } catch {
                print("ERROR: Error fetching record data for \(recordID.recordName): \(error.localizedDescription)")
                // Handle error - remove the pending change since we couldn't process it
                //print("DEBUG: Removing pending change due to error")
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                
                return nil
            }
        }
        
        let batch = await CKSyncEngine.RecordZoneChangeBatch(pendingChanges: changes, recordProvider: recordProvider)
        //print("DEBUG: Created batch with \(changes.count) changes")
        return batch
    }
    
    // Helper function to create the deletion tracking table and triggers
    private func ensureDeletionTrackingExists() async throws {
        // Check if the deletion tracking table exists
        let deletionTableExists = try await self.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_cloudkit_deletions'").count > 0
        
        if !deletionTableExists {
            try await self.execute(Blackbird.Table.createCloudKitDeletionsTableStatement())
            //print("DEBUG: Created _cloudkit_deletions table to track deletions")
        }
        
        // Find all tables that support sync
        let tables = try await self.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '_cloudkit_deletions'")
        
        for table in tables {
            guard let tableName = table["name"]?.stringValue else { continue }
            
            // Skip internal tables that shouldn't be synced
            if tableName == "_cloudkit_deletions" {
                //print("DEBUG: Skipping deletion trigger for internal table: \(tableName)")
                continue
            }
            
            // Check if _sync_status column exists (indicates this table is synced)
            let hasStatusColumn = try await self.query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
                row["name"]?.stringValue == "_sync_status"
            })
            
            if hasStatusColumn {
                // Check if delete trigger exists
                let triggerExists = try await self.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name=?", "\(tableName)_ck_delete_trigger").count > 0
                
                if !triggerExists {
                    // Add delete trigger to track deletions
                    let deleteTrigger = """
                    CREATE TRIGGER IF NOT EXISTS `\(tableName)_ck_delete_trigger`
                    BEFORE DELETE ON `\(tableName)`
                    FOR EACH ROW
                    BEGIN
                        INSERT INTO `_cloudkit_deletions` (table_name, record_id) VALUES ('\(tableName)', OLD.id);
                    END
                    """
                    
                    try await self.execute(deleteTrigger)
                    //print("DEBUG: Added deletion tracking trigger to table \(tableName)")
                }
            }
        }
    }
}

// MARK: - Sync Engine Control Methods

extension Blackbird.Database {
    /// Starts the CloudKit sync engine and sets up the initial configuration.
    /// This should be called after the database is initialized and user is authenticated.
    /// Note: This only sets up the sync engine but doesn't automatically sync. Call sync() to perform synchronization.
    internal func startCloudKitSync() {
        guard !containerIdentifier.isEmpty else {
            print("ERROR: Cannot start CloudKit sync: No container identifier provided")
            return
        }
        
        //print("DEBUG: Starting CloudKit sync with container: \(containerIdentifier)")
        
        // Try to load state from disk if available (only for non-memory databases)
        if path != nil && stateSerialization.value == nil {
            loadStateSerialization()
        }
        
        // Start the sync engine
        _ = syncEngine
        
        // Set up zones but don't start automatic sync
        Task {
            // Check iCloud account status
            do {
                let container = CKContainer(identifier: containerIdentifier)
                //print("DEBUG: Checking CloudKit account status...")
                let accountStatus = try await container.accountStatus()
                //print("DEBUG: CloudKit account status: \(accountStatus)")
                
                if accountStatus != .available {
                    print("ERROR: CloudKit account not available (status: \(accountStatus)), sync may not work properly")
                }
            } catch {
                print("ERROR: Failed to check CloudKit account status: \(error.localizedDescription)")
            }
            
            // Ensure deletion tracking exists
            do {
                //print("DEBUG: Setting up deletion tracking...")
                try await ensureDeletionTrackingExists()
                //print("DEBUG: Deletion tracking setup complete")
            } catch {
                print("ERROR: Failed to set up deletion tracking: \(error.localizedDescription)")
            }
            
            // Ensure zone setup
            //print("DEBUG: Setting up CloudKit zones...")
            await setupCloudKitZones()
            
            //print("DEBUG: CloudKit sync engine is ready to use. Call sync() to perform manual synchronization.")
        }
    }
    
    /// Manually synchronizes data with CloudKit.
    /// This performs both a push of local changes and a fetch of remote changes.
    /// Call this method whenever you want to explicitly sync data with CloudKit.
    public func sync() async throws {
        guard !containerIdentifier.isEmpty else {
            print("ERROR: Cannot sync: No container identifier provided")
            fatalError("CloudKit sync requires a container identifier")
        }
        
        //print("DEBUG: Starting manual sync operation")
        
        // First check and sync deletions
        let deletionTableExists = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name='_cloudkit_deletions'").count > 0
        if deletionTableExists {
            let deletions = try await query("SELECT id, table_name, record_id FROM _cloudkit_deletions")
            if !deletions.isEmpty {
                //print("DEBUG: Found \(deletions.count) pending deletions to sync")
                
                for deletion in deletions {
                    guard let tableName = deletion["table_name"]?.stringValue,
                          let recordIDValue = deletion["record_id"]?.stringValue ?? deletion["record_id"]?.intValue?.description,
                          let deletionID = deletion["id"]?.intValue else {
                        continue
                    }
                    
                    // Create a CKRecord.ID for the deletion
                    let zoneID = CKRecordZone.ID(zoneName: tableName, ownerName: CKCurrentUserDefaultName)
                    let recordID = CKRecord.ID(recordName: recordIDValue, zoneID: zoneID)
                    
                    // Queue this record for deletion
                    //print("DEBUG: Queueing deletion of record \(recordIDValue) from table \(tableName)")
                    syncEngine.state.add(pendingRecordZoneChanges: [.deleteRecord(recordID)])
                    
                    // Delete the record from our tracking table since it's been queued
                    try await query("DELETE FROM _cloudkit_deletions WHERE id = ?", deletionID)
                }
                
                // Send the changes
                //print("DEBUG: Sending deletions to CloudKit")
                try await syncEngine.sendChanges()
            }
        }
        
        // Queue and push local changes
        //print("DEBUG: Uploading local changes to CloudKit")
        try await queueLocalChangesForSync()
        
        // Fetch changes from CloudKit
        //print("DEBUG: Fetching changes from CloudKit")
        try await syncEngine.fetchChanges()
        
        //print("DEBUG: Manual sync completed successfully")
    }
    
    /// Finds local changes that need to be synced to CloudKit and queues them up.
    private func queueLocalChangesForSync() async throws {
        // Find all tables instead of just those with _sync_status
        let tables = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        //print("DEBUG: Found \(tables.count) tables in database to sync")
        
        for table in tables {
            guard let tableName = table["name"]?.stringValue else { continue }
            
            // Skip internal tables that shouldn't be synced
            if tableName == "_cloudkit_deletions" {
                //print("DEBUG: Skipping internal table: \(tableName)")
                continue
            }
            
            // Check if _sync_status column exists, add it if it doesn't
            let hasStatusColumn = try await query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
                row["name"]?.stringValue == "_sync_status"
            })
            
            if !hasStatusColumn {
                //print("DEBUG: Adding _sync_status column to table \(tableName) during sync")
                try await execute("ALTER TABLE `\(tableName)` ADD COLUMN _sync_status INTEGER NOT NULL DEFAULT 1")
                
                // Add sync triggers
                try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: tableName))
                try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: tableName))
                try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: tableName))
            }
            
            // Check if table has an id column
            let hasIDColumn = try await query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
                row["name"]?.stringValue == "id"
            })
            
            if !hasIDColumn {
                //print("DEBUG: Table \(tableName) has no id column, cannot sync")
                continue
            }
            
            //print("DEBUG: Finding records to sync in table \(tableName)")
            
            // Find records in this table that need to be synced (_sync_status = 1)
            // We only look for records explicitly marked as needing sync now (status = 1)
            let records = try await query("SELECT * FROM `\(tableName)` WHERE _sync_status = 1")
            
            //print("DEBUG: Found \(records.count) modified records to sync in table \(tableName)")
            
            // Find records that have already been queued (status = 2)
            let queuedCount = try await query("SELECT COUNT(*) as count FROM `\(tableName)` WHERE _sync_status = 2").first?["count"]?.intValue ?? 0
            //print("DEBUG: Already queued \(queuedCount) records in table \(tableName)")
            
            var recordsQueued = 0
            for record in records {
                // Get the ID for this record
                guard let recordIDValue = record["id"]?.stringValue ?? record["id"]?.intValue?.description else {
                    //print("DEBUG: Record missing ID, skipping")
                    continue
                }
                
                //print("DEBUG: Processing record \(recordIDValue) with columns: \(record.keys.joined(separator: ", "))")
                
                // Create a CKRecord.ID with the record's zone
                let zoneID = CKRecordZone.ID(zoneName: tableName, ownerName: CKCurrentUserDefaultName)
                let recordID = CKRecord.ID(recordName: recordIDValue, zoneID: zoneID)
                
                // Queue this record for syncing
                //print("DEBUG: Queueing record \(recordIDValue) for sync")
                syncEngine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)])
                recordsQueued += 1
                
                // Mark the record as queued for sync (_sync_status = 2)
                try await query("UPDATE `\(tableName)` SET _sync_status = 2 WHERE id = ?", recordIDValue)
                //print("DEBUG: Marked record \(recordIDValue) as queued for sync (status 2)")
            }
            
            //print("DEBUG: Successfully queued \(recordsQueued) additional records from table \(tableName) for sync")
        }
        
        // Check for deletions to sync
        // Create deletion tracking table if it doesn't exist
        let deletionTableExists = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name='_cloudkit_deletions'").count > 0
        if !deletionTableExists {
            try await execute(Blackbird.Table.createCloudKitDeletionsTableStatement())
            //print("DEBUG: Created _cloudkit_deletions table to track deletions")
        } else {
            // Process pending deletions
            let deletions = try await query("SELECT id, table_name, record_id FROM _cloudkit_deletions")
            //print("DEBUG: Found \(deletions.count) pending deletions to sync")
            
            var deletionsProcessed = 0
            for deletion in deletions {
                guard let tableName = deletion["table_name"]?.stringValue,
                      let recordIDValue = deletion["record_id"]?.stringValue ?? deletion["record_id"]?.intValue?.description,
                      let deletionID = deletion["id"]?.intValue else {
                    continue
                }
                
                // Create a CKRecord.ID for the deletion
                let zoneID = CKRecordZone.ID(zoneName: tableName, ownerName: CKCurrentUserDefaultName)
                let recordID = CKRecord.ID(recordName: recordIDValue, zoneID: zoneID)
                
                // Queue this record for deletion
                //print("DEBUG: Queueing deletion of record \(recordIDValue) from table \(tableName)")
                syncEngine.state.add(pendingRecordZoneChanges: [.deleteRecord(recordID)])
                deletionsProcessed += 1
                
                // Delete the record from our tracking table since it's been queued
                try await query("DELETE FROM _cloudkit_deletions WHERE id = ?", deletionID)
            }
            
            //print("DEBUG: Successfully queued \(deletionsProcessed) record deletions for sync")
        }
        
        // Force a send of changes
        //print("DEBUG: Forcing sendChanges to upload queued records")
        try await syncEngine.sendChanges()
    }
    
    /// Setup CloudKit zones for tables that need syncing
    private func setupCloudKitZones() async {
        //print("DEBUG: Setting up CloudKit zones...")
        do {
            // Find all tables instead of just those with _sync_status
            let tables = try await query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            
            //print("DEBUG: Found \(tables.count) tables in database to sync")
            for table in tables {
                if let name = table["name"]?.stringValue {
                    // Skip internal tables that shouldn't be synced
                    if name == "_cloudkit_deletions" {
                        //print("DEBUG: Skipping internal table: \(name)")
                        continue
                    }
                    
                    //print("DEBUG: Table eligible for sync: \(name)")
                    
                    // Check if _sync_status column exists, add it if it doesn't
                    let hasStatusColumn = try await query("PRAGMA table_info(`\(name)`)").contains(where: { row in
                        row["name"]?.stringValue == "_sync_status"
                    })
                    
                    if !hasStatusColumn {
                        //print("DEBUG: Adding _sync_status column to table \(name)")
                        try await execute("ALTER TABLE `\(name)` ADD COLUMN _sync_status INTEGER NOT NULL DEFAULT 0")
                        
                        // Add sync triggers
                        try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: name))
                        try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: name))
                        try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: name))
                    } else {
                        // Check if triggers need to be updated
                        // Drop existing triggers to recreate them with the latest implementation
                        try? await execute("DROP TRIGGER IF EXISTS `\(name)_ck_insert_trigger`")
                        try? await execute("DROP TRIGGER IF EXISTS `\(name)_ck_update_trigger`")
                        try? await execute("DROP TRIGGER IF EXISTS `\(name)_ck_delete_trigger`")
                        
                        //print("DEBUG: Updating sync triggers for table \(name)")
                        try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: name))
                        try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: name))
                        try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: name))
                    }
                }
            }
            
            let container = CKContainer(identifier: containerIdentifier)
            let database = container.privateCloudDatabase
            
            // Create record zones for each table that requires syncing
            var recordZonesToCreate: [CKRecordZone] = []
            
            for table in tables {
                guard let tableName = table["name"]?.stringValue else { continue }
                
                // Skip internal tables that shouldn't be synced
                if tableName == "_cloudkit_deletions" {
                    continue
                }
                
                let zoneID = CKRecordZone.ID(zoneName: tableName, ownerName: CKCurrentUserDefaultName)
                let zone = CKRecordZone(zoneID: zoneID)
                recordZonesToCreate.append(zone)
                //print("DEBUG: Preparing to create zone for table: \(tableName)")
            }
            
            if !recordZonesToCreate.isEmpty {
                do {
                    //print("DEBUG: Creating \(recordZonesToCreate.count) CloudKit zones")
                    let operation = try await database.modifyRecordZones(saving: recordZonesToCreate, deleting: [])
                    //print("DEBUG: Successfully created CloudKit zones")
                    
                    // Loop through all the saved zones
                    var successCount = 0
                    for (zoneID, result) in operation.saveResults {
                        switch result {
                        case .success(let zone):
                            //print("DEBUG: Created zone: \(zone.zoneID.zoneName)")
                            successCount += 1
                        case .failure(let error):
                            print("ERROR: Failed to create zone \(zoneID.zoneName): \(error.localizedDescription)")
                        }
                    }
                    //print("DEBUG: Successfully created \(successCount) CloudKit zones")
                } catch {
                    print("ERROR: Error creating CloudKit zones: \(error.localizedDescription)")
                    print("ERROR: Detailed error info: \(error)")
                }
            } else {
                //print("DEBUG: No zones to create")
            }
        } catch {
            print("ERROR: Error setting up CloudKit zones: \(error.localizedDescription)")
            print("ERROR: Detailed error info: \(error)")
        }
    }
}

// Add a new persistence method to save state serialization after the SyncEngineDelegate implementation
extension Blackbird.Database {
    // MARK: - State Persistence Methods
    
    /// Saves the current CloudKit sync state to disk for persistence between app launches
    internal func saveStateSerialization() {
        guard let serialization = stateSerialization.value else {
            //print("DEBUG: No state serialization to save")
            return
        }
        
        // Skip saving for in-memory databases
        guard let dbPath = path else {
            //print("DEBUG: Not saving state for in-memory database")
            return
        }
        
        // Create state file path based on database path
        let stateFilePath = dbPath + ".ckstate"
        
        do {
            // Convert serialization to Data
            let data = try JSONEncoder().encode(serialization)
            
            // Write to file
            try data.write(to: URL(fileURLWithPath: stateFilePath))
            //print("DEBUG: Successfully saved CloudKit sync state to \(stateFilePath)")
        } catch {
            print("ERROR: Failed to save CloudKit sync state: \(error.localizedDescription)")
        }
    }
    
    /// Loads the CloudKit sync state from disk if available
    internal func loadStateSerialization() {
        // Skip loading for in-memory databases
        guard let dbPath = path else {
            //print("DEBUG: Not loading state for in-memory database")
            return
        }
        
        // Create state file path based on database path
        let stateFilePath = dbPath + ".ckstate"
        let stateFileURL = URL(fileURLWithPath: stateFilePath)
        
        // Check if file exists
        guard FileManager.default.fileExists(atPath: stateFilePath) else {
            //print("DEBUG: No saved CloudKit sync state found at \(stateFilePath)")
            return
        }
        
        do {
            // Read data from file
            let data = try Data(contentsOf: stateFileURL)
            
            // Decode serialization
            let serialization = try JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: data)
            
            // Set serialization
            stateSerialization.value = serialization
            //print("DEBUG: Successfully loaded CloudKit sync state from \(stateFilePath)")
        } catch {
            print("ERROR: Failed to load CloudKit sync state: \(error.localizedDescription)")
        }
    }
}

extension Blackbird.Database {
    // MARK: - Helper Functions
    
    /// Ensures that a table has the _sync_status column, adding it if it doesn't exist
    internal func ensureSyncStatusColumnExists(for tableName: String) async throws {
        // Skip internal tables that shouldn't be synced
        if tableName == "_cloudkit_deletions" {
            return
        }
        
        // Check if _sync_status column exists
        let hasStatusColumn = try await query("PRAGMA table_info(`\(tableName)`)").contains(where: { row in
            row["name"]?.stringValue == "_sync_status"
        })
        
        if !hasStatusColumn {
            // Add _sync_status column
            try await execute("ALTER TABLE `\(tableName)` ADD COLUMN _sync_status INTEGER NOT NULL DEFAULT 0")
            
            // Add sync triggers
            try await execute(Blackbird.Table.createCloudKitInsertTriggerStatement(tableName: tableName))
            try await execute(Blackbird.Table.createCloudKitUpdateTriggerStatement(tableName: tableName))
            try await execute(Blackbird.Table.createCloudKitDeleteTriggerStatement(tableName: tableName))
        }
    }
}
