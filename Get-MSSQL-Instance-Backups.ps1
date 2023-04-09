Get-Content "C:\temp\Settings.ini" | foreach-object -begin {$h=@{}} -process { $k = [regex]::split($_,'='); if(($k[0].CompareTo("") -ne 0) -and ($k[0].StartsWith("[") -ne $True)) { $h.Add($k[0], $k[1]) } }
$server        = $h.Get_Item("NameOnCenteralServer")
$inventoryDB   = $h.Get_Item("NameOnIventoryDB")

if($server.length -eq 0){
    Write-Host "You must provide a value for the 'centralServer' in your Settings.ini file!!!" -BackgroundColor Red
    exit
}
if($inventoryDB.length -eq 0){
    Write-Host "You must provide a value for the 'inventoryDB' in your Settings.ini file!!!" -BackgroundColor Red
    exit
}

$mslExistenceQuery = "
SELECT Count(*) FROM dbo.sysobjects where id = object_id(N'[inventory].[MasterServerList]') and OBJECTPROPERTY(id, N'IsTable') = 1
"
$result = Invoke-Sqlcmd -Query $mslExistenceQuery -Database $inventoryDB -ServerInstance $server -ErrorAction Stop 

if($result[0] -eq 0){
    Write-Host "The table [inventory].[MasterServerList] wasn't found!!!" -BackgroundColor Red 
    exit
}

$enoughInstancesInMSLQuery = "
SELECT COUNT(*) FROM inventory.MasterServerList WHERE is_active = 1
"
$result = Invoke-Sqlcmd -Query $enoughInstancesInMSLQuery -Database $inventoryDB -ServerInstance $server -ErrorAction Stop 

if($result[0] -eq 0){
    Write-Host "There are no active instances registered to work with!!!" -BackgroundColor Red 
    exit
}

if ($h.Get_Item("username").length -gt 0 -and $h.Get_Item("password").length -gt 0) {
    $username   = $h.Get_Item("username")
    $password   = $h.Get_Item("password")
}

#Function to execute queries (depending on if the user will be using specific credentials or not)
function Execute-Query([string]$query,[string]$database,[string]$instance,[int]$trusted){
    if($trusted -eq 1){ 
        try{
            Invoke-Sqlcmd -Query $query -Database $database -ServerInstance $instance -ErrorAction Stop
        }
        catch{
            [string]$message = $_
            $errorQuery = "INSERT INTO monitoring.ErrorLog VALUES((SELECT serverId FROM inventory.MasterServerList WHERE CASE instance WHEN 'MSSQLSERVER' THEN server_name ELSE CONCAT(server_name,'\',instance) END = '$($instance)'),'Get-MSSQL-Instance-Backups','"+$message.replace("'","''")+"',GETDATE())"
            Invoke-Sqlcmd -Query $errorQuery -Database $inventoryDB -ServerInstance $server -ErrorAction Stop
        }
    }
    else{
        try{
            Invoke-Sqlcmd -Query $query -Database $database -ServerInstance $instance -Username $username -Password $password -ErrorAction Stop
        }
        catch{
            [string]$message = $_
            $errorQuery = "INSERT INTO monitoring.ErrorLog VALUES((SELECT serverId FROM inventory.MasterServerList WHERE CASE instance WHEN 'MSSQLSERVER' THEN server_name ELSE CONCAT(server_name,'\',instance) END = '$($instance)'),'Get-MSSQL-Instance-Backups','"+$message.replace("'","''")+"',GETDATE())"
            Invoke-Sqlcmd -Query $errorQuery -Database $inventoryDB -ServerInstance $server -ErrorAction Stop
        }
    }
}

##################################
#Backups inventory table creation#
##################################
$backupsInventoryTableQuery = "
IF NOT EXISTS (SELECT * FROM dbo.sysobjects where id = object_id(N'[inventory].[Backups]') and OBJECTPROPERTY(id, N'IsTable') = 1)
BEGIN
CREATE TABLE [inventory].[Backups](
    [serverId]                        [INT]NOT NULL,
    [database]                        [NVARCHAR](32) NOT NULL,
    [state]                           [NVARCHAR](32) NOT NULL,
	[recovery_model]                  [NVARCHAR](32) NOT NULL,
    [last_full]                       [DATETIME] NULL,
	[time_since_last_full]            [INT] NULL,
    [full_backup_size]                [DECIMAL](10,3) NULL,
    [full_backup_seconds_to_complete] [INT] NULL,
    [full_backup_path]                [NVARCHAR](255) NULL,
    [last_diff]                       [DATETIME] NULL,
	[time_since_last_diff]            [INT] NULL,
    [diff_backup_size]                [DECIMAL](10,3) NULL,
    [diff_backup_seconds_to_complete] [INT] NULL,
    [diff_backup_path]                [NVARCHAR](255) NULL,
    [last_tlog]                       [DATETIME] NULL,
	[time_since_last_tlog]            [INT] NULL,
    [tlog_backup_size]                [DECIMAL](10,3) NULL,
    [tlog_backup_seconds_to_complete] [INT] NULL,
    [tlog_backup_path]                [NVARCHAR](255) NULL,
    [data_collection_timestamp] [DATETIME] NOT NULL

    CONSTRAINT PK_BackupsInventory PRIMARY KEY CLUSTERED (serverId,[database]),
    CONSTRAINT FK_BackupsInventory_MasterServerList FOREIGN KEY (serverId) REFERENCES inventory.MasterServerList(serverId) ON DELETE NO ACTION ON UPDATE NO ACTION,

) ON [PRIMARY]
END
"
Execute-Query $backupsInventoryTableQuery $inventoryDB $server 1

#TRUNCATE the inventory.Backups table to always store a fresh copy of the information from all the instances
Execute-Query "TRUNCATE TABLE inventory.Backups" $inventoryDB $server 1

#Select the instances from the Master Server List that will be traversed
$instanceLookupQuery = "
SELECT
        serverId,
        trusted,
		CASE instance 
			WHEN 'MSSQLSERVER' THEN server_name                                   
			ELSE CONCAT(server_name,'\',instance)
		END AS 'instance',
		CASE instance 
			WHEN 'MSSQLSERVER' THEN ip                                   
			ELSE CONCAT(ip,'\',instance)
		END AS 'ip',
        CONCAT(ip,',',port) AS 'port'
FROM inventory.MasterServerList
WHERE is_active = 1
"
$instances = Execute-Query $instanceLookupQuery $inventoryDB $server 1

#For each instance, fetch the desired information
$backupsInformationQuery = "
WITH MostRecentBackups
AS(
    SELECT 
        database_name AS [Database],
        MAX(bus.backup_finish_date) AS LastBackupTime,
        CASE bus.type
            WHEN 'D' THEN 'Full'
            WHEN 'I' THEN 'Differential'
            WHEN 'L' THEN 'Transaction Log'
        END AS Type
    FROM msdb.dbo.backupset bus
    WHERE bus.type <> 'F'
    GROUP BY bus.database_name,bus.type
),
BackupsWithSize
AS(
    SELECT 
        mrb.*, 
		(SELECT TOP 1 CONVERT(DECIMAL(10,4), b.compressed_backup_size/1024/1024/1024) AS backup_size FROM msdb.dbo.backupset b WHERE [Database] = b.database_name AND LastBackupTime = b.backup_finish_date) AS [Backup Size],
		(SELECT TOP 1 DATEDIFF(s, b.backup_start_date, b.backup_finish_date) FROM msdb.dbo.backupset b WHERE [Database] = b.database_name AND LastBackupTime = b.backup_finish_date) AS [Seconds],
        (SELECT TOP 1 b.media_set_id FROM msdb.dbo.backupset b WHERE [Database] = b.database_name AND LastBackupTime = b.backup_finish_date) AS media_set_id
    FROM MostRecentBackups mrb
)

SELECT 
      SERVERPROPERTY('ServerName') AS Instance, 
      d.name AS [Database],
      d.state_desc AS State,
      d.recovery_model_desc AS [Recovery Model],
      bf.LastBackupTime AS [Last Full],
      DATEDIFF(DAY,bf.LastBackupTime,GETDATE()) AS [Time Since Last Full (in Days)],
      bf.[Backup Size] AS [Full Backup Size],
      bf.Seconds AS [Full Backup Seconds to Complete],
      CASE WHEN DATEDIFF(DAY,bf.LastBackupTime,GETDATE()) > 14 THEN NULL ELSE (SELECT TOP 1 bmf.physical_device_name FROM msdb.dbo.backupmediafamily bmf WHERE bmf.media_set_id = bf.media_set_id AND bmf.device_type = 2) END AS [Full Backup Path],
      bd.LastBackupTime AS [Last Differential],
      DATEDIFF(DAY,bd.LastBackupTime,GETDATE()) AS [Time Since Last Differential (in Days)],
      bd.[Backup Size] AS [Differential Backup Size],
      bd.Seconds AS [Diff Backup Seconds to Complete],
      CASE WHEN DATEDIFF(DAY,bd.LastBackupTime,GETDATE()) > 14 THEN NULL ELSE (SELECT TOP 1 bmf.physical_device_name FROM msdb.dbo.backupmediafamily bmf WHERE bmf.media_set_id = bd.media_set_id AND bmf.device_type = 2) END AS [Diff Backup Path],
      bt.LastBackupTime AS [Last Transaction Log],
      DATEDIFF(MINUTE,bt.LastBackupTime,GETDATE()) AS [Time Since Last Transaction Log (in Minutes)],
      bt.[Backup Size] AS [Transaction Log Backup Size],
      bt.Seconds AS [TLog Backup Seconds to Complete],
      CASE WHEN DATEDIFF(MINUTE,bt.LastBackupTime,GETDATE()) > 10080 THEN NULL ELSE (SELECT TOP 1 bmf.physical_device_name FROM msdb.dbo.backupmediafamily bmf WHERE bmf.media_set_id = bt.media_set_id AND bmf.device_type = 2) END AS [Transaction Log Backup Path]
FROM sys.databases d
LEFT JOIN BackupsWithSize bf ON (d.name = bf.[Database] AND (bf.Type = 'Full' OR bf.Type IS NULL))
LEFT JOIN BackupsWithSize bd ON (d.name = bd.[Database] AND (bd.Type = 'Differential' OR bd.Type IS NULL))
LEFT JOIN BackupsWithSize bt ON (d.name = bt.[Database] AND (bt.Type = 'Transaction Log' OR bt.Type IS NULL))
WHERE d.name <> 'tempdb' AND d.source_database_id IS NULL
"

foreach ($instance in $instances){
   if($instance.trusted -eq 'True'){$trusted = 1}else{$trusted = 0}
   $sqlInstance = $instance.instance

   #Go grab the complementary information for the instance
   Write-Host "Fetching backups information from instance" $instance.instance
   
   #Special logic for cases where the instance isn't reachable by name
   try{
        $results = Execute-Query $backupsInformationQuery "master" $sqlInstance $trusted
   }
   catch{
        $sqlInstance = $instance.ip
        [string]$message = $_
        $query = "INSERT INTO monitoring.ErrorLog VALUES("+$instance.serverId+",'Get-MSSQL-Instance-Backups','"+$message.replace("'","''")+"',GETDATE())"
        Execute-Query $query $inventoryDB $server 1

        try{  
            $results = Execute-Query $backupsInformationQuery "master" $sqlInstance $trusted
        }
        catch{
            $sqlInstance = $instance.port
            [string]$message = $_
            $query = "INSERT INTO monitoring.ErrorLog VALUES("+$instance.serverId+",'Get-MSSQL-Instance-Backups','"+$message.replace("'","''")+"',GETDATE())"
            Execute-Query $query $inventoryDB $server 1

            try{
                $results = Execute-Query $backupsInformationQuery "master" $sqlInstance $trusted
            }
            catch{
                [string]$message = $_
                $query = "INSERT INTO monitoring.ErrorLog VALUES("+$instance.serverId+",'Get-MSSQL-Instance-Backups','"+$message.replace("'","''")+"',GETDATE())"
                Execute-Query $query $inventoryDB $server 1
            }
        }
   }
   
   #Perform the INSERT in the inventory.Backups only if it returns information
   if($results.Length -ne 0){

      #Build the insert statement
      $insert = "INSERT INTO inventory.Backups VALUES"
      foreach($result in $results){ 
           if($result['Last Full'].ToString().trim() -eq [String]::Empty){$LastFull = "''"} else{$LastFull = $result['Last Full'] }
           if($result['Time Since Last Full (in Days)'].ToString().trim() -eq [String]::Empty){$TimeSinceLastFull = "''"} else{$TimeSinceLastFull = $result['Time Since Last Full (in Days)']}
           if($result['Full Backup Size'].ToString().trim() -eq [String]::Empty){$FullBackupSize = "''"} else{$FullBackupSize = $result['Full Backup Size']}
           if($result['Full Backup Seconds to Complete'].ToString().trim() -eq [String]::Empty){$FullBackupSecondstoComplete = "''"} else{$FullBackupSecondstoComplete = $result['Full Backup Seconds to Complete']}         
           if($result['Full Backup Path'].ToString().trim() -eq [String]::Empty){$FullBackupPath = "''"} else{$FullBackupPath = $result['Full Backup Path']}         
           if($result['Last Differential'].ToString().trim() -eq [String]::Empty){$LastDifferential = "''"} else{$LastDifferential = $result['Last Differential'] }
           if($result['Time Since Last Differential (in Days)'].ToString().trim() -eq [String]::Empty){$TimeSinceLastDifferential = "''"} else{$TimeSinceLastDifferential = $result['Time Since Last Differential (in Days)']}
           if($result['Differential Backup Size'].ToString().trim() -eq [String]::Empty){$DifferentialBackupSize = "''"} else{$DifferentialBackupSize = $result['Differential Backup Size']}
           if($result['Diff Backup Seconds to Complete'].ToString().trim() -eq [String]::Empty){$DiffBackupSecondstoComplete = "''"} else{$DiffBackupSecondstoComplete = $result['Diff Backup Seconds to Complete']}
           if($result['Diff Backup Path'].ToString().trim() -eq [String]::Empty){$DiffBackupPath = "''"} else{$DiffBackupPath = $result['Diff Backup Path']} 
           if($result['Last Transaction Log'].ToString().trim() -eq [String]::Empty){$LastTransactionLog = "''"} else{$LastTransactionLog = $result['Last Transaction Log'] }
           if($result['Time Since Last Transaction Log (in Minutes)'].ToString().trim() -eq [String]::Empty){$TimeSinceLastTransactionLog = "''"} else{$TimeSinceLastTransactionLog = $result['Time Since Last Transaction Log (in Minutes)']}
           if($result['Transaction Log Backup Size'].ToString().trim() -eq [String]::Empty){$TransactionLogBackupSize = "''"} else{$TransactionLogBackupSize = $result['Transaction Log Backup Size']}
           if($result['TLog Backup Seconds to Complete'].ToString().trim() -eq [String]::Empty){$TLogBackupSecondstoComplete = "''"} else{$TLogBackupSecondstoComplete = $result['TLog Backup Seconds to Complete']}
           if($result['Transaction Log Backup Path'].ToString().trim() -eq [String]::Empty){$TransactionLogBackupPath = "''"} else{$TransactionLogBackupPath = $result['Transaction Log Backup Path']} 

         $insert += "
         (
          '"+$instance.serverId+"',
          '"+$result['Database']+"',
          '"+$result['State']+"',
          '"+$result['Recovery Model']+"',
          '"+$LastFull+"',
          "+$TimeSinceLastFull+",
          "+$FullBackupSize+",
          "+$FullBackupSecondstoComplete+",
          '"+$FullBackupPath+"',
          '"+$LastDifferential+"',
          "+$TimeSinceLastDifferential+",
          "+$DifferentialBackupSize+",
          "+$DiffBackupSecondstoComplete+",
          '"+$DiffBackupPath+"',
          '"+$LastTransactionLog+"',
          "+$TimeSinceLastTransactionLog+",
          "+$TransactionLogBackupSize+",
          "+$TLogBackupSecondstoComplete+",
          '"+$TransactionLogBackupPath+"',
          GETDATE()
         ),
         "
       }

       $insert = $insert -replace "''",'NULL'
       $insert = $insert -replace "NULLNULL",'NULL'
       Execute-Query $insert.Substring(0,$insert.LastIndexOf(',')) $inventoryDB $server 1
   }
}

Write-Host "Done!"
