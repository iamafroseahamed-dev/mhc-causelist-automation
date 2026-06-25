# Updates MHC scheduled task timings and switches them to S4U logon
# (runs whether or not the user is logged on; no stored password needed).
$ErrorActionPreference = 'Stop'

$principal = New-ScheduledTaskPrincipal -UserId 'APAC\aahame425' -LogonType S4U -RunLevel Limited

Set-ScheduledTask -TaskName 'MHC_CauseList_Daily' `
    -Trigger (New-ScheduledTaskTrigger -Daily -At 3:00PM) `
    -Principal $principal | Out-Null

Set-ScheduledTask -TaskName 'MHC_VC_Link_Refresh' `
    -Trigger (New-ScheduledTaskTrigger -Daily -At 3:30PM) `
    -Principal $principal | Out-Null

foreach ($n in 'MHC_CauseList_Daily', 'MHC_VC_Link_Refresh') {
    $t = Get-ScheduledTask -TaskName $n
    $i = $t | Get-ScheduledTaskInfo
    Write-Host "$n | Logon=$($t.Principal.LogonType) | Start=$($t.Triggers.StartBoundary) | Next=$($i.NextRunTime)"
}
Write-Host ''
Write-Host 'Done. You can close this window.'
Start-Sleep -Seconds 20
