# Updates MHC scheduled task timings using the owner's credentials.
# A secure Windows credential dialog collects the password; it is never logged.
$ErrorActionPreference = 'Stop'

Write-Host 'A credential dialog will appear.' -ForegroundColor Cyan
Write-Host 'Enter the password for APAC\aahame425 (username is pre-filled).' -ForegroundColor Cyan

$cred = Get-Credential -UserName 'APAC\aahame425' -Message 'Enter Windows password for APAC\aahame425 to update the scheduled tasks'
$plain = $cred.GetNetworkCredential().Password

$updates = @(
    @{ Name = 'MHC_CauseList_Daily'; Time = '3:00PM' },
    @{ Name = 'MHC_VC_Link_Refresh'; Time = '3:30PM' }
)

foreach ($u in $updates) {
    try {
        Set-ScheduledTask -TaskName $u.Name `
            -Trigger (New-ScheduledTaskTrigger -Daily -At $u.Time) `
            -User $cred.UserName -Password $plain -ErrorAction Stop | Out-Null
        Write-Host ("OK: {0} -> daily {1}" -f $u.Name, $u.Time) -ForegroundColor Green
    } catch {
        Write-Host ("FAILED: {0} : {1}" -f $u.Name, $_.Exception.Message) -ForegroundColor Red
    }
}

$plain = $null
Write-Host ''
Write-Host '--- Final state ---'
foreach ($u in $updates) {
    $t = Get-ScheduledTask -TaskName $u.Name
    $i = $t | Get-ScheduledTaskInfo
    Write-Host ("{0} | Logon={1} | Start={2} | Next={3}" -f $u.Name, $t.Principal.LogonType, $t.Triggers.StartBoundary, $i.NextRunTime)
}
