$ErrorActionPreference = 'Stop'
$dir = Join-Path $env:USERPROFILE '.codex\deepseek-proxy'
$pidFile = Join-Path $dir 'watchdog.pid'

$existing = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue

if (Test-Path $pidFile) {
    $wdPid = Get-Content -LiteralPath $pidFile
    if (Get-Process -Id $wdPid -ErrorAction SilentlyContinue) {
        if ($existing) {
            Write-Output "proxy already listening on 8787 (pid $($existing.OwningProcess)); watchdog running (pid $wdPid)"
        } else {
            Write-Output "watchdog running (pid $wdPid), proxy starting..."
        }
        exit 0
    }
}

$ps = (Get-Command powershell -ErrorAction Stop).Source
Start-Process -FilePath $ps -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-WindowStyle','Hidden','-File',(Join-Path $dir 'watchdog.ps1') -WorkingDirectory $dir -WindowStyle Hidden

$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 300
    if (Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue) {
        $ready = $true
        break
    }
    if ($i -eq 4) {
        # proxy may take over an existing process later; watchdog is what matters
        if (Test-Path $pidFile) { $ready = $true; break }
    }
}
if ($ready) {
    Write-Output "proxy started via watchdog: http://127.0.0.1:8787 (watchdog log: $dir\proxy-watchdog.log)"
} else {
    Write-Output "proxy failed to start; see $dir\proxy-error.log"
    exit 1
}
