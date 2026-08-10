$ErrorActionPreference = 'Stop'
$dir = Join-Path $env:USERPROFILE '.codex\deepseek-proxy'
$node = (Get-Command node -ErrorAction Stop).Source
$log = Join-Path $dir 'proxy.log'
$errLog = Join-Path $dir 'proxy-error.log'

$existing = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue
if ($existing) {
    Write-Output "proxy already listening on 8787 (pid $($existing.OwningProcess))"
    exit 0
}

Start-Process -FilePath $node -ArgumentList (Join-Path $dir 'proxy.mjs') -WorkingDirectory $dir -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $errLog

$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 300
    if (Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue) {
        $ready = $true
        break
    }
}
if ($ready) {
    Write-Output "proxy started: http://127.0.0.1:8787 (logs: $log / $errLog)"
} else {
    Write-Output "proxy failed to start; see $errLog"
    exit 1
}
