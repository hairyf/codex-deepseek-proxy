$dir = Join-Path $env:USERPROFILE '.codex\deepseek-proxy'
$pidFile = Join-Path $dir '.cache\watchdog.pid'

if (Test-Path $pidFile) {
    $wdPid = Get-Content -LiteralPath $pidFile
    Stop-Process -Id $wdPid -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
    Write-Output "watchdog stopped (pid $wdPid)"
} else {
    Write-Output "watchdog pid file not found"
}

$c = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue
if ($c) {
    Stop-Process -Id $c.OwningProcess -Force
    Write-Output "proxy stopped (pid $($c.OwningProcess))"
} else {
    Write-Output "proxy is not running"
}
