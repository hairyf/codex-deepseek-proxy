$c = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue
if ($c) {
    Stop-Process -Id $c.OwningProcess -Force
    Write-Output "proxy stopped (pid $($c.OwningProcess))"
} else {
    Write-Output "proxy is not running"
}
