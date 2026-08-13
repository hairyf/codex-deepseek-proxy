$ErrorActionPreference = 'Continue'
$dir = 'C:\Users\wwu71\.codex\deepseek-proxy'
$env:DEEPSEEK_UPSTREAM = 'https://opencode.ai/zen/go/v1'
$node = (Get-Command node -ErrorAction Stop).Source
$log = Join-Path $dir 'logs\proxy.log'
$errLog = Join-Path $dir 'logs\proxy-error.log'
$wdLog = Join-Path $dir 'logs\proxy-watchdog.log'
$pidFile = Join-Path $dir '.cache\watchdog.pid'

New-Item -ItemType Directory -Force -Path (Join-Path $dir 'logs') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dir '.cache') | Out-Null

Set-Content -LiteralPath $pidFile -Value $PID

function Log($msg) {
    Add-Content -LiteralPath $wdLog -Value ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg)
}

Log "watchdog started (pid $PID)"

while ($true) {
    $busy = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue
    if ($busy) {
        Log "port 8787 in use (pid $($busy.OwningProcess)), waiting for it to free"
        Start-Sleep -Seconds 5
        continue
    }
    $proc = Start-Process -FilePath $node -ArgumentList (Join-Path $dir 'proxy.mjs') -WorkingDirectory $dir -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $errLog -PassThru
    Log "node started pid=$($proc.Id)"
    $proc.WaitForExit()
    Log "node exited code=$($proc.ExitCode), restarting in 2s"
    Start-Sleep -Seconds 2
}
