# 从 .env 载入本地配置（密钥、JAVA_HOME 等）后启动应用，免去每次手动设置环境变量
# 用法：powershell -ExecutionPolicy Bypass -File .\run.ps1
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$envFile = Join-Path $root ".env"

if (-not (Test-Path $envFile)) {
    Write-Host "未找到 $envFile，请参考 README 创建后重试" -ForegroundColor Red
    exit 1
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf("=")
    if ($idx -lt 1) { return }
    $name = $line.Substring(0, $idx).Trim()
    $value = $line.Substring($idx + 1).Trim()
    [Environment]::SetEnvironmentVariable($name, $value, "Process")
    Write-Host ("  {0}=***" -f $name)
}

if (-not $env:JAVA_HOME) {
    $env:JAVA_HOME = "D:\SoftwareDownload\JDKVersion\jdk8"
}
Write-Host ("JAVA_HOME={0}" -f $env:JAVA_HOME)

Set-Location $root
mvn -o spring-boot:run @args
