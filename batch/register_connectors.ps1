$ErrorActionPreference = "Stop"

$ConnectUrl = if ($env:CONNECT_URL) { $env:CONNECT_URL } else { "http://localhost:8083" }
$ConfigDir = if ($env:CONFIG_DIR) { $env:CONFIG_DIR } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

function Register-Connector {
    param (
        [string]$Name,
        [string]$File
    )

    Write-Host "Registering $Name from $File"
    Invoke-RestMethod `
        -Method Put `
        -Uri "$ConnectUrl/connectors/$Name/config" `
        -ContentType "application/json" `
        -InFile $File
}

Register-Connector "s3-sink-weather-realtime" (Join-Path $ConfigDir "config.json")
Register-Connector "s3-sink-weather-forecast" (Join-Path $ConfigDir "config_forecast.json")
Register-Connector "s3-sink-pollution" (Join-Path $ConfigDir "config_pollution.json")
