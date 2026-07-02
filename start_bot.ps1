$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) { return }
        $parts = $line.Split("=", 2)
        if ($parts.Count -eq 2) {
            [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
        }
    }
}

if (-not $env:DISCORD_TOKEN) {
    Write-Host "DISCORD_TOKEN is missing. Add it to .env first." -ForegroundColor Red
    exit 1
}

python -m pip install -r requirements.txt
python main.py
