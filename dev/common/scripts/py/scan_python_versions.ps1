Write-Host "🔍 Searching for installed Python versions..."

# Check common install paths
$paths = @(
    "$env:ProgramFiles\Python",
    "$env:ProgramFiles\Python*",
    "$env:ProgramFiles\WindowsApps",
    "$env:LocalAppData\Programs\Python",
    "$env:LocalAppData\Microsoft\WindowsApps"
)

$found = @()

foreach ($path in $paths) {
    if (Test-Path $path) {
        Get-ChildItem -Path $path -Recurse -Filter python.exe -ErrorAction SilentlyContinue | ForEach-Object {
            $version = & $_.FullName --version 2>&1
            $found += [PSCustomObject]@{
                Version = $version
                Path    = $_.FullName
            }
        }
    }
}

# Check registry for Python installations
$regPaths = @(
    "HKLM:\SOFTWARE\Python\PythonCore",
    "HKCU:\SOFTWARE\Python\PythonCore"
)

foreach ($regPath in $regPaths) {
    if (Test-Path $regPath) {
        Get-ChildItem $regPath | ForEach-Object {
            $ver = $_.PSChildName
            $installPath = (Get-ItemProperty "$regPath\$ver\InstallPath").ExecutablePath
            if ($installPath) {
                $found += [PSCustomObject]@{
                    Version = "Python $ver (from registry)"
                    Path    = $installPath
                }
            }
        }
    }
}

# Display results
if ($found.Count -eq 0) {
    Write-Host "⚠️ No Python installations found."
} else {
    Write-Host "`n✅ Installed Python Versions:`n"
    $found | Format-Table -AutoSize
}