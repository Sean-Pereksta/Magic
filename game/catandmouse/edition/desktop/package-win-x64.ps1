param(
    [string] $OutputDirectory,
    [string] $InnoSetup = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    [string] $DirectoryUrl = "",
    [switch] $PortableOnly
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$EditionRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
if (!$OutputDirectory) { $OutputDirectory = Join-Path $EditionRoot "dist" }
$ReleaseDirectory = (New-Item -ItemType Directory -Path $OutputDirectory -Force).FullName
if (!$PortableOnly -and !(Test-Path -LiteralPath $InnoSetup -PathType Leaf)) { throw "Install Inno Setup 6 or supply its ISCC.exe path." }
$Version = (Get-Content -LiteralPath (Join-Path $EditionRoot "package.json") -Raw | ConvertFrom-Json).version
$ReleaseName = "CatMouseLocal-$Version-win-x64"
$NodeVersion = "24.19.0"
$CloudflaredVersion = "2026.11.1"
$Build = New-Item -ItemType Directory -Path (Join-Path $ReleaseDirectory (".build-" + [Guid]::NewGuid().ToString("N")))
$Payload = New-Item -ItemType Directory -Path (Join-Path $Build.FullName "Cat Mouse Local Edition")
$Runtime = New-Item -ItemType Directory -Path (Join-Path $Payload.FullName "runtime")
$Licenses = New-Item -ItemType Directory -Path (Join-Path $Payload.FullName "licenses")
$Server = New-Item -ItemType Directory -Path (Join-Path $Payload.FullName "server")

function Save-VerifiedDownload([string] $Url, [string] $Destination, [string] $ExpectedHash) {
    Invoke-WebRequest -Uri $Url -OutFile $Destination
    if ((Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash -ne $ExpectedHash) {
        throw "SHA-256 verification failed for $Url"
    }
}

try {
    $NodeZip = Join-Path $Build.FullName "node.zip"
    Save-VerifiedDownload "https://nodejs.org/download/release/v$NodeVersion/node-v$NodeVersion-win-x64.zip" $NodeZip "57f71ab3652e797d84acddc79c81cc9ff1c6ddb2a1974cdb83f00fee9bff4c73"
    Expand-Archive -LiteralPath $NodeZip -DestinationPath $Build.FullName
    $NodeRoot = Join-Path $Build.FullName "node-v$NodeVersion-win-x64"
    Copy-Item -LiteralPath (Join-Path $NodeRoot "node.exe") -Destination $Runtime.FullName
    Copy-Item -LiteralPath (Join-Path $NodeRoot "LICENSE") -Destination (Join-Path $Licenses.FullName "Node-LICENSE.txt")
    Save-VerifiedDownload "https://github.com/cloudflare/cloudflared/releases/download/$CloudflaredVersion/cloudflared-windows-amd64.exe" (Join-Path $Runtime.FullName "cloudflared.exe") "341deda3db5ec498a0482254d6c38c0b7cf01a30c37bb5dd95d2216d08955e35"
    Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cloudflare/cloudflared/$CloudflaredVersion/LICENSE" -OutFile (Join-Path $Licenses.FullName "cloudflared-LICENSE.txt")

    Get-ChildItem -LiteralPath $EditionRoot -File | Where-Object { $_.Extension -in ".html", ".css", ".mjs", ".md" } | Copy-Item -Destination $Payload.FullName
    $Startup = Join-Path $Payload.FullName "index.html"
    (Get-Content -LiteralPath $Startup -Raw) -replace '(?m)^<script type="module" src="/hub/play-signal\.mjs"[^>]*></script>\r?\n', '' |
        Set-Content -LiteralPath $Startup -Encoding utf8 -NoNewline
    Copy-Item -LiteralPath (Join-Path $EditionRoot "art") -Destination $Payload.FullName -Recurse
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "Play Cat Mouse.vbs") -Destination $Payload.FullName
    Get-ChildItem -LiteralPath (Join-Path $EditionRoot "server") -Filter "*.mjs" -File |
        Where-Object { $_.Name -notlike "*.test.mjs" } | Copy-Item -Destination $Server.FullName
    Copy-Item -LiteralPath (Join-Path $EditionRoot "package.json") -Destination $Payload.FullName
    if ($DirectoryUrl) {
        $Url = [uri] $DirectoryUrl
        if (!$Url.IsAbsoluteUri -or $Url.Scheme -ne "https" -or $Url.UserInfo -or $Url.Query -or $Url.Fragment) {
            throw "DirectoryUrl must be an HTTPS service URL."
        }
        $ConfigFile = Join-Path $Payload.FullName "directory-config.mjs"
        $Config = Get-Content -LiteralPath $ConfigFile -Raw
        $QuotedUrl = $Url.AbsoluteUri.TrimEnd("/") | ConvertTo-Json -Compress
        $Config -replace "export const CAT_DIRECTORY_URL = [^;]*;", "export const CAT_DIRECTORY_URL = $QuotedUrl;" |
            Set-Content -LiteralPath $ConfigFile -Encoding utf8
    }
    Push-Location -LiteralPath $Payload.FullName
    try {
        & (Join-Path $NodeRoot "npm.cmd") install --omit=dev --omit=optional --ignore-scripts --no-audit --no-fund --prefer-offline
        if ($LASTEXITCODE -ne 0) { throw "The match server dependency could not be installed." }
        Copy-Item -LiteralPath (Join-Path $Payload.FullName "node_modules\ws\LICENSE") -Destination (Join-Path $Licenses.FullName "ws-LICENSE.txt")
        & (Join-Path $Runtime.FullName "node.exe") (Join-Path $PSScriptRoot "verify-payload.mjs") $Payload.FullName
        if ($LASTEXITCODE -ne 0) { throw "The offline payload verification failed." }
    } finally {
        Pop-Location
    }
    $ZipPath = Join-Path $ReleaseDirectory "$ReleaseName.zip"
    Compress-Archive -LiteralPath $Payload.FullName -DestinationPath $ZipPath -Force
    $Artifacts = @($ZipPath)
    if (!$PortableOnly) {
        & $InnoSetup "/DReleaseVersion=$Version" "/DReleaseName=$ReleaseName" "/DPayloadDirectory=$($Payload.FullName)" "/DReleaseDirectory=$ReleaseDirectory" (Join-Path $PSScriptRoot "installer.iss")
        if ($LASTEXITCODE -ne 0) { throw "Installer compilation failed." }
        $Artifacts += Join-Path $ReleaseDirectory "$ReleaseName-Setup.exe"
    }
    $Artifacts | ForEach-Object {
        $Hash = Get-FileHash -LiteralPath $_ -Algorithm SHA256
        $Hash.Hash.ToLowerInvariant() + "  " + [IO.Path]::GetFileName($_)
    } | Set-Content -LiteralPath (Join-Path $ReleaseDirectory "SHA256SUMS.txt") -Encoding ascii
} finally {
    $Target = (Resolve-Path -LiteralPath $Build.FullName).Path
    $ExpectedRoot = $ReleaseDirectory.TrimEnd("\") + "\"
    if ($Target.StartsWith($ExpectedRoot, [StringComparison]::OrdinalIgnoreCase) -and (Split-Path $Target -Leaf) -like ".build-*") {
        Remove-Item -LiteralPath $Target -Recurse -Force
    }
}
