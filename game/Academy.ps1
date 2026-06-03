# ============================================================
# Multi-Report Coaching Email Generator
# Opens Outlook email windows only.
# Does NOT send automatically.
# Does NOT intentionally save drafts.
# ============================================================
#
# Reports supported:
# 1. Insurance Cash Utilization
# 2. Tentative Wording
# 3. Call Disconnects
# 4. Save the Sale Usage
# 5. Rapport Utilization
# 6. Low Compliance / Low Soft Skills Scores
#
# Every selected report uses the same Master Leader Lookup file.
# Emails are grouped by supervisor/coach.
# Coach manager is CC'd.
# ============================================================

# Relaunch in STA mode if needed so the Windows file picker works correctly
if ([System.Threading.Thread]::CurrentThread.ApartmentState -ne "STA") {
    Write-Host "Relaunching PowerShell in STA mode for file picker..."
    Start-Process powershell.exe -ArgumentList "-NoProfile -ExecutionPolicy Bypass -STA -File `"$PSCommandPath`""
    exit
}

Add-Type -AssemblyName System.Windows.Forms

# -----------------------------
# Settings
# -----------------------------

$EmailSubject = "Coaching Follow-Up Needed"
$TopInsuranceCashCount = 50

# Preset report files.
# For reports with a preset path:
#   Y = use this preset file if it exists
#   U = upload/select a different file
#   N = skip the report
# For reports without a preset path:
#   Y automatically opens the file picker, same as U
$PresetReportFiles = @{
    InsuranceCash      = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Skipping Insurance Cash Offer ...2026-06-01T15_19_02.661Z.xlsx"
    TentativeWording  = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Using Tentative Language and C...2026-06-01T15_51_43.911Z.xlsx"
    SaveTheSale       = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Save The Sale Usage...2026-06-01T15_51_43.722Z.xlsx"
    CallDisconnects   = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Call Disconnects By Agent...2026-06-01T15_51_43.636Z.xlsx"
    RapportUtilization = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Rapport Percentage...2026-06-02T18_37_07.242Z.xlsx"
    LowScores          = "C:\Users\Sean.pereksta\OneDrive - Safelite Group\Desktop\WFM\Qualtrics\Email Report\Compliance and Soft Skills By ...2026-06-02T18_37_07.242Z.xlsx"
}

# -----------------------------
# Ensure ImportExcel is available
# -----------------------------

function Ensure-ImportExcel {
    if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
        Write-Host "ImportExcel module not found." -ForegroundColor Yellow
        $answer = Read-Host "Install ImportExcel now? Type Y to install"

        if ($answer -match "^[Yy]$") {
            Install-Module ImportExcel -Scope CurrentUser -Force
        }
        else {
            throw "ImportExcel is required. Install it with: Install-Module ImportExcel -Scope CurrentUser"
        }
    }

    Import-Module ImportExcel -ErrorAction Stop
}

# -----------------------------
# Ask Y/N/U and resolve report file
# -----------------------------

function Get-ReportFileSelection {
    param(
        [string]$Question,
        [string]$ReportKey,
        [string]$FilePickerTitle
    )

    while ($true) {
        $answer = Read-Host "$Question Y/N/U"

        if ($answer -match "^[Nn]$") {
            return $null
        }

        if ($answer -match "^[Uu]$") {
            Write-Host "Opening file picker for $Question" -ForegroundColor Cyan
            return (Select-ExcelFile -Title $FilePickerTitle)
        }

        if ($answer -match "^[Yy]$") {
            $presetPath = ""

            if ($null -ne $PresetReportFiles -and $PresetReportFiles.ContainsKey($ReportKey)) {
                $presetPath = $PresetReportFiles[$ReportKey]
            }

            if (-not [string]::IsNullOrWhiteSpace($presetPath)) {
                if (Test-Path -LiteralPath $presetPath) {
                    Write-Host "Using preset file:" -ForegroundColor Green
                    Write-Host $presetPath -ForegroundColor Green
                    return $presetPath
                }

                Write-Host "Preset file was configured, but it was not found:" -ForegroundColor Yellow
                Write-Host $presetPath -ForegroundColor Yellow
                Write-Host "Switching to U mode so you can select the file manually." -ForegroundColor Yellow
                return (Select-ExcelFile -Title $FilePickerTitle)
            }

            Write-Host "No preset file is configured for this report. Switching to U mode." -ForegroundColor Yellow
            return (Select-ExcelFile -Title $FilePickerTitle)
        }

        Write-Host "Please type Y, N, or U." -ForegroundColor Yellow
        Write-Host "Y = use preset file when available, N = skip, U = select/upload a file." -ForegroundColor Yellow
    }
}

# -----------------------------
# File picker
# -----------------------------

function Select-ExcelFile {
    param(
        [string]$Title
    )

    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.Title = $Title
    $dialog.Filter = "Excel Files (*.xlsx;*.xlsm)|*.xlsx;*.xlsm|All Files (*.*)|*.*"
    $dialog.Multiselect = $false
    $dialog.InitialDirectory = [Environment]::GetFolderPath("Desktop")

    $result = $dialog.ShowDialog()

    if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
        return $dialog.FileName
    }

    throw "No file was selected for: $Title"
}

# -----------------------------
# Normalize headers
# -----------------------------

function Normalize-Header {
    param([string]$Header)

    if ($null -eq $Header) {
        return ""
    }

    return (
        $Header.ToString().
            Trim().
            ToLower().
            Replace(" ", "").
            Replace("_", "").
            Replace("-", "").
            Replace(".", "").
            Replace("%", "percent").
            Replace("#", "number")
    )
}

# -----------------------------
# Get value from row using possible headers
# -----------------------------

function Get-ValueByHeader {
    param(
        [object]$Row,
        [string[]]$PossibleNames
    )

    foreach ($possible in $PossibleNames) {
        $target = Normalize-Header $possible

        foreach ($prop in $Row.PSObject.Properties) {
            if ((Normalize-Header $prop.Name) -eq $target) {
                return $prop.Value
            }
        }
    }

    return $null
}

# -----------------------------
# Parse number or percent
# Handles:
#   10
#   10%
#   0.10
#   10.5%
# -----------------------------

function Convert-ToPercentNumber {
    param(
        [object]$Value
    )

    if ($null -eq $Value) {
        return $null
    }

    $text = $Value.ToString().Trim()

    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    $text = $text.Replace("%", "").Trim()

    $number = 0.0

    if (-not [double]::TryParse($text, [ref]$number)) {
        return $null
    }

    # If Excel stores 10% as 0.10, convert to 10
    if ($number -le 1 -and $number -ge 0) {
        return ($number * 100)
    }

    return $number
}

function Convert-ToNumber {
    param(
        [object]$Value
    )

    if ($null -eq $Value) {
        return $null
    }

    $text = $Value.ToString().Trim()

    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    $text = $text.Replace(",", "").Replace("%", "").Trim()

    $number = 0.0

    if (-not [double]::TryParse($text, [ref]$number)) {
        return $null
    }

    return $number
}

function Format-PercentForEmail {
    param(
        [object]$Value
    )

    $percent = Convert-ToPercentNumber $Value

    if ($null -eq $percent) {
        return ""
    }

    return ("{0:N1}%" -f $percent)
}

function Format-ScoreForEmail {
    param(
        [object]$Value
    )

    $score = Convert-ToNumber $Value

    if ($null -eq $score) {
        return ""
    }

    return ("{0:N1}" -f $score)
}

function Format-NumberForEmail {
    param(
        [object]$Value
    )

    $number = Convert-ToNumber $Value

    if ($null -eq $number) {
        return ""
    }

    return ("{0:N1}" -f $number)
}

# -----------------------------
# Escape text for safe HTML email
# -----------------------------

function Escape-Html {
    param(
        [object]$Value
    )

    if ($null -eq $Value) {
        return ""
    }

    $text = $Value.ToString()

    $text = $text.Replace("&", "&amp;")
    $text = $text.Replace("<", "&lt;")
    $text = $text.Replace(">", "&gt;")
    $text = $text.Replace('"', "&quot;")
    $text = $text.Replace("'", "&#39;")

    return $text
}

# -----------------------------
# Report header colors only
# -----------------------------

function Get-ReportHeaderColor {
    param(
        [string]$ReportKey
    )

    switch ($ReportKey) {
        "InsuranceCash" {
            return "#7F1D1D" # dark red
        }

        "TentativeWording" {
            return "#5B21B6" # purple
        }

        "CallDisconnects" {
            return "#C2410C" # orange
        }

        "SaveTheSale" {
            return "#166534" # green
        }

        "RapportUtilization" {
            return "#0E7490" # teal / blue
        }

        "LowScores" {
            return "#BE123C" # rose / crimson
        }

        default {
            return "#1F2937" # dark gray
        }
    }
}

# -----------------------------
# Split "First Last"
# -----------------------------

function Split-FirstLast {
    param([string]$FullName)

    if ([string]::IsNullOrWhiteSpace($FullName)) {
        return $null
    }

    $clean = ($FullName -replace "\s+", " ").Trim()
    $parts = $clean.Split(" ")

    if ($parts.Count -lt 2) {
        return $null
    }

    $firstName = $parts[0].Trim()
    $lastName = $parts[$parts.Count - 1].Trim()

    return [PSCustomObject]@{
        FirstName = $firstName
        LastName  = $lastName
        FullName  = $clean
    }
}

# -----------------------------
# Split "Last, First"
# -----------------------------

function Split-LastCommaFirst {
    param([string]$FullName)

    if ([string]::IsNullOrWhiteSpace($FullName)) {
        return $null
    }

    $clean = ($FullName -replace "\s+", " ").Trim()

    if ($clean -notmatch ",") {
        return Split-FirstLast $clean
    }

    $parts = $clean.Split(",", 2)

    $lastName = $parts[0].Trim()
    $firstSide = $parts[1].Trim()

    if ([string]::IsNullOrWhiteSpace($lastName) -or [string]::IsNullOrWhiteSpace($firstSide)) {
        return $null
    }

    $firstName = $firstSide.Split(" ")[0].Trim()

    return [PSCustomObject]@{
        FirstName = $firstName
        LastName  = $lastName
        FullName  = "$firstName $lastName"
    }
}

# -----------------------------
# Build name key
# -----------------------------

function Get-NameKey {
    param(
        [string]$FirstName,
        [string]$LastName
    )

    if ([string]::IsNullOrWhiteSpace($FirstName) -or [string]::IsNullOrWhiteSpace($LastName)) {
        return $null
    }

    $first = $FirstName.Trim().ToLower()
    $last = $LastName.Trim().ToLower()

    return "$last|$first"
}

# -----------------------------
# Find header row by exact first columns
# -----------------------------

function Find-HeaderRowByFirstColumns {
    param(
        [object]$Worksheet,
        [string[]]$ExpectedHeaders
    )

    $rowCount = $Worksheet.Dimension.Rows

    for ($row = 1; $row -le $rowCount; $row++) {
        $matches = $true

        for ($i = 0; $i -lt $ExpectedHeaders.Count; $i++) {
            $col = $i + 1
            $cellText = $Worksheet.Cells[$row, $col].Text.Trim()

            if ($cellText -ne $ExpectedHeaders[$i]) {
                $matches = $false
                break
            }
        }

        if ($matches) {
            return $row
        }
    }

    return $null
}

# -----------------------------
# Find header row by required headers anywhere in row
# -----------------------------

function Find-HeaderRowAndMap {
    param(
        [object]$Worksheet,
        [string[]]$RequiredHeaders
    )

    $rowCount = $Worksheet.Dimension.Rows
    $colCount = $Worksheet.Dimension.Columns

    for ($row = 1; $row -le $rowCount; $row++) {
        $map = @{}

        for ($col = 1; $col -le $colCount; $col++) {
            $text = $Worksheet.Cells[$row, $col].Text.Trim()

            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $norm = Normalize-Header $text

                if (-not $map.ContainsKey($norm)) {
                    $map[$norm] = $col
                }
            }
        }

        $allFound = $true

        foreach ($header in $RequiredHeaders) {
            $normHeader = Normalize-Header $header

            if (-not $map.ContainsKey($normHeader)) {
                $allFound = $false
                break
            }
        }

        if ($allFound) {
            return [PSCustomObject]@{
                Row = $row
                Map = $map
            }
        }
    }

    return $null
}

# -----------------------------
# Read Master Leader Lookup
# -----------------------------

function Read-LeaderLookup {
    param(
        [string]$Path
    )

    Write-Host ""
    Write-Host "Reading Master Leader Lookup file..." -ForegroundColor Cyan

    $rows = Import-Excel -Path $Path

    if ($null -eq $rows -or $rows.Count -eq 0) {
        throw "Master Leader Lookup file appears to be empty."
    }

    $peopleByName = @{}

    foreach ($row in $rows) {
        $firstName = Get-ValueByHeader $row @(
            "First Name",
            "FirstName",
            "First",
            "Given Name",
            "GivenName"
        )

        $lastName = Get-ValueByHeader $row @(
            "Last Name",
            "LastName",
            "Last",
            "Surname"
        )

        $supervisor = Get-ValueByHeader $row @(
            "Supervisor",
            "Supervisor Name",
            "Manager",
            "Manager Name",
            "Leader"
        )

        $username = Get-ValueByHeader $row @(
            "Username",
            "User Name",
            "Email",
            "Email Address",
            "UserPrincipalName",
            "UPN"
        )

        if ([string]::IsNullOrWhiteSpace($firstName) -or [string]::IsNullOrWhiteSpace($lastName)) {
            continue
        }

        $key = Get-NameKey -FirstName $firstName -LastName $lastName

        if ([string]::IsNullOrWhiteSpace($key)) {
            continue
        }

        $person = [PSCustomObject]@{
            FirstName  = $firstName.ToString().Trim()
            LastName   = $lastName.ToString().Trim()
            FullName   = "$($firstName.ToString().Trim()) $($lastName.ToString().Trim())"
            Supervisor = if ($null -ne $supervisor) { $supervisor.ToString().Trim() } else { "" }
            Username   = if ($null -ne $username) { $username.ToString().Trim() } else { "" }
        }

        if (-not $peopleByName.ContainsKey($key)) {
            $peopleByName[$key] = $person
        }
    }

    Write-Host "Loaded $($peopleByName.Count) people from Master Leader Lookup file." -ForegroundColor Green

    return $peopleByName
}

# -----------------------------
# Resolve rep to supervisor and supervisor manager
# -----------------------------

function Resolve-RepRouting {
    param(
        [string]$AgentName,
        [hashtable]$PeopleByName
    )

    $splitRep = Split-FirstLast $AgentName

    if ($null -eq $splitRep) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - could not split First Last name"
        }
    }

    $repKey = Get-NameKey -FirstName $splitRep.FirstName -LastName $splitRep.LastName

    if (-not $PeopleByName.ContainsKey($repKey)) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - not found in master lookup file"
        }
    }

    $repLookup = $PeopleByName[$repKey]

    if ([string]::IsNullOrWhiteSpace($repLookup.Supervisor)) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - no supervisor listed"
        }
    }

    $supervisorParts = Split-LastCommaFirst $repLookup.Supervisor

    if ($null -eq $supervisorParts) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - could not parse supervisor: $($repLookup.Supervisor)"
        }
    }

    $supervisorKey = Get-NameKey -FirstName $supervisorParts.FirstName -LastName $supervisorParts.LastName

    if (-not $PeopleByName.ContainsKey($supervisorKey)) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - supervisor not found in lookup: $($repLookup.Supervisor)"
        }
    }

    $supervisor = $PeopleByName[$supervisorKey]

    if ([string]::IsNullOrWhiteSpace($supervisor.Username)) {
        return [PSCustomObject]@{
            Success = $false
            Error = "$AgentName - supervisor username/email missing for $($supervisor.FullName)"
        }
    }

    $managerEmail = ""
    $managerName = ""

    if (-not [string]::IsNullOrWhiteSpace($supervisor.Supervisor)) {
        $managerParts = Split-LastCommaFirst $supervisor.Supervisor

        if ($null -ne $managerParts) {
            $managerKey = Get-NameKey -FirstName $managerParts.FirstName -LastName $managerParts.LastName

            if ($PeopleByName.ContainsKey($managerKey)) {
                $manager = $PeopleByName[$managerKey]
                $managerName = $manager.FullName

                if (-not [string]::IsNullOrWhiteSpace($manager.Username)) {
                    $managerEmail = $manager.Username
                }
            }
        }
    }

    return [PSCustomObject]@{
        Success         = $true
        AgentName       = $AgentName
        SupervisorName  = $supervisor.FullName
        SupervisorEmail = $supervisor.Username
        ManagerName     = $managerName
        ManagerEmail    = $managerEmail
        Error           = ""
    }
}

# -----------------------------
# Ensure email group exists
# -----------------------------

function Ensure-EmailGroup {
    param(
        [hashtable]$Groups,
        [object]$Routing
    )

    $groupKey = $Routing.SupervisorEmail.ToLower()

    if (-not $Groups.ContainsKey($groupKey)) {
        $Groups[$groupKey] = [PSCustomObject]@{
            SupervisorName  = $Routing.SupervisorName
            SupervisorEmail = $Routing.SupervisorEmail
            ManagerName     = $Routing.ManagerName
            ManagerEmail    = $Routing.ManagerEmail
            Reports         = @{}
        }
    }

    return $groupKey
}

# -----------------------------
# Add report item to email groups
# -----------------------------

function Add-ReportItemToGroups {
    param(
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [string]$AgentName,
        [string]$ReportKey,
        [string]$ReportTitle,
        [string]$ReportIntro,
        [string]$DetailLine,
        [System.Collections.ArrayList]$Issues
    )

    $routing = Resolve-RepRouting -AgentName $AgentName -PeopleByName $PeopleByName

    if (-not $routing.Success) {
        [void]$Issues.Add($routing.Error)
        return
    }

    $groupKey = Ensure-EmailGroup -Groups $Groups -Routing $routing

    if (-not $Groups[$groupKey].Reports.ContainsKey($ReportKey)) {
        $Groups[$groupKey].Reports[$ReportKey] = [PSCustomObject]@{
            Title = $ReportTitle
            Intro = $ReportIntro
            Lines = New-Object System.Collections.ArrayList
            HtmlBlocks = New-Object System.Collections.ArrayList
        }
    }

    [void]$Groups[$groupKey].Reports[$ReportKey].Lines.Add($DetailLine)
}

# -----------------------------
# Add HTML block to email groups
# Used for reports that need tables.
# -----------------------------

function Add-ReportHtmlBlockToGroups {
    param(
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [string]$AgentName,
        [string]$ReportKey,
        [string]$ReportTitle,
        [string]$ReportIntro,
        [string]$HtmlBlock,
        [System.Collections.ArrayList]$Issues
    )

    $routing = Resolve-RepRouting -AgentName $AgentName -PeopleByName $PeopleByName

    if (-not $routing.Success) {
        [void]$Issues.Add($routing.Error)
        return
    }

    $groupKey = Ensure-EmailGroup -Groups $Groups -Routing $routing

    if (-not $Groups[$groupKey].Reports.ContainsKey($ReportKey)) {
        $Groups[$groupKey].Reports[$ReportKey] = [PSCustomObject]@{
            Title = $ReportTitle
            Intro = $ReportIntro
            Lines = New-Object System.Collections.ArrayList
            HtmlBlocks = New-Object System.Collections.ArrayList
        }
    }

    [void]$Groups[$groupKey].Reports[$ReportKey].HtmlBlocks.Add($HtmlBlock)
}

# -----------------------------
# Process Insurance Cash Utilization
# -----------------------------

function Process-InsuranceCashReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Insurance Cash Utilization report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Insurance Cash file."
    }

    $headerRow = Find-HeaderRowByFirstColumns -Worksheet $worksheet -ExpectedHeaders @(
        "Agent Name",
        "Volume",
        "Anything But Insurance Cash"
    )

    if ($null -eq $headerRow) {
        Close-ExcelPackage $package
        throw "Could not find Insurance Cash header row: Agent Name | Volume | Anything But Insurance Cash"
    }

    Write-Host "Found Insurance Cash header row at row $headerRow." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $count = 0

    for ($row = $headerRow + 1; $row -le $rowCount; $row++) {
        $agentName = $worksheet.Cells[$row, 1].Text.Trim()
        $volume = $worksheet.Cells[$row, 2].Text.Trim()
        $anythingButInsuranceCash = $worksheet.Cells[$row, 3].Text.Trim()

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        $detail = "$agentName - Volume: $volume, Anything But Insurance Cash: $anythingButInsuranceCash"

        Add-ReportItemToGroups `
            -Groups $Groups `
            -PeopleByName $PeopleByName `
            -AgentName $agentName `
            -ReportKey "InsuranceCash" `
            -ReportTitle "Insurance Cash Utilization" `
            -ReportIntro "The following Representative(s) did not consistently utilize Insurance Cash despite using other Save the Sale methods. Please investigate and coach or report back if these findings are not accurate." `
            -DetailLine $detail `
            -Issues $Issues

        $count++

        if ($count -ge $TopInsuranceCashCount) {
            break
        }
    }

    Close-ExcelPackage $package

    Write-Host "Insurance Cash representatives added: $count" -ForegroundColor Green
}

# -----------------------------
# Process Tentative Wording Report
# -----------------------------

function Process-TentativeWordingReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Tentative Wording report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Tentative Wording file."
    }

    $headerRow = Find-HeaderRowByFirstColumns -Worksheet $worksheet -ExpectedHeaders @(
        "Language Patterns",
        "Agent Name",
        "Volume"
    )

    if ($null -eq $headerRow) {
        Close-ExcelPackage $package
        throw "Could not find Tentative Wording header row: Language Patterns | Agent Name | Volume"
    }

    Write-Host "Found Tentative Wording header row at row $headerRow." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $count = 0

    for ($row = $headerRow + 1; $row -le $rowCount; $row++) {
        $languagePattern = $worksheet.Cells[$row, 1].Text.Trim()
        $agentName = $worksheet.Cells[$row, 2].Text.Trim()
        $volumeText = $worksheet.Cells[$row, 3].Text.Trim()
        $volumeNumber = Convert-ToNumber $volumeText

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        if ($null -eq $volumeNumber) {
            continue
        }

        if ($volumeNumber -lt 2) {
            continue
        }

        $detail = "$agentName - Language Pattern: $languagePattern, Volume: $volumeText"

        Add-ReportItemToGroups `
            -Groups $Groups `
            -PeopleByName $PeopleByName `
            -AgentName $agentName `
            -ReportKey "TentativeWording" `
            -ReportTitle "Tentative Language" `
            -ReportIntro "The following representative(s) utilized tentative language. Please reference the coach dashboard in Qualtrics to find the data." `
            -DetailLine $detail `
            -Issues $Issues

        $count++
    }

    Close-ExcelPackage $package

    Write-Host "Tentative Wording representatives added: $count" -ForegroundColor Green
}

# -----------------------------
# Process Call Disconnects Report
# -----------------------------

function Process-CallDisconnectsReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Call Disconnects report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Call Disconnects file."
    }

    $headerInfo = Find-HeaderRowAndMap -Worksheet $worksheet -RequiredHeaders @(
        "Agent Name",
        "ConversationId",
        "Disconnect Type",
        "Filtered",
        "hold time",
        "duration_s"
    )

    if ($null -eq $headerInfo) {
        Close-ExcelPackage $package
        throw "Could not find Call Disconnects headers: Agent Name, ConversationId, Disconnect Type, Filtered, hold time, duration_s"
    }

    Write-Host "Found Call Disconnects header row at row $($headerInfo.Row)." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $map = $headerInfo.Map
    $count = 0

    $agentCol = $map[(Normalize-Header "Agent Name")]
    $conversationCol = $map[(Normalize-Header "ConversationId")]
    $disconnectTypeCol = $map[(Normalize-Header "Disconnect Type")]
    $filteredCol = $map[(Normalize-Header "Filtered")]
    $holdTimeCol = $map[(Normalize-Header "hold time")]
    $durationCol = $map[(Normalize-Header "duration_s")]

    for ($row = $headerInfo.Row + 1; $row -le $rowCount; $row++) {
        $agentName = $worksheet.Cells[$row, $agentCol].Text.Trim()

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        $conversationId = $worksheet.Cells[$row, $conversationCol].Text.Trim()
        $disconnectType = $worksheet.Cells[$row, $disconnectTypeCol].Text.Trim()
        $filtered = $worksheet.Cells[$row, $filteredCol].Text.Trim()
        $holdTime = $worksheet.Cells[$row, $holdTimeCol].Text.Trim()
        $duration = $worksheet.Cells[$row, $durationCol].Text.Trim()

        $detail = "$agentName - ConversationId: $conversationId, Disconnect Type: $disconnectType, Filtered: $filtered, Hold Time: $holdTime, Duration_s: $duration"

        Add-ReportItemToGroups `
            -Groups $Groups `
            -PeopleByName $PeopleByName `
            -AgentName $agentName `
            -ReportKey "CallDisconnects" `
            -ReportTitle "Call Disconnects" `
            -ReportIntro "The following representative(s) disconnected calls early without communicating to the customer." `
            -DetailLine $detail `
            -Issues $Issues

        $count++
    }

    Close-ExcelPackage $package

    Write-Host "Call Disconnect rows added: $count" -ForegroundColor Green
}

# -----------------------------
# Process Save the Sale Usage Report
# -----------------------------

function Process-SaveTheSaleReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Save the Sale Usage report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Save the Sale file."
    }

    $headerInfo = Find-HeaderRowAndMap -Worksheet $worksheet -RequiredHeaders @(
        "Agent Name",
        "Cash Scheduled %",
        "Volume",
        "% of usage of Save the Sale"
    )

    if ($null -eq $headerInfo) {
        Close-ExcelPackage $package
        throw "Could not find Save the Sale headers: Agent Name, Cash Scheduled %, Volume, % of usage of Save the Sale"
    }

    Write-Host "Found Save the Sale header row at row $($headerInfo.Row)." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $map = $headerInfo.Map
    $count = 0

    $agentCol = $map[(Normalize-Header "Agent Name")]
    $cashScheduledCol = $map[(Normalize-Header "Cash Scheduled %")]
    $volumeCol = $map[(Normalize-Header "Volume")]
    $usageCol = $map[(Normalize-Header "% of usage of Save the Sale")]

    for ($row = $headerInfo.Row + 1; $row -le $rowCount; $row++) {
        $agentName = $worksheet.Cells[$row, $agentCol].Text.Trim()

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        $cashScheduledText = $worksheet.Cells[$row, $cashScheduledCol].Text.Trim()
        $volumeText = $worksheet.Cells[$row, $volumeCol].Text.Trim()
        $usageText = $worksheet.Cells[$row, $usageCol].Text.Trim()

        $cashScheduledPercent = Convert-ToPercentNumber $cashScheduledText
        $usagePercent = Convert-ToPercentNumber $usageText

        if ($null -eq $cashScheduledPercent -or $null -eq $usagePercent) {
            continue
        }

        if ($usagePercent -lt 10 -and $cashScheduledPercent -lt 45) {
            $cashScheduledDisplay = Format-PercentForEmail $cashScheduledText
            $usageDisplay = Format-PercentForEmail $usageText

            $detail = "$agentName - Cash Scheduled %: $cashScheduledDisplay, Save the Sale Usage %: $usageDisplay, Volume: $volumeText"

            Add-ReportItemToGroups `
                -Groups $Groups `
                -PeopleByName $PeopleByName `
                -AgentName $agentName `
                -ReportKey "SaveTheSale" `
                -ReportTitle "Save the Sale Usage" `
                -ReportIntro "The following Representative(s) had low performance in Cash and underutilized Save the Sale methods." `
                -DetailLine $detail `
                -Issues $Issues

            $count++
        }
    }

    Close-ExcelPackage $package

    Write-Host "Save the Sale representatives added: $count" -ForegroundColor Green
}

# -----------------------------
# Process Rapport Utilization Report
# -----------------------------

function Process-RapportUtilizationReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Rapport Utilization report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Rapport Utilization file."
    }

    $headerInfo = Find-HeaderRowAndMap -Worksheet $worksheet -RequiredHeaders @(
        "Agent Name",
        "Volume",
        "Cash Scheduled %",
        "Build Rapport %"
    )

    if ($null -eq $headerInfo) {
        Close-ExcelPackage $package
        throw "Could not find Rapport Utilization headers: Agent Name, Volume, Cash Scheduled %, Build Rapport %"
    }

    Write-Host "Found Rapport Utilization header row at row $($headerInfo.Row)." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $map = $headerInfo.Map
    $count = 0

    $agentCol = $map[(Normalize-Header "Agent Name")]
    $volumeCol = $map[(Normalize-Header "Volume")]
    $cashScheduledCol = $map[(Normalize-Header "Cash Scheduled %")]
    $buildRapportCol = $map[(Normalize-Header "Build Rapport %")]

    for ($row = $headerInfo.Row + 1; $row -le $rowCount; $row++) {
        $agentName = $worksheet.Cells[$row, $agentCol].Text.Trim()

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        $volumeText = $worksheet.Cells[$row, $volumeCol].Text.Trim()
        $cashScheduledText = $worksheet.Cells[$row, $cashScheduledCol].Text.Trim()
        $buildRapportText = $worksheet.Cells[$row, $buildRapportCol].Text.Trim()

        $cashScheduledPercent = Convert-ToPercentNumber $cashScheduledText
        $buildRapportPercent = Convert-ToPercentNumber $buildRapportText

        if ($null -eq $cashScheduledPercent -or $null -eq $buildRapportPercent) {
            continue
        }

        if ($cashScheduledPercent -lt 45 -and $buildRapportPercent -lt 25) {
            $cashScheduledDisplay = Format-PercentForEmail $cashScheduledText
            $buildRapportDisplay = Format-PercentForEmail $buildRapportText

            $detail = "$agentName - Cash Scheduled %: $cashScheduledDisplay, Build Rapport %: $buildRapportDisplay, Volume: $volumeText"

            Add-ReportItemToGroups `
                -Groups $Groups `
                -PeopleByName $PeopleByName `
                -AgentName $agentName `
                -ReportKey "RapportUtilization" `
                -ReportTitle "Rapport Utilization" `
                -ReportIntro "The following Representative(s) had low Scheduling and Low Rapport building. Please coach on Rapport building behaviors." `
                -DetailLine $detail `
                -Issues $Issues

            $count++
        }
    }

    Close-ExcelPackage $package

    Write-Host "Rapport Utilization representatives added: $count" -ForegroundColor Green
}

# -----------------------------
# Build HTML table
# -----------------------------

function New-LowScoresHtmlTable {
    param(
        [string]$Title,
        [array]$Rows
    )

    if ($null -eq $Rows -or $Rows.Count -eq 0) {
        return ""
    }

    $safeTitle = Escape-Html $Title

    $html = ""
    $html += "<h4 style='margin:14px 0 6px 0;'>$safeTitle</h4>"
    $html += "<table style='border-collapse:collapse;width:100%;font-size:13px;margin-bottom:14px;'>"
    $html += "<tr>"
    $html += "<th style='border:1px solid #D1D5DB;padding:6px;background-color:#F3F4F6;text-align:left;'>Representative</th>"
    $html += "<th style='border:1px solid #D1D5DB;padding:6px;background-color:#F3F4F6;text-align:left;'>Retail Soft Skills Score</th>"
    $html += "<th style='border:1px solid #D1D5DB;padding:6px;background-color:#F3F4F6;text-align:left;'>Retail Compliance Score</th>"
    $html += "</tr>"

    foreach ($row in $Rows) {
        $agent = Escape-Html $row.AgentName
        $soft = Escape-Html $row.SoftDisplay
        $compliance = Escape-Html $row.ComplianceDisplay

        $html += "<tr>"
        $html += "<td style='border:1px solid #D1D5DB;padding:6px;'>$agent</td>"
        $html += "<td style='border:1px solid #D1D5DB;padding:6px;'>$soft</td>"
        $html += "<td style='border:1px solid #D1D5DB;padding:6px;'>$compliance</td>"
        $html += "</tr>"
    }

    $html += "</table>"

    return $html
}

# -----------------------------
# Process Low Compliance / Low Soft Skills Report
# -----------------------------

function Process-LowScoresReport {
    param(
        [string]$Path,
        [hashtable]$Groups,
        [hashtable]$PeopleByName,
        [System.Collections.ArrayList]$Issues
    )

    Write-Host ""
    Write-Host "Processing Low Compliance / Low Soft Skills Scores report..." -ForegroundColor Cyan

    $package = Open-ExcelPackage -Path $Path
    $worksheet = $package.Workbook.Worksheets[1]

    if ($null -eq $worksheet) {
        Close-ExcelPackage $package
        throw "Could not find a worksheet in the Low Compliance / Low Soft Skills file."
    }

    $headerInfo = Find-HeaderRowAndMap -Worksheet $worksheet -RequiredHeaders @(
        "Agent Name",
        "Retail Soft Skills Score",
        "Retail Compliance Score"
    )

    if ($null -eq $headerInfo) {
        Close-ExcelPackage $package
        throw "Could not find Low Scores headers: Agent Name, Retail Soft Skills Score, Retail Compliance Score"
    }

    Write-Host "Found Low Scores header row at row $($headerInfo.Row)." -ForegroundColor Green

    $rowCount = $worksheet.Dimension.Rows
    $map = $headerInfo.Map

    $agentCol = $map[(Normalize-Header "Agent Name")]
    $softCol = $map[(Normalize-Header "Retail Soft Skills Score")]
    $complianceCol = $map[(Normalize-Header "Retail Compliance Score")]

    $rows = @()

    for ($row = $headerInfo.Row + 1; $row -le $rowCount; $row++) {
        $agentName = $worksheet.Cells[$row, $agentCol].Text.Trim()

        if ([string]::IsNullOrWhiteSpace($agentName)) {
            continue
        }

        $softText = $worksheet.Cells[$row, $softCol].Text.Trim()
        $complianceText = $worksheet.Cells[$row, $complianceCol].Text.Trim()

        $softScore = Convert-ToNumber $softText
        $complianceScore = Convert-ToNumber $complianceText

        if ($null -eq $softScore -or $null -eq $complianceScore) {
            continue
        }

        $rows += [PSCustomObject]@{
            AgentName = $agentName
            SoftScore = $softScore
            ComplianceScore = $complianceScore
            SoftDisplay = Format-ScoreForEmail $softText
            ComplianceDisplay = Format-ScoreForEmail $complianceText
        }
    }

    Close-ExcelPackage $package

    if ($rows.Count -eq 0) {
        Write-Host "No valid rows found in Low Scores report." -ForegroundColor Yellow
        return
    }

    $softAverage = ($rows | Measure-Object -Property SoftScore -Average).Average
    $complianceAverage = ($rows | Measure-Object -Property ComplianceScore -Average).Average

    $bottomCount = [Math]::Ceiling($rows.Count / 3)

    $bottomSoftNames = @{}
    $bottomComplianceNames = @{}

    $rows |
        Sort-Object SoftScore |
        Select-Object -First $bottomCount |
        ForEach-Object {
            $bottomSoftNames[$_.AgentName.ToLower()] = $true
        }

    $rows |
        Sort-Object ComplianceScore |
        Select-Object -First $bottomCount |
        ForEach-Object {
            $bottomComplianceNames[$_.AgentName.ToLower()] = $true
        }

    $bothRows = @()
    $softOnlyRows = @()
    $complianceOnlyRows = @()

    foreach ($row in $rows) {
        $key = $row.AgentName.ToLower()
        $inSoft = $bottomSoftNames.ContainsKey($key)
        $inCompliance = $bottomComplianceNames.ContainsKey($key)

        if ($inSoft -and $inCompliance) {
            $bothRows += $row
        }
        elseif ($inSoft) {
            $softOnlyRows += $row
        }
        elseif ($inCompliance) {
            $complianceOnlyRows += $row
        }
    }

    $allFlagged = @()
    $allFlagged += $bothRows
    $allFlagged += $softOnlyRows
    $allFlagged += $complianceOnlyRows

    if ($allFlagged.Count -eq 0) {
        Write-Host "No representatives fell into the bottom third for Soft Skills or Compliance." -ForegroundColor Yellow
        return
    }

    $reportTitle = "Low Compliance / Low Soft Skills Scores"
    $reportIntro = "The following representative(s) had low Soft Skills Scores or Low Compliance Scores or both. Please review the calls and see if they were accurate, then please coach or provide feedback on the inaccuracies. Please cite specific phrases if possible for where they were mis-scored."

    foreach ($rep in $allFlagged) {
        $routing = Resolve-RepRouting -AgentName $rep.AgentName -PeopleByName $PeopleByName

        if (-not $routing.Success) {
            [void]$Issues.Add($routing.Error)
            continue
        }

        $groupKey = Ensure-EmailGroup -Groups $Groups -Routing $routing

        if (-not $Groups[$groupKey].Reports.ContainsKey("LowScores")) {
            $Groups[$groupKey].Reports["LowScores"] = [PSCustomObject]@{
                Title = $reportTitle
                Intro = $reportIntro
                Lines = New-Object System.Collections.ArrayList
                HtmlBlocks = New-Object System.Collections.ArrayList
                BothRows = New-Object System.Collections.ArrayList
                SoftOnlyRows = New-Object System.Collections.ArrayList
                ComplianceOnlyRows = New-Object System.Collections.ArrayList
                SoftAverage = $softAverage
                ComplianceAverage = $complianceAverage
            }
        }

        $repKey = $rep.AgentName.ToLower()
        $inSoft = $bottomSoftNames.ContainsKey($repKey)
        $inCompliance = $bottomComplianceNames.ContainsKey($repKey)

        if ($inSoft -and $inCompliance) {
            [void]$Groups[$groupKey].Reports["LowScores"].BothRows.Add($rep)
        }
        elseif ($inSoft) {
            [void]$Groups[$groupKey].Reports["LowScores"].SoftOnlyRows.Add($rep)
        }
        elseif ($inCompliance) {
            [void]$Groups[$groupKey].Reports["LowScores"].ComplianceOnlyRows.Add($rep)
        }
    }

    Write-Host "Low Scores rows found: $($allFlagged.Count)" -ForegroundColor Green
    Write-Host ("Soft Skills average: {0:N1}" -f $softAverage) -ForegroundColor Green
    Write-Host ("Compliance average: {0:N1}" -f $complianceAverage) -ForegroundColor Green
}

# -----------------------------
# Open Outlook email windows
# -----------------------------

function Open-OutlookEmails {
    param(
        [hashtable]$Groups
    )

    Write-Host ""
    Write-Host "Opening Outlook email windows..." -ForegroundColor Cyan
    Write-Host "These emails will NOT be sent automatically." -ForegroundColor Yellow
    Write-Host "Review each email, then manually click Send if it looks correct." -ForegroundColor Yellow

    try {
        $outlook = New-Object -ComObject Outlook.Application
    }
    catch {
        throw "Could not start Outlook. Make sure Outlook desktop is installed and configured."
    }

    $emailCount = 0

    foreach ($key in $Groups.Keys) {
        $group = $Groups[$key]

        $htmlBody = @"
<html>
<head>
<style>
    body {
        font-family: Arial, sans-serif;
        color: #111827;
        background-color: #FFFFFF;
        font-size: 14px;
    }

    .container {
        max-width: 900px;
    }

    .intro {
        margin-bottom: 18px;
        line-height: 1.5;
    }

    .instructions {
        background-color: #F9FAFB;
        border: 1px solid #E5E7EB;
        border-radius: 8px;
        padding: 12px 16px;
    }

    a {
        color: #1D4ED8;
        text-decoration: underline;
    }

    .report-card {
        border: 1px solid #D1D5DB;
        border-radius: 8px;
        margin: 18px 0;
        overflow: hidden;
        background-color: #FFFFFF;
    }

    .report-header {
        color: white;
        font-size: 17px;
        font-weight: bold;
        padding: 10px 14px;
    }

    .report-body {
        padding: 12px 16px;
        background-color: #FFFFFF;
        color: #111827;
        line-height: 1.45;
    }

    .report-intro {
        margin-bottom: 10px;
        font-weight: bold;
    }

    ul {
        margin-top: 8px;
        margin-bottom: 4px;
    }

    li {
        margin-bottom: 7px;
    }

    .footer {
        margin-top: 22px;
    }
</style>
</head>
<body>
<div class="container">
    <p>Hello,</p>

    <p class="intro">
        Please review the coaching follow-up items below and fill out this form:
        <a href="https://fakeform.com" target="_blank">Fakeform.com</a>
    </p>

    <div class="instructions" style="margin: 0 0 24px 0; line-height: 1.6;">
        <p style="margin: 0 0 10px 0;">
            <strong>1:</strong> Go to
            <a href="https://cxstudio.clarabridge.net/dashboard/#/dashboards" target="_blank">
                https://cxstudio.clarabridge.net/dashboard/#/dashboards
            </a>
        </p>

        <p style="margin: 0 0 10px 0;">
            <strong>2:</strong> Log in and scroll down until you see <strong>Explore Dashboards &amp; Books</strong>.
        </p>

        <p style="margin: 0 0 10px 0;">
            <strong>3:</strong> Search <strong>Coach Dashboard</strong>.
        </p>

        <p style="margin: 0 0 10px 0;">
            <strong>4:</strong> You can filter by your team by selecting your name from the <strong>Coach Name</strong> drop down.
        </p>

        <p style="margin: 0 0 18px 0;">
            <strong>5:</strong> Find the relevant tables/graphs to the information provided below. You can click on your representative’s name and click <strong>Open Document Explorer</strong> to look into the call. The call’s <strong>ConversationId</strong> will be on the right side bar, under <strong>Attributes</strong>, should you need it.
        </p>
    </div>
"@

        foreach ($reportKey in $group.Reports.Keys) {
            $report = $group.Reports[$reportKey]
            $headerColor = Get-ReportHeaderColor $reportKey

            $title = Escape-Html $report.Title
            $intro = Escape-Html $report.Intro

            $htmlBody += @"
    <div class="report-card">
        <div class="report-header" style="background-color: $headerColor;">
            $title
        </div>
        <div class="report-body">
            <div class="report-intro">$intro</div>
"@

            if ($reportKey -eq "LowScores") {
                $softAverageDisplay = ("{0:N1}" -f $report.SoftAverage)
                $complianceAverageDisplay = ("{0:N1}" -f $report.ComplianceAverage)

                $htmlBody += "<p><strong>Overall Soft Skills Average:</strong> $softAverageDisplay<br>"
                $htmlBody += "<strong>Overall Compliance Average:</strong> $complianceAverageDisplay</p>"

                $htmlBody += New-LowScoresHtmlTable -Title "Bottom Third in Both Soft Skills and Compliance" -Rows $report.BothRows
                $htmlBody += New-LowScoresHtmlTable -Title "Bottom Third in Soft Skills Only" -Rows $report.SoftOnlyRows
                $htmlBody += New-LowScoresHtmlTable -Title "Bottom Third in Compliance Only" -Rows $report.ComplianceOnlyRows
            }
            else {
                if ($report.Lines.Count -gt 0) {
                    $htmlBody += "<ul>`r`n"

                    foreach ($line in $report.Lines) {
                        $safeLine = Escape-Html $line
                        $htmlBody += "                <li>$safeLine</li>`r`n"
                    }

                    $htmlBody += "</ul>`r`n"
                }

                if ($null -ne $report.HtmlBlocks -and $report.HtmlBlocks.Count -gt 0) {
                    foreach ($block in $report.HtmlBlocks) {
                        $htmlBody += $block
                    }
                }
            }

            $htmlBody += @"
        </div>
    </div>
"@
        }

        $htmlBody += @"
    <p class="footer">Thank you,</p>
</div>
</body>
</html>
"@

        $mail = $outlook.CreateItem(0)

        $mail.To = $group.SupervisorEmail

        if (-not [string]::IsNullOrWhiteSpace($group.ManagerEmail)) {
            $mail.CC = $group.ManagerEmail
        }

        $mail.Subject = $EmailSubject

        # Use HTMLBody so the section headers and score tables can display cleanly.
        $mail.HTMLBody = $htmlBody

        # Opens the email on screen.
        # Does NOT send.
        # Does NOT intentionally save as a draft.
        $mail.Display()

        $emailCount++

        Write-Host ("Opened email window for " + $group.SupervisorName + " - " + $group.SupervisorEmail + " with " + $group.Reports.Count + " report section(s).") -ForegroundColor Green

        Start-Sleep -Milliseconds 400
    }

    return $emailCount
}

# -----------------------------
# Main
# -----------------------------

try {
    Ensure-ImportExcel

    $groups = @{}
    $issues = New-Object System.Collections.ArrayList

    Write-Host ""
    Write-Host "Select the Master Leader Lookup Excel file." -ForegroundColor Cyan
    $leaderFile = Select-ExcelFile -Title "Select Master Leader Lookup Excel File"
    $peopleByName = Read-LeaderLookup -Path $leaderFile

    Write-Host ""
    $insuranceCashFile = Get-ReportFileSelection `
        -Question "Will you be sending out Insurance Cash Utilization stats?" `
        -ReportKey "InsuranceCash" `
        -FilePickerTitle "Select Insurance Cash Utilization Excel File"

    if (-not [string]::IsNullOrWhiteSpace($insuranceCashFile)) {
        Process-InsuranceCashReport `
            -Path $insuranceCashFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    Write-Host ""
    $tentativeFile = Get-ReportFileSelection `
        -Question "Will you be sending out Tentative Wording stats?" `
        -ReportKey "TentativeWording" `
        -FilePickerTitle "Select Tentative Wording Excel File"

    if (-not [string]::IsNullOrWhiteSpace($tentativeFile)) {
        Process-TentativeWordingReport `
            -Path $tentativeFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    Write-Host ""
    $disconnectFile = Get-ReportFileSelection `
        -Question "Will you be sending out Call Disconnect stats?" `
        -ReportKey "CallDisconnects" `
        -FilePickerTitle "Select Call Disconnects Excel File"

    if (-not [string]::IsNullOrWhiteSpace($disconnectFile)) {
        Process-CallDisconnectsReport `
            -Path $disconnectFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    Write-Host ""
    $saveTheSaleFile = Get-ReportFileSelection `
        -Question "Will you be sending out Usage of Save the Sale stats?" `
        -ReportKey "SaveTheSale" `
        -FilePickerTitle "Select Save the Sale Usage Excel File"

    if (-not [string]::IsNullOrWhiteSpace($saveTheSaleFile)) {
        Process-SaveTheSaleReport `
            -Path $saveTheSaleFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    Write-Host ""
    $rapportFile = Get-ReportFileSelection `
        -Question "Will you be sending out Rapport Utilization stats?" `
        -ReportKey "RapportUtilization" `
        -FilePickerTitle "Select Rapport Utilization Excel File"

    if (-not [string]::IsNullOrWhiteSpace($rapportFile)) {
        Process-RapportUtilizationReport `
            -Path $rapportFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    Write-Host ""
    $lowScoresFile = Get-ReportFileSelection `
        -Question "Will you be sending out Low Compliance / Low Soft Skills scores?" `
        -ReportKey "LowScores" `
        -FilePickerTitle "Select Low Compliance / Low Soft Skills Excel File"

    if (-not [string]::IsNullOrWhiteSpace($lowScoresFile)) {
        Process-LowScoresReport `
            -Path $lowScoresFile `
            -Groups $groups `
            -PeopleByName $peopleByName `
            -Issues $issues
    }

    if ($groups.Count -eq 0) {
        Write-Host ""
        Write-Host "No email groups were created. No Outlook emails will be opened." -ForegroundColor Yellow
    }
    else {
        $emailCount = Open-OutlookEmails -Groups $groups

        Write-Host ""
        Write-Host "================ SUMMARY ================" -ForegroundColor Cyan
        Write-Host "Supervisor email groups created: $($groups.Count)"
        Write-Host "Outlook email windows opened: $emailCount"
        Write-Host "=========================================" -ForegroundColor Cyan
    }

    if ($issues.Count -gt 0) {
        Write-Host ""
        Write-Host "Issues / unmatched names:" -ForegroundColor Yellow

        foreach ($issue in $issues) {
            Write-Host " - $issue" -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "Done. Outlook email windows were opened. Nothing was sent automatically." -ForegroundColor Green
}
catch {
    Write-Host ""
    Write-Host "ERROR:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
}

